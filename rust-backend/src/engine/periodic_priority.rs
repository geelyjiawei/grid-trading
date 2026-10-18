use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::Notify;

const MAX_PERIODIC_DEFERRALS: usize = 3;
const MAX_PERIODIC_AUDIT_DELAY: Duration = Duration::from_secs(5);

struct AuditSchedule {
    completed_at: Instant,
    deferrals: usize,
}

impl AuditSchedule {
    fn is_due(&self) -> bool {
        self.deferrals >= MAX_PERIODIC_DEFERRALS
            || self.completed_at.elapsed() >= MAX_PERIODIC_AUDIT_DELAY
    }
}

struct PriorityState {
    waiters: AtomicUsize,
    stop_waiters: AtomicUsize,
    changed: Notify,
    advancing: AtomicBool,
    audit: Mutex<AuditSchedule>,
}

#[derive(Clone)]
pub(super) struct PeriodicPriority(Arc<PriorityState>);

impl Default for PeriodicPriority {
    fn default() -> Self {
        Self(Arc::new(PriorityState {
            waiters: AtomicUsize::new(0),
            stop_waiters: AtomicUsize::new(0),
            changed: Notify::new(),
            advancing: AtomicBool::new(false),
            audit: Mutex::new(AuditSchedule {
                completed_at: Instant::now(),
                deferrals: 0,
            }),
        }))
    }
}

impl PeriodicPriority {
    pub(super) fn has_waiters(&self) -> bool {
        self.0.waiters.load(Ordering::Acquire) > 0
    }

    pub(super) fn audit_due(&self) -> bool {
        self.0
            .audit
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_due()
    }

    pub(super) fn is_advancing(&self) -> bool {
        self.0.advancing.load(Ordering::Acquire)
    }

    pub(super) fn begin(&self) -> Option<PeriodicReadPriority> {
        self.0
            .advancing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(PeriodicReadPriority {
            state: Some(Arc::clone(&self.0)),
            deferred: AtomicBool::new(false),
            protected: AtomicBool::new(self.audit_due()),
            audit_read_completed: AtomicBool::new(false),
        })
    }
}

pub(super) struct PriorityWaiter {
    state: Arc<PriorityState>,
    stop: bool,
}

impl PriorityWaiter {
    pub(super) fn register(priority: &PeriodicPriority) -> Self {
        Self::register_kind(priority, false)
    }

    pub(super) fn register_stop(priority: &PeriodicPriority) -> Self {
        Self::register_kind(priority, true)
    }

    fn register_kind(priority: &PeriodicPriority, stop: bool) -> Self {
        priority.0.waiters.fetch_add(1, Ordering::AcqRel);
        if stop {
            priority.0.stop_waiters.fetch_add(1, Ordering::AcqRel);
        }
        priority.0.changed.notify_waiters();
        Self {
            state: Arc::clone(&priority.0),
            stop,
        }
    }
}

impl Drop for PriorityWaiter {
    fn drop(&mut self) {
        if self.stop {
            self.state.stop_waiters.fetch_sub(1, Ordering::AcqRel);
        }
        self.state.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Preemption is allowed only around explicitly read-only periodic work.
/// The caller must never wrap accounting, placement, or acknowledgement commits.
#[derive(Default)]
pub(crate) struct PeriodicReadPriority {
    state: Option<Arc<PriorityState>>,
    deferred: AtomicBool,
    protected: AtomicBool,
    audit_read_completed: AtomicBool,
}

impl PeriodicReadPriority {
    pub(crate) async fn read<F: Future>(&self, future: F) -> Option<F::Output> {
        let Some(state) = &self.state else {
            return Some(future.await);
        };
        let mut future = std::pin::pin!(future);
        loop {
            // Subscribe before checking counts; a waiter may arrive during GET polling.
            let changed = state.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.defer_for_waiters(state) {
                return None;
            }
            tokio::select! {
                biased;
                _ = &mut changed => {},
                result = &mut future => {
                    // Also cover a waiter registered by the GET's final poll.
                    return (!self.defer_for_waiters(state)).then_some(result);
                }
            }
        }
    }

    fn defer_for_waiters(&self, state: &PriorityState) -> bool {
        if self.was_deferred() {
            return true;
        }
        let stopping = state.stop_waiters.load(Ordering::Acquire) > 0;
        if !stopping && state.waiters.load(Ordering::Acquire) == 0 {
            return false;
        }
        let mut audit = state
            .audit
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !stopping && (self.is_protected() || audit.is_due()) {
            self.protected.store(true, Ordering::Release);
            return false;
        }
        if !self.deferred.swap(true, Ordering::AcqRel) {
            audit.deferrals = audit.deferrals.saturating_add(1);
        }
        true
    }

    pub(crate) fn was_deferred(&self) -> bool {
        self.deferred.load(Ordering::Acquire)
    }

    pub(crate) fn audit_read_completed(&self) {
        self.audit_read_completed.store(true, Ordering::Release);
    }

    pub(super) fn is_protected(&self) -> bool {
        self.protected.load(Ordering::Acquire)
    }

    pub(super) fn complete(&self, successful: bool) {
        if successful
            && !self.was_deferred()
            && self.audit_read_completed.load(Ordering::Acquire)
            && let Some(state) = &self.state
        {
            let mut audit = state
                .audit
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            audit.completed_at = Instant::now();
            audit.deferrals = 0;
        }
    }
}

impl Drop for PeriodicReadPriority {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.advancing.store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::poll_fn, task::Poll};

    use tokio::sync::{Mutex as AsyncMutex, oneshot};

    use super::*;

    #[tokio::test]
    async fn pending_read_yields_to_a_new_priority_waiter() {
        let priority = PeriodicPriority::default();
        let context = priority.begin().unwrap();
        let mut read = Box::pin(context.read(std::future::pending::<()>()));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let _waiter = PriorityWaiter::register(&priority);
        let result = tokio::time::timeout(Duration::from_millis(100), read).await;
        assert!(
            matches!(result, Ok(None)),
            "pending GET must yield: {result:?}"
        );
        assert!(context.was_deferred());
    }

    #[tokio::test]
    async fn existing_waiter_defers_without_polling_the_read() {
        let priority = PeriodicPriority::default();
        let context = priority.begin().unwrap();
        let _waiter = PriorityWaiter::register(&priority);
        let polled = AtomicBool::new(false);
        let result = context
            .read(poll_fn(|_| {
                polled.store(true, Ordering::Release);
                Poll::Ready(7)
            }))
            .await;
        assert_eq!(result, None);
        assert!(!polled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn waiter_registering_during_the_first_read_poll_is_not_missed() {
        let priority = PeriodicPriority::default();
        let context = priority.begin().unwrap();
        let mut waiter = None;
        let read = context.read(poll_fn(|_| {
            waiter.get_or_insert_with(|| PriorityWaiter::register(&priority));
            Poll::<()>::Pending
        }));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), read).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn waiter_arriving_with_a_ready_response_wins_before_result_application() {
        let priority = PeriodicPriority::default();
        let context = priority.begin().unwrap();
        let mut waiter = None;
        let result = context
            .read(poll_fn(|_| {
                waiter.get_or_insert_with(|| PriorityWaiter::register(&priority));
                Poll::Ready(7)
            }))
            .await;
        assert_eq!(result, None);
        assert!(context.was_deferred());
    }

    #[tokio::test]
    async fn canceling_a_priority_lock_waiter_releases_its_registration() {
        let priority = PeriodicPriority::default();
        let lock = Arc::new(AsyncMutex::new(()));
        let owner = lock.lock().await;
        let waiting_priority = priority.clone();
        let waiting_lock = Arc::clone(&lock);
        let (entered, started) = oneshot::channel();
        let waiting = tokio::spawn(async move {
            let _waiter = PriorityWaiter::register(&waiting_priority);
            entered.send(()).unwrap();
            let _guard = waiting_lock.lock().await;
        });
        started.await.unwrap();
        assert!(priority.has_waiters());
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert!(!priority.has_waiters());
        drop(owner);
        let context = priority.begin().unwrap();
        assert_eq!(context.read(std::future::ready(9)).await, Some(9));
    }

    #[tokio::test]
    async fn deferral_drops_the_pending_read_without_applying_its_result() {
        let priority = PeriodicPriority::default();
        let context = priority.begin().unwrap();
        let value = AtomicUsize::new(42);
        let (release, response) = oneshot::channel::<usize>();
        let mut read = Box::pin(context.read(async {
            let next = response.await.unwrap();
            value.store(next, Ordering::Release);
        }));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let _waiter = PriorityWaiter::register(&priority);
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), read).await,
            Ok(None)
        ));
        assert!(release.send(99).is_err());
        assert_eq!(value.load(Ordering::Acquire), 42);
    }

    #[tokio::test]
    async fn three_deferred_ticks_protect_the_next_audit_until_it_finishes() {
        let priority = PeriodicPriority::default();
        let _waiter = PriorityWaiter::register(&priority);
        for _ in 0..3 {
            let context = priority.begin().unwrap();
            assert_eq!(context.read(std::future::ready(7)).await, None);
            context.complete(true);
        }
        let context = priority.begin().unwrap();
        assert!(context.is_protected());
        let (release, response) = oneshot::channel();
        let mut read = Box::pin(context.read(response));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(9).unwrap();
        assert_eq!(read.await, Some(Ok(9)));
        assert!(!context.was_deferred());
        context.audit_read_completed();
        context.complete(true);
        drop(context);
        assert_eq!(
            priority.begin().unwrap().read(std::future::ready(11)).await,
            None
        );
    }

    #[tokio::test]
    async fn overdue_audit_remains_protected_after_an_error() {
        let priority = PeriodicPriority::default();
        priority.0.audit.lock().unwrap().completed_at = Instant::now() - Duration::from_secs(6);
        let _waiter = PriorityWaiter::register(&priority);
        let context = priority.begin().unwrap();
        assert!(context.is_protected());
        assert_eq!(
            context
                .read(std::future::ready(Err::<(), _>("GET failed")))
                .await,
            Some(Err("GET failed"))
        );
        context.audit_read_completed();
        context.complete(false);
        drop(context);
        let next = priority.begin().unwrap();
        assert!(next.is_protected());
        assert_eq!(next.read(std::future::ready(5)).await, Some(5));
    }

    #[tokio::test]
    async fn a_successful_empty_tick_cannot_claim_a_completed_audit() {
        let priority = PeriodicPriority::default();
        priority.0.audit.lock().unwrap().completed_at = Instant::now() - Duration::from_secs(6);
        let context = priority.begin().unwrap();
        context.complete(true);
        drop(context);
        assert!(priority.begin().unwrap().is_protected());
    }

    #[tokio::test]
    async fn stop_preempts_even_a_protected_pending_audit_read() {
        let priority = PeriodicPriority::default();
        priority.0.audit.lock().unwrap().completed_at = Instant::now() - Duration::from_secs(6);
        let context = priority.begin().unwrap();
        let execution = PriorityWaiter::register(&priority);
        let mut read = Box::pin(context.read(std::future::pending::<()>()));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let _stop = PriorityWaiter::register_stop(&priority);
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), read).await,
            Ok(None)
        ));
        assert!(context.was_deferred());
        drop(execution);
        context.audit_read_completed();
        context.complete(true);
        drop(context);
        assert!(priority.begin().unwrap().is_protected());
    }

    #[tokio::test]
    async fn canceled_periodic_context_releases_the_single_audit_lease() {
        let priority = PeriodicPriority::default();
        let context = priority.begin().unwrap();
        assert!(priority.begin().is_none());
        let mut read = Box::pin(context.read(std::future::pending::<()>()));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(read);
        assert!(!context.was_deferred());
        drop(context);
        let next = priority.begin().unwrap();
        assert_eq!(next.read(std::future::ready(5)).await, Some(5));
    }
}
