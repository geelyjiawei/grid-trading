import asyncio
import contextlib
import json
import logging
import re
import time
import uuid
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from itertools import pairwise
from typing import Any, Callable, Optional

import requests

from exchange_errors import (
    ExchangeRateLimitError,
    ExchangeRequestUncertainError,
    is_exchange_rate_limit_message,
)
from exchange_snapshots import (
    OPEN_ORDER_STATUSES,
    validate_execution_response,
    validate_instrument_response,
    validate_order_row,
    validate_position_response,
    validate_ticker_response,
)


logger = logging.getLogger(__name__)

NORMAL_POLL_SECONDS = 3.0
FAST_POLL_SECONDS = 0.3
FAST_POLL_WINDOW_SECONDS = 15.0
USER_STREAM_KEEPALIVE_SECONDS = 30 * 60
USER_STREAM_RECONNECT_SECONDS = 5.0
BATCH_ORDER_CHUNK_SIZE = 5
POSITION_SYNC_GRACE_SECONDS = 2.0
SUBMISSION_RETRY_SECONDS = 10.0
SUBMISSION_MAX_RETRIES = 3
SUBMISSION_REQUIRED_NOT_FOUND_CHECKS = 5
SUBMISSION_NOT_FOUND_CHECK_INTERVAL_SECONDS = 0.5
MANAGED_CANCEL_MAX_ROUNDS = 5
MANAGED_CANCEL_RETRY_SECONDS = 0.25
ORDER_LINK_RANDOM_HEX_LENGTH = 16
ORDER_REJECTION_BACKOFF_BASE_SECONDS = 3.0
ORDER_REJECTION_BACKOFF_MAX_SECONDS = 60.0
RESTORE_REFRESH_MESSAGE_PREFIX = "Restore refresh paused:"
COMPLETED_REPAIR_MESSAGE_PREFIXES = (
    "Repaired ",
    "Restored ",
    "Placed boundary reduce-only fallback",
)


class GridEngine:
    def __init__(self, client, config: dict, state_callback: Callable[["GridEngine"], None] | None = None):
        self.client = client
        self.config = config
        self.state_callback = state_callback
        self.running = False
        self.grid_levels: list[float] = []
        self.active_orders: dict[str, dict] = {}
        self.filled_orders: list[dict] = []
        self.filled_count = 0
        self.completed_pairs = 0
        self.reduce_lots_by_level: dict[str, dict[str, float]] = {}
        self.reduce_lots_complete = False
        self.target_qty_by_level: dict[str, float] = {}
        self.gross_profit = 0.0
        self.total_profit = 0.0
        self.total_fee = 0.0
        self.total_volume = 0.0
        self.start_time: float | None = None
        self.tick_size = "0.1"
        self.qty_step = "0.001"
        self.min_qty = 0.001
        self.min_notional = 0.0
        self.market_qty_step = "0.001"
        self.market_min_qty = 0.001
        self.max_market_qty = 0.0
        self.current_price = 0.0
        self.current_mark_price = 0.0
        self.initial_side = ""
        self.initial_qty = 0.0
        self.initial_entry_price = 0.0
        self.opening_target_qty = 0.0
        self.opening_filled_qty = 0.0
        self.opening_filled_volume = 0.0
        self.baseline_position_side = ""
        self.baseline_position_qty = 0.0
        self.baseline_position_entry_price = 0.0
        self.grid_position_net_qty = 0.0
        self.grid_profit_pct = 0.0
        self.waiting_trigger = False
        self.trigger_message = ""
        self.grid_ready = False
        self.waiting_initial_order = False
        self.opening_order: dict | None = None
        self.pending_reduce_action: dict | None = None
        self.risk_shutdown_pending = False
        self.manual_stop_pending = False
        self.initialization_in_progress = False
        self.initialization_failed = False
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger: dict[str, dict[str, Any]] = {}
        self.ownership_conflicts: list[dict] = []
        self._pending_targets: dict | None = None
        self.paused_replacements: list[dict] = []
        self._stopping = False
        self._boundary_repair_in_progress = False
        self._boundary_repair_retry_after = 0.0
        self._allow_restore_baseline_migration = False
        self._task: Optional[asyncio.Task] = None
        self._wake_event: asyncio.Event | None = None
        self._fast_poll_until = 0.0
        self._user_stream_task: Optional[asyncio.Task] = None
        self._user_stream_listen_key = ""
        self._position_mismatch_seen_at = 0.0
        self._position_mismatch_signature: tuple[float, float, float] | None = None
        self._last_actual_grid_position_net_qty = 0.0
        self._reduce_warning_at_by_signature: dict[tuple, float] = {}
        self._exchange_rate_limit_until = 0.0
        self._order_rejection_backoff: dict[str, dict[str, float | int | str]] = {}
        self.restore_refresh_pending = False
        self.restore_refresh_error = ""
        self.restore_refresh_retry_after = 0.0
        self.restore_refresh_attempts = 0
        self._restore_saved_running = False
        self._restore_legacy_bootstrap_pending = False
        self._restore_previous_trigger_message = ""

    def _persist_state(self):
        if self.state_callback:
            self.state_callback(self)

    def to_state(self) -> dict[str, Any]:
        return {
            "version": 1,
            "config": self.config,
            "running": self.running,
            "grid_levels": self.grid_levels,
            "active_orders": self.active_orders,
            "filled_orders": self.filled_orders[-200:],
            "filled_count": self.filled_count,
            "completed_pairs": self.completed_pairs,
            "reduce_lots_by_level": self.reduce_lots_by_level,
            "reduce_lots_complete": self.reduce_lots_complete,
            "target_qty_by_level": self.target_qty_by_level,
            "gross_profit": self.gross_profit,
            "total_profit": self.total_profit,
            "total_fee": self.total_fee,
            "total_volume": self.total_volume,
            "start_time": self.start_time,
            "tick_size": self.tick_size,
            "qty_step": self.qty_step,
            "min_qty": self.min_qty,
            "min_notional": self.min_notional,
            "market_qty_step": self.market_qty_step,
            "market_min_qty": self.market_min_qty,
            "max_market_qty": self.max_market_qty,
            "current_price": self.current_price,
            "current_mark_price": self.current_mark_price,
            "initial_side": self.initial_side,
            "initial_qty": self.initial_qty,
            "initial_entry_price": self.initial_entry_price,
            "opening_target_qty": self.opening_target_qty,
            "opening_filled_qty": self.opening_filled_qty,
            "opening_filled_volume": self.opening_filled_volume,
            "baseline_position_side": self.baseline_position_side,
            "baseline_position_qty": self.baseline_position_qty,
            "baseline_position_entry_price": self.baseline_position_entry_price,
            "grid_position_net_qty": self.grid_position_net_qty,
            "grid_profit_pct": self.grid_profit_pct,
            "waiting_trigger": self.waiting_trigger,
            "trigger_message": self.trigger_message,
            "grid_ready": self.grid_ready,
            "waiting_initial_order": self.waiting_initial_order,
            "opening_order": self.opening_order,
            "pending_reduce_action": self.pending_reduce_action,
            "risk_shutdown_pending": self.risk_shutdown_pending,
            "manual_stop_pending": self.manual_stop_pending,
            "initialization_in_progress": self.initialization_in_progress,
            "initialization_failed": self.initialization_failed,
            "initial_grid_deployment_pending": self.initial_grid_deployment_pending,
            "initial_grid_deployment_ledger": self.initial_grid_deployment_ledger,
            "ownership_conflicts": self.ownership_conflicts,
            "pending_targets": self._pending_targets,
            # This is live recovery work, not display history. Truncating it
            # would permanently discard older missing grid legs after restart.
            "paused_replacements": list(self.paused_replacements),
            "exchange_rate_limit_until": self._exchange_rate_limit_until,
            "order_rejection_backoff": self._order_rejection_backoff,
            "restore_refresh_pending": self.restore_refresh_pending,
            "restore_refresh_error": self.restore_refresh_error,
            "restore_refresh_retry_after": self.restore_refresh_retry_after,
            "restore_refresh_attempts": self.restore_refresh_attempts,
            "restore_saved_running": (
                self._restore_saved_running if self.restore_refresh_pending else False
            ),
            "restore_legacy_bootstrap_pending": self._restore_legacy_bootstrap_pending,
            "restore_previous_trigger_message": self._restore_previous_trigger_message,
            "saved_at": time.time(),
        }

    def restore_state(self, state: dict[str, Any]):
        saved_running = bool(
            state.get("running", False) or state.get("restore_saved_running", False)
        )
        self.config = dict(state.get("config") or self.config)
        self.grid_levels = list(state.get("grid_levels") or [])
        self.active_orders = dict(state.get("active_orders") or {})
        self.filled_orders = list(state.get("filled_orders") or [])
        self.filled_count = max(
            len(self.filled_orders),
            int(state.get("filled_count") or 0),
        )
        self.completed_pairs = int(state.get("completed_pairs") or 0)
        allow_reduce_lot_legacy_bootstrap = "reduce_lots_complete" not in state
        self.reduce_lots_by_level = self._normalize_reduce_lots(state.get("reduce_lots_by_level") or {})
        self.reduce_lots_complete = bool(state.get("reduce_lots_complete", bool(self.reduce_lots_by_level)))
        if not self.reduce_lots_complete:
            self.reduce_lots_by_level = {}
        self.target_qty_by_level = self._normalize_level_qtys(state.get("target_qty_by_level") or {})
        self.gross_profit = float(state.get("gross_profit") or 0)
        self.total_profit = float(state.get("total_profit") or 0)
        self.total_fee = float(state.get("total_fee") or 0)
        self.total_volume = float(state.get("total_volume") or 0)
        self.start_time = state.get("start_time")
        self.tick_size = str(state.get("tick_size") or self.tick_size)
        self.qty_step = str(state.get("qty_step") or self.qty_step)
        self.min_qty = float(state.get("min_qty") or self.min_qty)
        self.min_notional = float(state.get("min_notional") or 0)
        self.market_qty_step = str(state.get("market_qty_step") or self.qty_step)
        self.market_min_qty = float(state.get("market_min_qty") or self.min_qty)
        self.max_market_qty = float(state.get("max_market_qty") or 0)
        self.current_price = float(state.get("current_price") or 0)
        self.current_mark_price = float(state.get("current_mark_price") or 0)
        self.initial_side = str(state.get("initial_side") or "")
        self.initial_qty = float(state.get("initial_qty") or 0)
        self.initial_entry_price = float(state.get("initial_entry_price") or 0)
        self.opening_target_qty = float(state.get("opening_target_qty") or 0)
        self.opening_filled_qty = float(state.get("opening_filled_qty") or 0)
        self.opening_filled_volume = float(state.get("opening_filled_volume") or 0)
        self.baseline_position_side = str(state.get("baseline_position_side") or "")
        self.baseline_position_qty = float(state.get("baseline_position_qty") or 0)
        self.baseline_position_entry_price = float(state.get("baseline_position_entry_price") or 0)
        self._allow_restore_baseline_migration = "grid_position_net_qty" not in state
        if "grid_position_net_qty" in state:
            self._set_grid_position_net_qty(state.get("grid_position_net_qty") or 0)
        else:
            self._set_grid_position_net_qty(self._derive_grid_position_net_qty())
        self.grid_profit_pct = float(state.get("grid_profit_pct") or 0)
        self.waiting_trigger = bool(state.get("waiting_trigger", False))
        self.trigger_message = str(state.get("trigger_message") or "")
        self.grid_ready = bool(state.get("grid_ready", False))
        self.waiting_initial_order = bool(state.get("waiting_initial_order", False))
        self.opening_order = state.get("opening_order")
        self.pending_reduce_action = state.get("pending_reduce_action")
        self.risk_shutdown_pending = bool(state.get("risk_shutdown_pending", False))
        self.manual_stop_pending = bool(state.get("manual_stop_pending", False))
        self.initialization_in_progress = bool(
            state.get("initialization_in_progress", False)
        )
        self.initialization_failed = bool(state.get("initialization_failed", False))
        self.initial_grid_deployment_pending = bool(
            state.get("initial_grid_deployment_pending", False)
        )
        initial_deployment_ledger_present = "initial_grid_deployment_ledger" in state
        raw_initial_deployment_ledger = state.get("initial_grid_deployment_ledger")
        initial_deployment_ledger_invalid = bool(
            initial_deployment_ledger_present
            and (
                not isinstance(raw_initial_deployment_ledger, dict)
                or any(
                    not isinstance(value, dict)
                    for value in (
                        raw_initial_deployment_ledger.values()
                        if isinstance(raw_initial_deployment_ledger, dict)
                        else []
                    )
                )
            )
        )
        if not isinstance(raw_initial_deployment_ledger, dict):
            raw_initial_deployment_ledger = {}
        self.initial_grid_deployment_ledger = {
            str(key): dict(value)
            for key, value in raw_initial_deployment_ledger.items()
            if isinstance(value, dict)
        }
        self.ownership_conflicts = list(state.get("ownership_conflicts") or [])
        self._pending_targets = state.get("pending_targets")
        self.paused_replacements = list(state.get("paused_replacements") or [])
        self._exchange_rate_limit_until = float(
            state.get("exchange_rate_limit_until") or 0
        )
        raw_backoff = state.get("order_rejection_backoff") or {}
        self._order_rejection_backoff = {
            str(key): dict(value)
            for key, value in raw_backoff.items()
            if isinstance(value, dict)
        }
        self.restore_refresh_pending = True
        self.restore_refresh_error = ""
        self.restore_refresh_retry_after = 0.0
        self.restore_refresh_attempts = 0
        self._restore_saved_running = saved_running
        self._restore_legacy_bootstrap_pending = bool(
            state.get(
                "restore_legacy_bootstrap_pending",
                allow_reduce_lot_legacy_bootstrap,
            )
        )
        self._restore_previous_trigger_message = str(
            state.get("restore_previous_trigger_message") or ""
        )

        invalid_initial_deployment = bool(
            self.initial_grid_deployment_pending
            and (
                self.grid_ready
                or not isinstance(self._pending_targets, dict)
                or not self._pending_targets
                or not initial_deployment_ledger_present
                or initial_deployment_ledger_invalid
            )
        )
        recoverable_initial_deployment = bool(
            self.initial_grid_deployment_pending
            and isinstance(self._pending_targets, dict)
            and self._pending_targets
            and not self.initialization_failed
            and not self.manual_stop_pending
            and not self.grid_ready
        )
        if invalid_initial_deployment:
            self.initial_grid_deployment_pending = False
            self.initial_grid_deployment_ledger.clear()
            self.initialization_in_progress = False
            self.initialization_failed = True
            self.manual_stop_pending = True
            self.grid_ready = False
            self.trigger_message = (
                "Initial grid deployment recovery state is inconsistent. Managed orders "
                "will be reconciled and cancelled; the retained position will not be "
                "market-closed automatically."
            )
        elif recoverable_initial_deployment:
            self.initialization_in_progress = True
        elif self.initial_grid_deployment_pending:
            self.initial_grid_deployment_pending = False
            self.initial_grid_deployment_ledger.clear()
            self.initialization_in_progress = False
        elif self.initialization_in_progress and not self.grid_ready:
            self.initial_grid_deployment_pending = False
            self.initial_grid_deployment_ledger.clear()
            self.initialization_in_progress = False
            self.initialization_failed = True
            self.manual_stop_pending = True
            self.trigger_message = (
                "Grid initialization was interrupted after exchange work began. Managed "
                "orders will be reconciled and cancelled; the retained position will not "
                "be market-closed or reused by a new grid without explicit review."
            )

        if not self.grid_levels:
            self.grid_levels = self._calculate_levels()
        self._complete_restore_refresh()
        self._persist_state()

    def _complete_restore_refresh(self) -> bool:
        saved_running = self._restore_saved_running
        try:
            self._fetch_precision()
            self.current_price = self._get_current_price()
            if self._restore_legacy_bootstrap_pending:
                self._bootstrap_reduce_lots_from_legacy_state()
            if saved_running:
                self._migrate_baseline_position_from_exchange()
                if not self.risk_shutdown_pending and not self.manual_stop_pending:
                    self._reconcile_exchange_open_orders()
                    if not self.risk_shutdown_pending and not self.manual_stop_pending:
                        self._reconcile_grid_position_protection()
        except Exception as exc:
            self.restore_refresh_pending = True
            self.restore_refresh_error = str(exc)
            self.restore_refresh_attempts += 1
            delay = min(
                ORDER_REJECTION_BACKOFF_MAX_SECONDS,
                NORMAL_POLL_SECONDS * (2 ** min(self.restore_refresh_attempts - 1, 5)),
            )
            if isinstance(exc, ExchangeRateLimitError):
                delay = max(delay, float(exc.retry_after))
                self._exchange_rate_limit_until = max(
                    self._exchange_rate_limit_until,
                    time.time() + float(exc.retry_after),
                )
            self.restore_refresh_retry_after = time.time() + delay
            if not self.trigger_message.startswith(RESTORE_REFRESH_MESSAGE_PREFIX):
                self._restore_previous_trigger_message = self.trigger_message
            self.trigger_message = (
                f"{RESTORE_REFRESH_MESSAGE_PREFIX} exchange rules or authoritative state "
                f"are unavailable ({exc}); normal grid placement is disabled and recovery "
                "will retry without discarding the saved ledger."
            )
            logger.warning("Restore refresh failed symbol=%s msg=%s", self.config.get("symbol"), exc)
            return False

        self.restore_refresh_pending = False
        self.restore_refresh_error = ""
        self.restore_refresh_retry_after = 0.0
        self.restore_refresh_attempts = 0
        self._restore_saved_running = False
        self._restore_legacy_bootstrap_pending = False
        if self.trigger_message.startswith(RESTORE_REFRESH_MESSAGE_PREFIX):
            self.trigger_message = self._restore_previous_trigger_message
        self._restore_previous_trigger_message = ""
        return True

    def _clear_restore_refresh_state(self):
        """Discard retry metadata once the strategy has reached a terminal stop."""
        self.restore_refresh_pending = False
        self.restore_refresh_error = ""
        self.restore_refresh_retry_after = 0.0
        self.restore_refresh_attempts = 0
        self._restore_saved_running = False
        self._restore_legacy_bootstrap_pending = False
        self._restore_previous_trigger_message = ""
        if self.trigger_message.startswith(RESTORE_REFRESH_MESSAGE_PREFIX):
            self.trigger_message = ""

    async def initialize(self):
        symbol = self.config["symbol"]
        leverage = str(self.config["leverage"])

        self._fetch_precision()
        leverage_resp = self.client.set_leverage(symbol, leverage)
        if leverage_resp.get("retCode") not in (0, 110043):
            raise RuntimeError(leverage_resp.get("retMsg", "Failed to set leverage"))

        self.grid_levels = self._calculate_levels()
        self.current_price = self._get_current_price()
        self.grid_profit_pct = self._calculate_grid_profit_pct(self.current_price)

        trigger_price = self.config.get("trigger_price")
        if trigger_price is not None and not self._is_trigger_hit(self.current_price):
            self.waiting_trigger = True
            self.trigger_message = f"Waiting for trigger price {trigger_price}"
            return

        self._deploy_initial_grid(self.current_price)

    def start(self):
        if self.running:
            return
        self._stopping = False
        self.running = True
        if self.start_time is None:
            self.start_time = time.time()
        self._wake_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop())
        if self._supports_user_stream():
            self._user_stream_task = asyncio.create_task(self._user_stream_loop())
        self._persist_state()

    def _resolve_pending_for_cancel(self, order: dict, open_orders: list[dict]) -> str:
        if not order.get("submission_pending"):
            return "confirmed"

        link_id = str(order.get("link_id", "") or "")
        open_snapshot = next(
            (
                item
                for item in open_orders
                if str(item.get("orderLinkId", "") or "") == link_id
            ),
            None,
        )
        if open_snapshot and self._confirm_pending_submission(order, open_snapshot):
            return "confirmed"
        if order.get("accepted_shape_mismatch") and order.get("order_id"):
            return "confirmed"

        snapshot, authoritative = self._submission_snapshot_by_link(link_id)
        if snapshot and self._confirm_pending_submission(order, snapshot):
            return "confirmed"
        if order.get("accepted_shape_mismatch") and order.get("order_id"):
            return "confirmed"

        history_snapshot = self._submission_history_by_link({link_id}).get(link_id)
        if history_snapshot and self._confirm_pending_submission(order, history_snapshot):
            return "confirmed"
        if order.get("accepted_shape_mismatch") and order.get("order_id"):
            return "confirmed"

        now = time.time()
        if authoritative:
            last_check = float(order.get("submission_last_not_found_at", 0) or 0)
            if now - last_check >= SUBMISSION_NOT_FOUND_CHECK_INTERVAL_SECONDS:
                order["submission_not_found_count"] = int(
                    order.get("submission_not_found_count", 0) or 0
                ) + 1
                order["submission_last_not_found_at"] = now
            if (
                int(order.get("submission_not_found_count", 0) or 0)
                >= SUBMISSION_REQUIRED_NOT_FOUND_CHECKS
            ):
                return "absent"
        return "pending"

    def _begin_opening_progress(self, side: str, target_qty: float) -> None:
        target = self._normalized_qty_decimal(target_qty, self.qty_step)
        self.initial_side = side
        self.initial_qty = 0.0
        self.initial_entry_price = 0.0
        self.opening_target_qty = float(target)
        self.opening_filled_qty = 0.0
        self.opening_filled_volume = 0.0
        self._set_grid_position_net_qty(0)

    def _opening_target_qty_decimal(self) -> Decimal:
        candidates = (
            self.opening_target_qty,
            self.config.get("derived_total_qty"),
        )
        for candidate in candidates:
            try:
                qty = Decimal(str(candidate or 0))
            except Exception:
                continue
            if qty > 0:
                return self._normalized_qty_decimal(qty, self.qty_step)

        order_qty = Decimal(str((self.opening_order or {}).get("qty", 0) or 0))
        filled_qty = Decimal(str(self.opening_filled_qty or 0))
        return self._normalized_qty_decimal(order_qty + filled_qty, self.qty_step)

    def _record_opening_execution_delta(self, order: dict, stats: dict) -> bool:
        delta_stats = self._fill_delta_stats(order, stats)
        if not delta_stats:
            return False

        self._mark_order_fill_processed(order, stats)
        filled_qty = Decimal(str(self.opening_filled_qty or 0)) + Decimal(
            str(delta_stats["qty"])
        )
        filled_volume = Decimal(str(self.opening_filled_volume or 0)) + Decimal(
            str(delta_stats["volume"])
        )
        self.opening_filled_qty = float(filled_qty)
        self.opening_filled_volume = float(filled_volume)
        self.initial_side = str(order.get("side") or self.initial_side)
        self.initial_qty = float(filled_qty)
        self.initial_entry_price = float(filled_volume / filled_qty) if filled_qty > 0 else 0.0
        self._set_initial_grid_position(self.initial_side, self.initial_qty)
        self._record_trade_value(
            delta_stats["price"],
            delta_stats["qty"],
            volume=delta_stats["volume"],
            fee=delta_stats["fee"],
            fee_asset=delta_stats["fee_asset"],
            fee_source=delta_stats["fee_source"],
        )
        self._persist_state()
        return True

    def _record_opening_execution_for_stop(self, order: dict, stats: dict) -> bool:
        return self._record_opening_execution_delta(order, stats)

    def _terminal_order_accounted_for_stop(
        self,
        order: dict,
        *,
        opening: bool = False,
        place_counter: bool = False,
    ) -> bool:
        snapshot = self._get_order_snapshot(order)
        status = self._order_status_from_snapshot(snapshot)
        stats = self._authoritative_execution_stats(order, snapshot)
        if stats and stats.get("qty", 0) > 0:
            if opening:
                self._record_opening_execution_for_stop(order, stats)
            else:
                self._record_execution_delta(order, stats, place_counter=place_counter)

        planned_qty = Decimal(str(order.get("qty", 0) or 0))
        processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
        fully_accounted = processed_qty + self._qty_tolerance_decimal() >= planned_qty
        if self._is_cancelled_status(status):
            return True
        if self._is_filled_status(status):
            return fully_accounted
        return fully_accounted and status == "UNKNOWN"

    def _cancel_managed_order_once(
        self,
        order: dict,
        open_orders: list[dict],
        *,
        opening: bool = False,
        place_counter: bool = False,
    ) -> str:
        submission_state = self._resolve_pending_for_cancel(order, open_orders)
        if submission_state == "absent":
            return "done"
        if submission_state == "pending":
            return "pending"

        order_id = str(order.get("order_id", "") or "")
        link_id = str(order.get("link_id", "") or "")
        open_ids = {str(item.get("orderId", "") or "") for item in open_orders}
        open_links = {str(item.get("orderLinkId", "") or "") for item in open_orders}
        should_cancel = bool(order_id and (order_id in open_ids or link_id in open_links))
        if order_id and not should_cancel:
            snapshot = self._get_order_snapshot(order)
            status = self._order_status_from_snapshot(snapshot)
            if self._is_filled_status(status) or self._is_cancelled_status(status):
                return (
                    "done"
                    if self._terminal_order_accounted_for_stop(
                        order,
                        opening=opening,
                        place_counter=place_counter,
                    )
                    else "pending"
                )
            should_cancel = status != "UNKNOWN"

        if should_cancel:
            try:
                result = self.client.cancel_order(self.config["symbol"], order_id)
                if result.get("retCode") != 0:
                    raise RuntimeError(result.get("retMsg", "Managed order cancellation rejected"))
            except Exception as exc:
                logger.warning(
                    "Managed order cancellation is unconfirmed symbol=%s order_id=%s link_id=%s msg=%s",
                    self.config.get("symbol"),
                    order_id,
                    link_id,
                    exc,
                )

        if self._terminal_order_accounted_for_stop(
            order,
            opening=opening,
            place_counter=place_counter,
        ):
            return "done"
        return "pending"

    def _cancel_managed_orders_once(self) -> bool:
        open_orders = self._fetch_open_orders()
        for link_id, order in list(self.active_orders.items()):
            outcome = self._cancel_managed_order_once(order, open_orders)
            if outcome == "done":
                self.active_orders.pop(link_id, None)

        if self.opening_order:
            outcome = self._cancel_managed_order_once(
                self.opening_order,
                open_orders,
                opening=True,
            )
            if outcome == "done":
                self.opening_order = None
                self.waiting_initial_order = False

        self._persist_state()
        return not self.active_orders and self.opening_order is None

    async def stop(self):
        self._stopping = True
        self.manual_stop_pending = True
        self.initialization_in_progress = False
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger.clear()
        self.running = True
        self._persist_state()
        await self._stop_user_stream()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        completed = False
        for attempt in range(MANAGED_CANCEL_MAX_ROUNDS):
            if self.pending_reduce_action:
                with contextlib.suppress(Exception):
                    self._resolve_pending_reduce_action()
            completed = self._cancel_managed_orders_once() and not self.pending_reduce_action
            if completed:
                break
            if attempt + 1 < MANAGED_CANCEL_MAX_ROUNDS:
                await asyncio.sleep(MANAGED_CANCEL_RETRY_SECONDS)

        if not completed:
            self.trigger_message = (
                "Stop is incomplete: one or more managed orders still lack terminal exchange "
                "confirmation. State was retained for a safe retry."
            )
            self._persist_state()
            raise RuntimeError(self.trigger_message)

        self.running = False
        self.risk_shutdown_pending = False
        self.manual_stop_pending = False
        self.initialization_failed = False
        self.initialization_in_progress = False
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger.clear()
        self.paused_replacements.clear()
        self.grid_ready = False
        self._clear_restore_refresh_state()
        self._persist_state()

    async def suspend(self):
        """Stop the local polling task without touching exchange orders."""
        await self._stop_user_stream()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._persist_state()

    def _mark_fast_poll(self, seconds: float = FAST_POLL_WINDOW_SECONDS):
        self._fast_poll_until = max(self._fast_poll_until, time.time() + seconds)
        if self._wake_event:
            self._wake_event.set()

    def _rate_limit_remaining(self) -> float:
        return max(0.0, self._exchange_rate_limit_until - time.time())

    @staticmethod
    def _order_shape_key(
        side: str,
        price_text: str,
        qty_text: str,
        reduce_only: bool,
    ) -> str:
        return "|".join((side, price_text, qty_text, "1" if reduce_only else "0"))

    def _order_shape_retry_remaining(self, shape_key: str) -> float:
        retry = self._order_rejection_backoff.get(shape_key) or {}
        try:
            return max(0.0, float(retry.get("retry_after", 0) or 0) - time.time())
        except (TypeError, ValueError):
            return 0.0

    def _record_order_rejection(
        self,
        shape_key: str,
        message: str,
        *,
        rate_limit_retry_after: float | None = None,
        track_shape: bool = True,
    ):
        now = time.time()
        changed = False
        if rate_limit_retry_after is not None or is_exchange_rate_limit_message(message):
            delay = max(1.0, float(rate_limit_retry_after or ORDER_REJECTION_BACKOFF_MAX_SECONDS))
            self._exchange_rate_limit_until = max(
                self._exchange_rate_limit_until,
                now + delay,
            )
            self._fast_poll_until = min(self._fast_poll_until, now)
            changed = True

        if track_shape:
            previous = self._order_rejection_backoff.get(shape_key) or {}
            attempts = int(previous.get("attempts", 0) or 0) + 1
            delay = min(
                ORDER_REJECTION_BACKOFF_MAX_SECONDS,
                ORDER_REJECTION_BACKOFF_BASE_SECONDS * (2 ** min(attempts - 1, 8)),
            )
            self._order_rejection_backoff[shape_key] = {
                "attempts": attempts,
                "retry_after": now + delay,
                "message": str(message),
            }
            changed = True
        if changed:
            self._persist_state()

    def _clear_order_rejection(self, shape_key: str):
        if self._order_rejection_backoff.pop(shape_key, None) is not None:
            self._persist_state()

    def _poll_interval(self) -> float:
        rate_limit_remaining = self._rate_limit_remaining()
        if rate_limit_remaining > 0:
            return max(NORMAL_POLL_SECONDS, min(rate_limit_remaining, 60.0))
        if self.waiting_initial_order or time.time() < self._fast_poll_until:
            return FAST_POLL_SECONDS
        return NORMAL_POLL_SECONDS

    async def _sleep_until_next_poll(self):
        interval = self._poll_interval()
        if not self._wake_event:
            await asyncio.sleep(interval)
            return

        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        self._wake_event.clear()

    def _supports_user_stream(self) -> bool:
        return all(
            hasattr(self.client, name)
            for name in (
                "start_user_stream",
                "keepalive_user_stream",
                "close_user_stream",
                "user_stream_url",
            )
        )

    async def _stop_user_stream(self):
        task = self._user_stream_task
        self._user_stream_task = None
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        listen_key = self._user_stream_listen_key
        await self._close_user_stream_listen_key(listen_key)

    async def _close_user_stream_listen_key(self, listen_key: str):
        if not listen_key:
            return
        if self._user_stream_listen_key == listen_key:
            self._user_stream_listen_key = ""
        if hasattr(self.client, "close_user_stream"):
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self.client.close_user_stream, listen_key)

    async def _keepalive_user_stream(self, listen_key: str):
        while self.running and not self._stopping:
            await asyncio.sleep(USER_STREAM_KEEPALIVE_SECONDS)
            if not self.running or self._stopping:
                return
            await asyncio.to_thread(self.client.keepalive_user_stream, listen_key)

    async def _next_user_stream_message(self, websocket, keepalive_task: asyncio.Task):
        receive_task = asyncio.create_task(websocket.recv())
        try:
            done, _ = await asyncio.wait(
                {receive_task, keepalive_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive_task in done:
                await keepalive_task
                return None
            return await receive_task
        finally:
            if not receive_task.done():
                receive_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await receive_task

    def _is_relevant_user_stream_event(self, event: dict[str, Any]) -> bool:
        event_type = str(event.get("e") or "")
        if event_type not in {"ORDER_TRADE_UPDATE", "TRADE_LITE"}:
            return False
        symbol = str(event.get("s") or event.get("o", {}).get("s") or "").upper()
        return symbol == str(self.config.get("symbol", "")).upper()

    async def _user_stream_loop(self):
        try:
            import websockets
        except Exception as exc:
            logger.warning("Binance user stream unavailable symbol=%s msg=%s", self.config.get("symbol"), exc)
            return

        while self.running and not self._stopping:
            keepalive_task: asyncio.Task | None = None
            listen_key = ""
            reconnect_delay = 0.0
            try:
                listen_key = await asyncio.to_thread(self.client.start_user_stream)
                if not listen_key:
                    raise RuntimeError("Empty listen key")
                self._user_stream_listen_key = listen_key
                async with websockets.connect(
                    self.client.user_stream_url(listen_key),
                    ping_interval=20,
                    close_timeout=5,
                ) as websocket:
                    keepalive_task = asyncio.create_task(
                        self._keepalive_user_stream(listen_key)
                    )
                    while self.running and not self._stopping:
                        raw_message = await self._next_user_stream_message(
                            websocket,
                            keepalive_task,
                        )
                        if raw_message is None:
                            break
                        try:
                            event = json.loads(raw_message)
                        except (TypeError, ValueError):
                            continue
                        if self._is_relevant_user_stream_event(event):
                            self._mark_fast_poll()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.running and not self._stopping:
                    reconnect_delay = USER_STREAM_RECONNECT_SECONDS
                    if isinstance(exc, ExchangeRateLimitError):
                        reconnect_delay = max(reconnect_delay, float(exc.retry_after))
                        with contextlib.suppress(Exception):
                            self._record_order_rejection(
                                "user_stream",
                                str(exc),
                                rate_limit_retry_after=float(exc.retry_after),
                                track_shape=False,
                            )
                    elif is_exchange_rate_limit_message(exc):
                        reconnect_delay = max(
                            reconnect_delay,
                            ORDER_REJECTION_BACKOFF_MAX_SECONDS,
                        )
                        with contextlib.suppress(Exception):
                            self._record_order_rejection(
                                "user_stream",
                                str(exc),
                                rate_limit_retry_after=reconnect_delay,
                                track_shape=False,
                            )
                    logger.warning(
                        "Binance user stream disconnected symbol=%s msg=%s",
                        self.config.get("symbol"),
                        exc,
                    )
            finally:
                if keepalive_task:
                    keepalive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await keepalive_task
                await self._close_user_stream_listen_key(listen_key)
            if reconnect_delay > 0 and self.running and not self._stopping:
                await asyncio.sleep(reconnect_delay)

    def get_status(self) -> dict:
        return {
            "running": self.running,
            "grid_ready": self.grid_ready,
            "waiting_trigger": self.waiting_trigger,
            "waiting_initial_order": self.waiting_initial_order,
            "trigger_message": self.trigger_message,
            "symbol": self.config.get("symbol", ""),
            "direction": self.config.get("direction", ""),
            "grid_mode": self.config.get("grid_mode", "arithmetic"),
            "grid_levels": self.grid_levels,
            "active_orders": list(self.active_orders.values()),
            "pending_submission_count": sum(
                1 for order in self.active_orders.values() if order.get("submission_pending")
            )
            + int(bool(self.opening_order and self.opening_order.get("submission_pending")))
            + int(
                bool(
                    self.pending_reduce_action
                    and self.pending_reduce_action.get("submission_pending")
                )
            ),
            "pending_reduce_action": self.pending_reduce_action,
            "risk_shutdown_pending": self.risk_shutdown_pending,
            "manual_stop_pending": self.manual_stop_pending,
            "initialization_in_progress": self.initialization_in_progress,
            "initialization_failed": self.initialization_failed,
            "initial_grid_deployment_pending": self.initial_grid_deployment_pending,
            "initial_grid_deployment_submitted_count": (
                len(self.initial_grid_deployment_ledger)
                if self.initial_grid_deployment_pending
                else 0
            ),
            "initial_grid_deployment_total_count": self._initial_grid_target_count(),
            "ownership_conflicts": self.ownership_conflicts,
            "paused_replacements": self.paused_replacements,
            "paused_replacements_count": len(self.paused_replacements),
            "completed_pairs": self.completed_pairs,
            "filled_count": self.filled_count,
            "filled_orders": self.filled_orders[-50:],
            "reduce_lots_complete": self.reduce_lots_complete,
            "reduce_lots_by_level": self.reduce_lots_by_level,
            "reduce_protection": self.reduce_protection_snapshot(),
            "grid_coverage": self.grid_coverage_snapshot(),
            "target_qty_by_level": self.target_qty_by_level,
            "gross_profit": round(self.gross_profit, 4),
            "total_profit": round(self.total_profit, 4),
            "realized_net_profit": round(self.total_profit, 4),
            "total_fee": round(self.total_fee, 4),
            "total_volume": round(self.total_volume, 4),
            "fee_rate": self._fee_rate(),
            "maker_fee_rate": self._maker_fee_rate(),
            "taker_fee_rate": self._taker_fee_rate(),
            "fee_rate_source": self.config.get("fee_rate_source", "saved_config"),
            "fee_rate_fetched_at": self.config.get("fee_rate_fetched_at"),
            "start_time": self.start_time,
            "current_price": self.current_price,
            "current_mark_price": self.current_mark_price,
            "market_qty_step": self.market_qty_step,
            "market_min_qty": self.market_min_qty,
            "max_market_qty": self.max_market_qty,
            "min_notional": self.min_notional,
            "exchange_rate_limit_retry_after": round(self._rate_limit_remaining(), 3),
            "restore_refresh_pending": self.restore_refresh_pending,
            "restore_refresh_error": self.restore_refresh_error,
            "restore_refresh_retry_after": round(
                max(0.0, self.restore_refresh_retry_after - time.time()),
                3,
            ),
            "initial_side": self.initial_side,
            "initial_qty": round(self.initial_qty, 8),
            "initial_entry_price": round(self.initial_entry_price, 10),
            "opening_target_qty": round(self.opening_target_qty, 8),
            "opening_filled_qty": round(self.opening_filled_qty, 8),
            "opening_remaining_qty": round(
                max(0.0, self.opening_target_qty - self.opening_filled_qty),
                8,
            ),
            "baseline_position": {
                "side": self.baseline_position_side,
                "qty": round(self.baseline_position_qty, 8),
                "entry_price": round(self.baseline_position_entry_price, 10),
            },
            "grid_position_net_qty": round(self._grid_position_net_qty(), 8),
            "grid_position_qty": round(self._grid_position_qty(), 8),
            "expected_position_net_qty": round(self._expected_position_net_qty(), 8),
            "actual_grid_position_net_qty": round(
                self._last_actual_grid_position_net_qty, 8
            ),
            "position_ledger_consistent": self._position_mismatch_signature is None,
            "grid_profit_pct": round(self.grid_profit_pct, 6),
            "config": self.config,
        }

    def _fetch_precision(self):
        symbol = str(self.config.get("symbol") or "").upper()
        response = self.client.get_instrument_info(symbol)
        rules = validate_instrument_response(response, symbol=symbol)
        self.tick_size = rules["tick_size"]
        self.qty_step = rules["qty_step"]
        self.min_qty = float(rules["min_qty"])
        self.min_notional = float(rules["min_notional"])
        self.market_qty_step = rules["market_qty_step"]
        self.market_min_qty = float(rules["market_min_qty"])
        self.max_market_qty = float(rules["max_market_qty"])

    def _get_current_price(self) -> float:
        symbol = str(self.config.get("symbol") or "").upper()
        response = self.client.get_ticker(symbol)
        ticker = validate_ticker_response(response, symbol=symbol)
        last_price = float(ticker["last_price"])
        self.current_mark_price = float(ticker["mark_price"])
        return last_price

    def _calculate_levels(self) -> list[float]:
        lower = float(self.config["lower_price"])
        upper = float(self.config["upper_price"])
        count = int(self.config["grid_count"])
        grid_mode = self.config.get("grid_mode", "arithmetic")

        if grid_mode == "geometric":
            ratio = (upper / lower) ** (1 / count)
            return [round(lower * (ratio ** idx), 10) for idx in range(count + 1)]

        step = (upper - lower) / count
        return [round(lower + (step * idx), 10) for idx in range(count + 1)]

    def _calculate_grid_profit_pct(self, reference_price: float) -> float:
        if reference_price <= 0 or len(self.grid_levels) < 2:
            return 0.0

        if self.config.get("grid_mode", "arithmetic") == "geometric":
            return ((self.grid_levels[1] / self.grid_levels[0]) - 1) * 100

        step = self.grid_levels[1] - self.grid_levels[0]
        return (step / reference_price) * 100

    def _in_grid_range(self, price: float | None = None) -> bool:
        current = self.current_price if price is None else float(price)
        return float(self.config["lower_price"]) <= current <= float(self.config["upper_price"])

    def _calc_total_qty(self, reference_price: float) -> float:
        total_investment = float(self.config["total_investment"])
        leverage = float(self.config["leverage"])
        return (total_investment * leverage) / reference_price

    def _position_sizing_mode(self) -> str:
        return str(self.config.get("position_sizing_mode") or "investment").lower().strip()

    def _fixed_grid_qty_for_level(self, level_idx: int, fallback_qty: float | None = None) -> float:
        if self._position_sizing_mode() != "fixed_grid_qty":
            return float(fallback_qty or 0)

        raw_qty = (
            self.target_qty_by_level.get(str(level_idx))
            or self.config.get("grid_order_qty")
            or self.config.get("qty_per_grid")
            or fallback_qty
            or 0
        )
        steps = self._qty_to_steps(float(raw_qty))
        if steps <= 0:
            return float(fallback_qty or 0)
        return self._steps_to_qty(steps)

    def _counter_qty_for_order(self, order: dict) -> float:
        # Counter orders must match the actual executed quantity. Upsizing a
        # partial fill back to the fixed per-grid quantity creates overcommit,
        # trimmed orders, and confusing extra same-level orders.
        return float(order["qty"])

    def _fee_rate(self) -> float:
        return max(0.0, float(self.config.get("fee_rate", 0.0005) or 0))

    def _maker_fee_rate(self) -> float:
        if self.config.get("maker_fee_rate") is not None:
            return max(0.0, float(self.config.get("maker_fee_rate") or 0))
        return self._fee_rate()

    def _taker_fee_rate(self) -> float:
        if self.config.get("taker_fee_rate") is not None:
            return max(0.0, float(self.config.get("taker_fee_rate") or 0))
        return self._fee_rate()

    def _estimate_fee(self, volume: float, liquidity: str = "taker") -> float:
        rate = self._maker_fee_rate() if liquidity == "maker" else self._taker_fee_rate()
        return volume * rate

    def _record_trade_value(
        self,
        price: float,
        qty: float,
        gross_profit: float = 0.0,
        *,
        volume: float | None = None,
        fee: float | None = None,
        fee_asset: str = "USDT",
        fee_source: str = "estimated",
    ) -> dict:
        price_decimal = Decimal(str(price))
        qty_decimal = Decimal(str(qty))
        notional_decimal = (
            Decimal(str(volume))
            if volume is not None
            else price_decimal * qty_decimal
        )
        fee_decimal = (
            Decimal(str(fee))
            if fee is not None
            else Decimal(str(self._estimate_fee(float(notional_decimal))))
        )
        gross_profit_decimal = Decimal(str(gross_profit))
        net_profit_decimal = gross_profit_decimal - fee_decimal

        self.total_volume = float(
            Decimal(str(self.total_volume)) + notional_decimal
        )
        self.total_fee = float(Decimal(str(self.total_fee)) + fee_decimal)
        self.total_profit = float(
            Decimal(str(self.total_profit)) + net_profit_decimal
        )
        if gross_profit_decimal:
            self.gross_profit = float(
                Decimal(str(self.gross_profit)) + gross_profit_decimal
            )

        return {
            "volume": float(notional_decimal),
            "fee": float(fee_decimal),
            "gross_profit": float(gross_profit_decimal),
            "net_profit": float(net_profit_decimal),
            "fee_asset": fee_asset,
            "fee_source": fee_source,
        }

    def _get_trade_stats(
        self,
        order_id: str,
        fallback_price: float,
        fallback_qty: float,
        *,
        allow_estimate: bool = True,
        liquidity_hint: str = "taker",
    ) -> dict | None:
        fallback_volume = fallback_price * fallback_qty
        fallback_fee = self._estimate_fee(fallback_volume, liquidity_hint)
        stats = {
            "price": fallback_price,
            "qty": fallback_qty,
            "volume": fallback_volume,
            "fee": fallback_fee,
            "fee_asset": "USDT estimated",
            "fee_source": "estimated",
            "fee_conversion_complete": False,
            "maker_count": 1 if liquidity_hint == "maker" else 0,
            "taker_count": 1 if liquidity_hint != "maker" else 0,
        }

        if not order_id or not hasattr(self.client, "get_order_trades"):
            if not allow_estimate:
                return None
            return stats

        try:
            resp = self.client.get_order_trades(self.config["symbol"], order_id)
        except Exception as exc:
            logger.warning("Fetch trade details failed order_id=%s msg=%s", order_id, exc)
            if not allow_estimate:
                return None
            return stats

        if not isinstance(resp, dict):
            logger.warning(
                "Fetch trade details returned malformed response order_id=%s",
                order_id,
            )
            if not allow_estimate:
                return None
            return stats

        if resp.get("retCode") != 0:
            logger.warning(
                "Fetch trade details rejected order_id=%s msg=%s",
                order_id,
                resp.get("retMsg"),
            )
            if not allow_estimate:
                return None
            return stats

        try:
            trades = validate_execution_response(
                resp,
                expected_symbol=self.config["symbol"],
                expected_order_id=order_id,
                require_identity=False,
            )
        except RuntimeError as exc:
            logger.warning(
                "Fetch trade details failed validation order_id=%s msg=%s",
                order_id,
                exc,
            )
            if not allow_estimate:
                return None
            return stats
        if not trades:
            if not allow_estimate:
                return None
            return stats

        # Exchange quantities are decimal strings. Summing them as binary
        # floats can turn an exact 0.18 + 0.02 fill into 0.19999999999999998;
        # flooring that value to a 0.01 step then places only 0.19. Keep the
        # cumulative execution exact until the public stats boundary.
        total_qty = Decimal("0")
        total_volume = Decimal("0")
        total_fee = Decimal("0")
        fee_assets: set[str] = set()
        fee_conversion_sources: set[str] = set()
        converted_all = True
        maker_count = 0
        taker_count = 0

        for validated_trade in trades:
            trade = validated_trade["raw"]
            qty = validated_trade["qty"]
            volume = validated_trade["volume"]
            fee_usdt = validated_trade["fee_usdt"]
            fee_asset = validated_trade["fee_asset"]
            fee_assets.add(fee_asset)
            if validated_trade["is_maker"]:
                maker_count += 1
            else:
                taker_count += 1

            total_qty += qty
            total_volume += volume
            if fee_usdt is not None:
                total_fee += fee_usdt
                fee_conversion_sources.add(
                    str(trade.get("feeUsdtSource") or "exchange_reported")
                )
            else:
                converted_all = False
                fee_conversion_sources.add("estimated")
                total_fee += Decimal(
                    str(
                        self._estimate_fee(
                            float(volume),
                            "maker" if validated_trade["is_maker"] else "taker",
                        )
                    )
                )

        if total_qty <= 0 or total_volume <= 0:
            if not allow_estimate:
                return None
            return stats

        return {
            "price": float(total_volume / total_qty),
            "qty": float(total_qty),
            "volume": float(total_volume),
            "fee": float(total_fee),
            "fee_asset": ",".join(sorted(fee_assets)) if fee_assets else "USDT",
            "fee_source": "exchange" if converted_all else "mixed",
            "fee_conversion_source": ",".join(sorted(fee_conversion_sources)),
            "fee_conversion_complete": converted_all,
            "maker_count": maker_count,
            "taker_count": taker_count,
        }

    def _fill_delta_stats(self, order: dict, stats: dict) -> dict | None:
        planned_qty = Decimal(str(order.get("qty", 0) or 0))
        total_qty = min(Decimal(str(stats.get("qty", 0) or 0)), planned_qty)
        processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
        delta_qty = total_qty - processed_qty
        if delta_qty <= self._qty_tolerance_decimal():
            return None

        total_volume = Decimal(str(stats.get("volume", 0) or 0))
        total_fee = Decimal(str(stats.get("fee", 0) or 0))
        processed_volume = Decimal(str(order.get("processed_fill_volume", 0) or 0))
        processed_fee = Decimal(str(order.get("processed_fill_fee", 0) or 0))
        delta_volume = total_volume - processed_volume
        if delta_volume <= 0:
            delta_volume = Decimal(str(stats.get("price", order.get("price", 0)) or 0)) * delta_qty
        # Exchange trade snapshots can be temporarily incomplete or fee-asset
        # conversion can move between polls. Cumulative accounting must never
        # reverse a fee that was already recorded.
        delta_fee = max(Decimal("0"), total_fee - processed_fee)
        delta_price = delta_volume / delta_qty if delta_qty > 0 else Decimal(str(stats.get("price", 0) or 0))

        return {
            **stats,
            "price": float(delta_price),
            "qty": float(delta_qty),
            "volume": float(delta_volume),
            "fee": float(delta_fee),
        }

    def _mark_order_fill_processed(self, order: dict, stats: dict):
        planned_qty = Decimal(str(order.get("qty", 0) or 0))
        processed_qty = min(Decimal(str(stats.get("qty", 0) or 0)), planned_qty)
        order["processed_fill_qty"] = float(processed_qty)
        order["processed_fill_volume"] = float(
            max(
                Decimal(str(order.get("processed_fill_volume", 0) or 0)),
                Decimal(str(stats.get("volume", 0) or 0)),
            )
        )
        order["processed_fill_fee"] = float(
            max(
                Decimal(str(order.get("processed_fill_fee", 0) or 0)),
                Decimal(str(stats.get("fee", 0) or 0)),
            )
        )

    def _record_execution_delta(
        self,
        order: dict,
        stats: dict,
        *,
        place_counter: bool = True,
    ) -> bool:
        delta_stats = self._fill_delta_stats(order, stats)
        if not delta_stats:
            return False
        self._mark_order_fill_processed(order, stats)

        count_completed_pair = bool(order.get("reduce_only")) and not bool(
            order.get("completed_pair_counted")
        )
        if count_completed_pair:
            order["completed_pair_counted"] = True
        filled_order = {**order, "qty": str(delta_stats["qty"]), "fill_price": delta_stats["price"]}
        self._record_fill(
            filled_order,
            delta_stats,
            count_completed_pair=count_completed_pair,
            persist_state=False,
        )
        if not place_counter:
            self._persist_state()
            return True
        if filled_order.get("tag") == "boundary_reduce_fallback":
            self._persist_state()
            self._repair_flat_open_side_grid()
            return True

        plan = self._counter_order_plan(filled_order)
        if not plan:
            self._persist_state()
            return True

        # Commit the execution and its exact counter-order identity together.
        # A crash can then replay the counter safely without losing the grid leg
        # or submitting a second order under a different client ID.
        filled_order["replacement_mode"] = "counter_order"
        filled_order["replacement_link_id"] = (
            f"g_{int(plan['level_idx'])}_{str(plan['side'])[0]}_"
            f"{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
        )
        self.paused_replacements.append(filled_order)
        self.trigger_message = (
            "Counter order is durably queued and awaiting exchange confirmation."
        )
        self._mark_fast_poll()
        self._persist_state()
        self._resume_paused_replacements()
        return True

    @staticmethod
    def _liquidity_label(stats: dict) -> str:
        maker_count = int(stats.get("maker_count", 0) or 0)
        taker_count = int(stats.get("taker_count", 0) or 0)
        if maker_count and not taker_count:
            return "maker"
        if taker_count and not maker_count:
            return "taker"
        if maker_count or taker_count:
            return "mixed"
        return "unknown"

    def _qty_to_steps(self, qty: float) -> int:
        step_decimal = Decimal(self.qty_step)
        qty_decimal = self._normalized_qty_decimal(qty, step_decimal)
        return int((qty_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN))

    @staticmethod
    def _normalized_qty_decimal(value: Decimal | float | str, step: Decimal | str) -> Decimal:
        """Remove binary-float dust without rounding a genuinely smaller quantity up."""
        try:
            qty = max(Decimal(str(value)), Decimal("0"))
            step_decimal = abs(Decimal(str(step)))
        except Exception:
            return Decimal("0")
        if step_decimal <= 0:
            return qty

        nearest_steps = (qty / step_decimal).to_integral_value(rounding=ROUND_HALF_UP)
        nearest_qty = nearest_steps * step_decimal
        tolerance = max(step_decimal * Decimal("1e-9"), Decimal("1e-18"))
        if abs(qty - nearest_qty) <= tolerance:
            return nearest_qty
        return qty

    def _qty_step_decimal(self) -> Decimal:
        try:
            step = Decimal(str(self.qty_step))
        except Exception:
            step = Decimal("0")
        if step <= 0:
            step = Decimal(str(self.min_qty or 0))
        return step if step > 0 else Decimal("1e-12")

    def _qty_tolerance_decimal(self) -> Decimal:
        return max(self._qty_step_decimal() * Decimal("1e-9"), Decimal("1e-18"))

    def _qty_reaches_accounting_step(self, qty: Decimal | float) -> bool:
        try:
            amount = abs(Decimal(str(qty)))
        except Exception:
            return False
        return amount + self._qty_tolerance_decimal() >= self._qty_step_decimal()

    def _steps_to_qty(self, steps: int) -> float:
        return float(Decimal(self.qty_step) * Decimal(steps))

    def _fp(self, value: float) -> str:
        return self.client.round_to_step(value, self.tick_size)

    def _fq(self, value: float) -> str:
        normalized = self._normalized_qty_decimal(value, self.qty_step)
        return self.client.round_to_step(str(normalized), self.qty_step)

    def _order_qty_text(self, value: float, *, reduce_only: bool) -> str:
        qty_text = self._fq(value)
        try:
            qty = Decimal(str(qty_text))
        except Exception:
            return ""
        if qty + self._qty_tolerance_decimal() < self._qty_step_decimal():
            return ""
        if not reduce_only and qty + self._qty_tolerance_decimal() < Decimal(str(self.min_qty)):
            return ""
        return qty_text

    def _limit_notional(self, price_text: str, qty_text: str) -> Decimal:
        try:
            return Decimal(str(price_text)) * Decimal(str(qty_text))
        except Exception:
            return Decimal("0")

    def _meets_min_notional(self, price_text: str, qty_text: str) -> bool:
        minimum = Decimal(str(self.min_notional or 0))
        if minimum <= 0:
            return True
        tolerance = max(minimum * Decimal("1e-12"), Decimal("1e-18"))
        return self._limit_notional(price_text, qty_text) + tolerance >= minimum

    def _round_trip_open_price_text(self, level_idx: int) -> str:
        if level_idx < 0 or level_idx + 1 >= len(self.grid_levels):
            return "0"
        price_index = level_idx + 1 if self.config.get("direction") == "short" else level_idx
        return self._fp(self.grid_levels[price_index])

    def _market_order_qty_text(self, value: float, *, reduce_only: bool) -> str:
        step = Decimal(str(self.market_qty_step or self.qty_step))
        normalized = self._normalized_qty_decimal(value, step)
        qty_text = self.client.round_to_step(str(normalized), str(step))
        try:
            qty = Decimal(str(qty_text))
        except Exception:
            return ""
        tolerance = max(abs(step) * Decimal("1e-9"), Decimal("1e-18"))
        if qty + tolerance < step:
            return ""
        if not reduce_only and qty + tolerance < Decimal(str(self.market_min_qty)):
            return ""
        return qty_text

    def _has_active_order(self, side: str, level_idx: int, reduce_only: bool) -> bool:
        for order in self.active_orders.values():
            if (
                order["side"] == side
                and order["level_idx"] == level_idx
                and order["reduce_only"] == reduce_only
            ):
                return True
        return False

    def _active_order_qty(self, side: str, level_idx: int, reduce_only: bool) -> float:
        total = Decimal("0")
        for order in self.active_orders.values():
            if (
                order.get("side") == side
                and int(order.get("level_idx", 0) or 0) == level_idx
                and bool(order.get("reduce_only")) == reduce_only
            ):
                total += Decimal(str(order.get("qty", 0) or 0))
        return float(total)

    def _active_order_remaining_qty(self, side: str, level_idx: int, reduce_only: bool) -> float:
        total = Decimal("0")
        for order in self.active_orders.values():
            if (
                order.get("side") == side
                and int(order.get("level_idx", 0) or 0) == level_idx
                and bool(order.get("reduce_only")) == reduce_only
            ):
                planned = Decimal(str(order.get("qty", 0) or 0))
                processed = Decimal(str(order.get("processed_fill_qty", 0) or 0))
                remaining = planned - processed
                if remaining > 0:
                    total += remaining
        return float(total)

    def _active_order_remaining_qty_deficit(
        self, side: str, level_idx: int, reduce_only: bool, qty: float
    ) -> float:
        requested = Decimal(str(qty))
        active = Decimal(str(self._active_order_remaining_qty(side, level_idx, reduce_only)))
        deficit = requested - active
        # This method measures ledger coverage, not whether a new order can be
        # submitted immediately. A valid step-sized deficit below minQty must
        # reach _place(), whose rejection keeps the exact counter task queued
        # until matching fragments can be coalesced.
        if deficit <= 0 or not self._qty_reaches_accounting_step(deficit):
            return 0.0
        return float(self._normalized_qty_decimal(deficit, self.qty_step))

    def _active_order_qty_deficit(self, side: str, level_idx: int, reduce_only: bool, qty: float) -> float:
        requested = Decimal(str(qty))
        active = Decimal(str(self._active_order_qty(side, level_idx, reduce_only)))
        deficit = requested - active
        minimum_order_qty = max(Decimal(self.qty_step), Decimal(str(self.min_qty)))
        if deficit < minimum_order_qty:
            return 0.0
        return float(deficit)

    def _active_reduce_qty(self, side: str) -> float:
        total = Decimal("0")
        for order in self.active_orders.values():
            if order.get("side") != side or not order.get("reduce_only"):
                continue
            planned = Decimal(str(order.get("qty", 0) or 0))
            processed = Decimal(str(order.get("processed_fill_qty", 0) or 0))
            remaining = planned - processed
            if remaining > 0:
                total += remaining
        return float(total)

    def reduce_protection_snapshot(self) -> dict:
        direction = self.config.get("direction")
        reduce_side = self._reduce_side()
        grid_qty = Decimal(str(self._grid_position_qty()))
        active_total = Decimal(str(self._active_reduce_qty(reduce_side))) if reduce_side else Decimal("0")
        pending_submission_count = sum(
            1
            for order in self.active_orders.values()
            if order.get("submission_pending")
            and order.get("reduce_only")
            and order.get("side") == reduce_side
        )

        snapshot = {
            "enabled": direction in {"long", "short"} and bool(reduce_side),
            "ledger_ok": True,
            "ledger_reason": "",
            "grid_qty": float(grid_qty),
            "active_reduce_qty": float(active_total),
            "expected_reduce_qty": 0.0,
            "missing_by_level": [],
            "excess_by_level": [],
            "has_level_gap": False,
            "pending_submission_count": pending_submission_count,
            "has_risk": bool(pending_submission_count),
        }
        if not snapshot["enabled"] or not self._qty_reaches_accounting_step(grid_qty):
            return snapshot

        lots, reason = self._reduce_lots_for_repair()
        if lots is None:
            snapshot["ledger_ok"] = False
            snapshot["ledger_reason"] = reason or "reduce protection ledger is incomplete"
            stored_lots = self._reduce_lot_decimal_map()
            snapshot["has_risk"] = True
            if stored_lots:
                lots = stored_lots
            else:
                return snapshot

        expected_total = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        snapshot["expected_reduce_qty"] = float(expected_total)
        if self._qty_reaches_accounting_step(expected_total - grid_qty):
            snapshot["ledger_ok"] = False
            snapshot["ledger_reason"] = (
                f"reduce protection ledger qty {float(expected_total)} "
                f"does not match grid qty {float(grid_qty)}"
            )
            snapshot["has_risk"] = True

        level_indexes = set(lots)
        for order in self.active_orders.values():
            if order.get("side") == reduce_side and order.get("reduce_only"):
                level_indexes.add(int(order.get("level_idx", 0) or 0))

        for level_idx in sorted(level_indexes):
            expected_qty = Decimal("0")
            if level_idx in lots:
                expected_qty = lots[level_idx]["qty"]
            active_qty = Decimal(str(self._active_order_remaining_qty(reduce_side, level_idx, True)))
            diff = expected_qty - active_qty
            if diff > 0 and self._qty_reaches_accounting_step(diff):
                snapshot["missing_by_level"].append(
                    {
                        "level": level_idx,
                        "price": self._fp(self.grid_levels[level_idx])
                        if 0 <= level_idx < len(self.grid_levels)
                        else "",
                        "expected_qty": float(expected_qty),
                        "active_qty": float(active_qty),
                        "missing_qty": float(diff),
                    }
                )
            elif diff < 0 and self._qty_reaches_accounting_step(diff):
                snapshot["excess_by_level"].append(
                    {
                        "level": level_idx,
                        "price": self._fp(self.grid_levels[level_idx])
                        if 0 <= level_idx < len(self.grid_levels)
                        else "",
                        "expected_qty": float(expected_qty),
                        "active_qty": float(active_qty),
                        "excess_qty": float(-diff),
                    }
                )

        snapshot["has_level_gap"] = bool(snapshot["missing_by_level"] or snapshot["excess_by_level"])
        snapshot["has_risk"] = bool(snapshot["has_risk"] or snapshot["has_level_gap"])
        return snapshot

    def grid_coverage_snapshot(self) -> dict:
        direction = self.config.get("direction")
        open_side = self._open_side_for_direction()
        pending_submission_count = sum(
            1
            for order in self.active_orders.values()
            if order.get("submission_pending")
            and not order.get("reduce_only")
            and order.get("side") == open_side
        )
        enabled = (
            direction in {"long", "short"}
            and bool(open_side)
            and self.grid_ready
            and len(self.grid_levels) >= 2
        )
        snapshot = {
            "enabled": enabled,
            "ledger_ok": True,
            "ledger_reason": "",
            "open_side": open_side,
            "target_qty": 0.0,
            "position_lot_qty": 0.0,
            "open_order_remaining_qty": 0.0,
            "coverage_qty": 0.0,
            "net_delta_qty": 0.0,
            "missing_by_level": [],
            "excess_by_level": [],
            "pending_submission_count": pending_submission_count,
            "has_risk": bool(pending_submission_count),
        }
        if not enabled:
            return snapshot

        if not self.reduce_lots_complete:
            snapshot["ledger_ok"] = False
            snapshot["ledger_reason"] = "position lot ledger is incomplete"
            snapshot["has_risk"] = True
            return snapshot

        lots = self._reduce_lot_decimal_map()
        target_total = Decimal("0")
        lot_total = Decimal("0")
        open_total = Decimal("0")

        for level_idx in range(len(self.grid_levels) - 1):
            target_qty = Decimal(str(self._target_open_qty_for_level(level_idx)))
            lot_qty = lots.get(level_idx, {}).get("qty", Decimal("0"))
            active_open_qty = Decimal(
                str(self._active_order_remaining_qty(open_side, level_idx, False))
            )
            coverage_qty = lot_qty + active_open_qty
            delta = coverage_qty - target_qty

            target_total += target_qty
            lot_total += lot_qty
            open_total += active_open_qty

            details = {
                "level": level_idx,
                "target_qty": float(target_qty),
                "position_lot_qty": float(lot_qty),
                "open_order_remaining_qty": float(active_open_qty),
                "coverage_qty": float(coverage_qty),
            }
            if delta < 0 and self._qty_reaches_accounting_step(delta):
                snapshot["missing_by_level"].append(
                    {**details, "missing_qty": float(-delta)}
                )
            elif delta > 0 and self._qty_reaches_accounting_step(delta):
                snapshot["excess_by_level"].append(
                    {**details, "excess_qty": float(delta)}
                )

        coverage_total = lot_total + open_total
        snapshot.update(
            {
                "target_qty": float(target_total),
                "position_lot_qty": float(lot_total),
                "open_order_remaining_qty": float(open_total),
                "coverage_qty": float(coverage_total),
                "net_delta_qty": float(coverage_total - target_total),
                "has_risk": bool(
                    snapshot["has_risk"]
                    or snapshot["missing_by_level"]
                    or snapshot["excess_by_level"]
                ),
            }
        )
        return snapshot

    def _reduce_side(self) -> str:
        direction = self.config["direction"]
        if direction == "long":
            return "Sell"
        if direction == "short":
            return "Buy"
        return ""

    def _is_grid_reduce_side(self, side: str, reduce_only: bool) -> bool:
        return bool(reduce_only) and side == self._reduce_side()

    def _available_reduce_qty(self, side: str) -> float:
        if not self._is_grid_reduce_side(side, True):
            return float("inf")
        available = Decimal(str(self._grid_position_qty())) - Decimal(str(self._active_reduce_qty(side)))
        if available <= 0:
            return 0.0
        return float(available)

    def _cap_reduce_order_qty(self, side: str, qty: float) -> float:
        # Normal grid orders must be deterministic: place the requested grid
        # quantity and let the exchange accept or reject it. Local capping made
        # live order quantities drift from the configured grid.
        return qty

    def _allocate_qtys(self, raw_total_qty: float, target_count: int) -> list[float]:
        if target_count <= 0:
            return []
        total_steps = self._qty_to_steps(raw_total_qty)
        if total_steps < target_count:
            return []
        base_steps = total_steps // target_count
        remainder_steps = total_steps % target_count
        return [
            self._steps_to_qty(base_steps + (1 if index < remainder_steps else 0))
            for index in range(target_count)
        ]

    def _sort_reduce_orders_for_trim(self, reduce_side: str) -> list[tuple[str, dict]]:
        orders = [
            (link_id, order)
            for link_id, order in self.active_orders.items()
            if order.get("side") == reduce_side and order.get("reduce_only")
        ]
        reverse = reduce_side == "Buy"
        return sorted(
            orders,
            key=lambda item: float(item[1].get("price", 0) or 0),
            reverse=reverse,
        )

    def _trim_reduce_overcommit(self) -> bool:
        return False

    def _normalize_fixed_reduce_protection(self) -> bool:
        # Never reshape reduce-only protection by total quantity alone. Safety
        # depends on the exact grid level/price each reduce lot protects.
        return False

    def _halt_if_baseline_breached(self) -> bool:
        if not self.baseline_position_side or not self._qty_reaches_accounting_step(
            self.baseline_position_qty
        ):
            return False
        if self.config["direction"] not in {"long", "short"}:
            return False

        actual_same_side_qty = self._position_size(self.baseline_position_side)
        shortfall = Decimal(str(self.baseline_position_qty)) - Decimal(str(actual_same_side_qty))
        if shortfall <= 0 or not self._qty_reaches_accounting_step(shortfall):
            return False

        self.trigger_message = (
            f"Baseline position protection halted grid: expected at least "
            f"{self.baseline_position_qty:g} {self.baseline_position_side}, "
            f"exchange has {actual_same_side_qty:g}."
        )
        logger.error(
            "Baseline position breached symbol=%s baseline_side=%s baseline_qty=%s actual_qty=%s",
            self.config.get("symbol"),
            self.baseline_position_side,
            self.baseline_position_qty,
            actual_same_side_qty,
        )
        self._stopping = True
        all_terminal = False
        try:
            all_terminal = self._cancel_managed_orders_once()
            if not all_terminal:
                self.trigger_message += (
                    " Managed orders with unconfirmed terminal status were retained for audit "
                    "and safe retry."
                )
        except Exception as exc:
            logger.warning(
                "Failed to reconcile managed orders after baseline breach symbol=%s msg=%s",
                self.config.get("symbol"),
                exc,
            )
            self.trigger_message += " Managed-order cancellation remains unconfirmed."
        self.grid_ready = False
        if all_terminal and not self.pending_reduce_action:
            self.running = False
            self.manual_stop_pending = False
            self._clear_restore_refresh_state()
        else:
            # A one-shot cancellation failure must not leave live orders orphaned.
            # Keep only the cleanup state machine running; normal grid placement
            # stays disabled because _stopping and manual_stop_pending are set.
            self.running = True
            self.manual_stop_pending = True
            self._mark_fast_poll()
        self._persist_state()
        return True

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        return bool(value)

    @staticmethod
    def _parse_grid_link_id(link_id: str) -> tuple[int, str] | None:
        match = re.match(r"^g_(\d+)_([BS])_", str(link_id or ""))
        if not match:
            return None
        return int(match.group(1)), "Buy" if match.group(2) == "B" else "Sell"

    @staticmethod
    def _is_grid_managed_link(link_id: str) -> bool:
        return str(link_id or "").startswith(("g_", "open_", "init_", "repair_"))

    def _validate_open_order_snapshot(self, open_orders: Any) -> list[dict]:
        if not isinstance(open_orders, list):
            raise RuntimeError("Invalid open-order snapshot: expected a list")

        managed_orders = [
            order for order in self.active_orders.values() if isinstance(order, dict)
        ]
        if isinstance(self.opening_order, dict):
            managed_orders.append(self.opening_order)

        managed_by_order_id: dict[str, dict] = {}
        managed_by_link_id: dict[str, dict] = {}
        for order in managed_orders:
            order_id = str(order.get("order_id", "") or "")
            link_id = str(order.get("link_id", "") or "")
            if order_id:
                existing = managed_by_order_id.get(order_id)
                if existing is not None and existing is not order:
                    raise RuntimeError(
                        "Invalid open-order snapshot context: duplicate managed order ID"
                    )
                managed_by_order_id[order_id] = order
            if link_id:
                existing = managed_by_link_id.get(link_id)
                if existing is not None and existing is not order:
                    raise RuntimeError(
                        "Invalid open-order snapshot context: duplicate managed client order ID"
                    )
                managed_by_link_id[link_id] = order

        validated: list[dict] = []
        seen_order_ids: set[str] = set()
        seen_link_ids: set[str] = set()
        for index, item in enumerate(open_orders):
            snapshot = validate_order_row(
                item,
                expected_symbol=str(self.config.get("symbol") or ""),
                require_details=False,
                allowed_statuses=OPEN_ORDER_STATUSES,
                row_index=index,
                context="Invalid open-order snapshot",
            )
            order_id = snapshot["order_id"]
            link_id = snapshot["link_id"]
            if order_id in seen_order_ids:
                raise RuntimeError(
                    "Invalid open-order snapshot: duplicate exchange order ID"
                )
            if link_id and link_id in seen_link_ids:
                raise RuntimeError(
                    "Invalid open-order snapshot: duplicate client order ID"
                )

            managed_by_id = managed_by_order_id.get(order_id)
            if managed_by_id is not None:
                expected_link = str(managed_by_id.get("link_id", "") or "")
                if expected_link != link_id:
                    raise RuntimeError(
                        "Invalid open-order snapshot: managed order ID maps to a different "
                        "client order ID"
                    )
            managed_by_link = managed_by_link_id.get(link_id) if link_id else None
            if managed_by_link is not None:
                expected_order_id = str(managed_by_link.get("order_id", "") or "")
                if expected_order_id and expected_order_id != order_id:
                    raise RuntimeError(
                        "Invalid open-order snapshot: managed client order ID maps to a "
                        "different exchange order ID"
                    )

            managed_order = managed_by_id or managed_by_link
            if managed_order is not None:
                required_fields = ["side", "qty", "reduceOnly"]
                if str(managed_order.get("order_type") or "").lower() == "limit":
                    required_fields.append("price")
                missing_fields = [
                    field
                    for field in required_fields
                    if field not in item or item.get(field) in (None, "")
                ]
                if missing_fields:
                    raise RuntimeError(
                        "Invalid open-order snapshot: managed order is missing "
                        + ", ".join(missing_fields)
                    )

            seen_order_ids.add(order_id)
            if link_id:
                seen_link_ids.add(link_id)
            validated.append(item)
        return validated

    def _fetch_open_orders(self) -> list[dict]:
        resp = self.client.get_open_orders(self.config["symbol"])
        if not isinstance(resp, dict):
            raise RuntimeError("Invalid open-order snapshot response: expected an object")
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch open orders"))
        result = resp.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("Invalid open-order snapshot response: missing result object")
        return self._validate_open_order_snapshot(result.get("list"))

    def _pending_limit_order_state(
        self,
        *,
        link_id: str,
        level_idx: int,
        side: str,
        price: str,
        qty: str,
        reduce_only: bool,
        entry_price: float | None,
        time_in_force: str,
        tag: str | None = None,
    ) -> dict:
        state = {
            "link_id": link_id,
            "order_id": "",
            "level_idx": level_idx,
            "side": side,
            "price": price,
            "qty": qty,
            "status": "SUBMITTING",
            "order_type": "Limit",
            "time_in_force": time_in_force,
            "reduce_only": reduce_only,
            "entry_price": entry_price,
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
            "submission_pending": True,
            "submission_attempts": 1,
            "submission_not_found_count": 0,
            "submission_last_not_found_at": 0.0,
            "submission_updated_at": time.time(),
        }
        if tag:
            state["tag"] = tag
        return state

    def _accepted_shape_mismatch_reason(self, order: dict, snapshot: dict) -> str:
        snapshot_side = str(snapshot.get("side", "") or "")
        expected_side = str(order.get("side", "") or "")
        if snapshot_side and snapshot_side.lower() != expected_side.lower():
            return f"side expected={expected_side} actual={snapshot_side}"

        snapshot_reduce_only = snapshot.get("reduceOnly")
        expected_reduce_only = bool(order.get("reduce_only", False))
        accepted_reduce_only: bool | None = None
        if isinstance(snapshot_reduce_only, bool):
            accepted_reduce_only = snapshot_reduce_only
        elif isinstance(snapshot_reduce_only, str):
            reduce_text = snapshot_reduce_only.strip().lower()
            if reduce_text in {"true", "false"}:
                accepted_reduce_only = reduce_text == "true"
        elif snapshot_reduce_only is not None:
            return f"reduce-only expected={expected_reduce_only} actual={snapshot_reduce_only}"
        if snapshot_reduce_only is not None and accepted_reduce_only is None:
            return f"reduce-only expected={expected_reduce_only} actual={snapshot_reduce_only}"
        if accepted_reduce_only is not None and accepted_reduce_only != expected_reduce_only:
            return (
                f"reduce-only expected={expected_reduce_only} "
                f"actual={accepted_reduce_only}"
            )

        snapshot_qty = snapshot.get("qty")
        if snapshot_qty not in (None, ""):
            try:
                expected_qty = Decimal(str(order.get("qty", 0) or 0))
                accepted_qty = Decimal(str(snapshot_qty))
            except Exception:
                return f"quantity expected={order.get('qty')} actual={snapshot_qty}"
            if (
                not expected_qty.is_finite()
                or not accepted_qty.is_finite()
                or accepted_qty <= 0
                or abs(accepted_qty - expected_qty) > self._qty_tolerance_decimal()
            ):
                return f"quantity expected={order.get('qty')} actual={snapshot_qty}"

        snapshot_price = snapshot.get("price")
        if (
            str(order.get("order_type", "")).lower() == "limit"
            and snapshot_price not in (None, "")
        ):
            try:
                expected_price = Decimal(str(order.get("price", 0) or 0))
                accepted_price = Decimal(str(snapshot_price))
                tick = Decimal(str(self.tick_size or 0))
                tolerance = max(abs(tick) * Decimal("1e-9"), Decimal("1e-18"))
            except Exception:
                return f"price expected={order.get('price')} actual={snapshot_price}"
            if (
                not expected_price.is_finite()
                or not accepted_price.is_finite()
                or accepted_price <= 0
                or abs(accepted_price - expected_price) > tolerance
            ):
                return f"price expected={order.get('price')} actual={snapshot_price}"

        return ""

    def _record_accepted_shape_mismatch(
        self,
        order: dict,
        snapshot: dict,
        reason: str,
    ) -> None:
        order_id = str(snapshot.get("orderId", "") or "")
        if not order_id:
            return
        order["expected_side"] = order.get("side")
        order["expected_price"] = order.get("price")
        order["expected_qty"] = order.get("qty")
        order["expected_reduce_only"] = bool(order.get("reduce_only", False))
        order["order_id"] = order_id
        order["status"] = "ACCEPTED_SHAPE_MISMATCH"
        order["accepted_shape_mismatch"] = str(reason)
        for source, target in (
            ("side", "exchange_accepted_side"),
            ("price", "exchange_accepted_price"),
            ("qty", "exchange_accepted_qty"),
            ("reduceOnly", "exchange_accepted_reduce_only"),
        ):
            if snapshot.get(source) not in (None, ""):
                order[target] = snapshot.get(source)
        accepted_side = str(snapshot.get("side", "") or "")
        if accepted_side:
            order["side"] = (
                "Buy"
                if accepted_side.upper() == "BUY"
                else "Sell"
                if accepted_side.upper() == "SELL"
                else accepted_side
            )
        if snapshot.get("price") not in (None, ""):
            order["price"] = str(snapshot.get("price"))
        if snapshot.get("qty") not in (None, ""):
            order["qty"] = str(snapshot.get("qty"))
        if snapshot.get("reduceOnly") is not None:
            order["reduce_only"] = self._truthy(snapshot.get("reduceOnly"))
        for field in (
            "submission_pending",
            "submission_attempts",
            "submission_not_found_count",
            "submission_last_not_found_at",
            "submission_updated_at",
            "submission_retry_blocked",
            "submission_retry_safe",
        ):
            order.pop(field, None)
        self.grid_ready = False
        self.manual_stop_pending = True
        self.initialization_in_progress = False
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger.clear()
        self.trigger_message = (
            f"Exchange accepted a grid order with a different shape ({reason}); "
            "new placement is stopped and managed orders will be cancelled without "
            "market-closing the retained position."
        )
        self._mark_fast_poll()
        self._persist_state()

    def _confirm_pending_submission(self, order: dict, snapshot: dict) -> bool:
        snapshot_link_id = str(snapshot.get("orderLinkId", "") or "")
        expected_link_id = str(order.get("link_id", "") or "")
        if expected_link_id and not snapshot_link_id:
            logger.warning(
                "Pending submission acknowledgement omitted the client order ID "
                "symbol=%s expected=%s",
                self.config.get("symbol"),
                expected_link_id,
            )
            return False
        if snapshot_link_id and snapshot_link_id != str(order.get("link_id", "")):
            logger.error(
                "Pending submission lookup returned a different client order ID "
                "symbol=%s expected=%s actual=%s",
                self.config.get("symbol"),
                order.get("link_id"),
                snapshot_link_id,
            )
            return False
        mismatch_reason = self._accepted_shape_mismatch_reason(order, snapshot)
        if mismatch_reason:
            logger.error(
                "Pending submission lookup returned a different immutable order shape "
                "symbol=%s link_id=%s reason=%s",
                self.config.get("symbol"),
                order.get("link_id"),
                mismatch_reason,
            )
            self._record_accepted_shape_mismatch(order, snapshot, mismatch_reason)
            return False
        order_id = str(snapshot.get("orderId", "") or "")
        if not order_id:
            return False
        order["order_id"] = order_id
        accepted_price = snapshot.get("avgPrice")
        try:
            has_positive_average = float(accepted_price or 0) > 0
        except (TypeError, ValueError):
            has_positive_average = False
        if not has_positive_average:
            accepted_price = snapshot.get("price")
        try:
            has_positive_price = float(accepted_price or 0) > 0
        except (TypeError, ValueError):
            has_positive_price = False
        if has_positive_price:
            order["price"] = str(accepted_price)
        if snapshot.get("qty") not in (None, ""):
            order["qty"] = str(snapshot.get("qty"))
        status = snapshot.get("orderStatus") or snapshot.get("status") or "open"
        order["status"] = str(status)
        if snapshot.get("reduceOnly") is not None:
            order["reduce_only"] = self._truthy(snapshot.get("reduceOnly"))
        for field in (
            "executedQty",
            "cumExecQty",
            "cumQty",
            "cum_exec_qty",
            "cumQuote",
            "cumExecValue",
            "cum_exec_value",
            "avgPrice",
            "avg_price",
            "averagePrice",
        ):
            if snapshot.get(field) not in (None, ""):
                order[field] = snapshot.get(field)
        order.pop("submission_pending", None)
        order.pop("submission_error", None)
        order.pop("submission_attempts", None)
        order.pop("submission_not_found_count", None)
        order.pop("submission_last_not_found_at", None)
        order.pop("submission_updated_at", None)
        order.pop("submission_retry_blocked", None)
        order.pop("submission_retry_safe", None)
        return True

    @staticmethod
    def _is_uncertain_submission_exception(exc: Exception) -> bool:
        uncertain_request_errors = (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        )
        if isinstance(
            exc,
            (
                ExchangeRequestUncertainError,
                TimeoutError,
                ConnectionError,
                *uncertain_request_errors,
            ),
        ):
            return True
        message = str(exc)
        return GridEngine._is_uncertain_submission_message(
            message
        ) or GridEngine._is_duplicate_client_order_rejection(message)

    @staticmethod
    def _is_uncertain_submission_message(message: str) -> bool:
        normalized = str(message or "").lower()
        return any(
            marker in normalized
            for marker in (
                "execution status unknown",
                "execution status is unknown",
                "send status unknown",
                "status is unknown",
                "request status unknown",
                "server timeout",
                "service unavailable",
                "internal server error",
                "response lost",
            )
        )

    @staticmethod
    def _is_uncertain_submission_result(result: dict[str, Any]) -> bool:
        try:
            code = int(result.get("retCode"))
        except (TypeError, ValueError):
            code = None
        if code in {
            -1006,
            -1007,
            10000,
            10014,
            10016,
            110072,
            170141,
        }:
            return True
        message = str(result.get("retMsg") or "")
        return GridEngine._is_uncertain_submission_message(
            message
        ) or GridEngine._is_duplicate_client_order_rejection(message)

    @staticmethod
    def _is_duplicate_client_order_rejection(message: str) -> bool:
        normalized = str(message or "").lower()
        return any(
            marker in normalized
            for marker in (
                "client order id is not unique",
                "clientorderid is not unique",
                "duplicate client order",
                "duplicate orderlinkid",
                "duplicated orderlinkid",
                "invalid duplicate request",
                "orderlinkedid is duplicate",
                "order link id is duplicate",
                "duplicate clientorderid",
            )
        )

    def _mark_submission_unknown(self, order: dict, error: Exception | str) -> None:
        order["status"] = "SUBMIT_UNKNOWN"
        order["submission_error"] = str(error)
        order["submission_updated_at"] = time.time()
        order["submission_not_found_count"] = 0
        order["submission_last_not_found_at"] = 0.0
        self._mark_fast_poll()
        self._persist_state()

    @staticmethod
    def _is_rate_limit_result(result: dict[str, Any]) -> bool:
        try:
            code = int(result.get("retCode"))
        except (TypeError, ValueError):
            code = None
        return code in {-1003, 418, 429, 10006} or is_exchange_rate_limit_message(
            result.get("retMsg")
        )

    def _mark_submission_rate_limited(
        self,
        order: dict,
        error: Exception | str,
        *,
        retry_after: float | None = None,
    ) -> None:
        delay = (
            float(retry_after)
            if retry_after is not None
            else ORDER_REJECTION_BACKOFF_MAX_SECONDS
        )
        order["status"] = "SUBMIT_RATE_LIMITED"
        order["submission_pending"] = True
        order["submission_error"] = str(error)
        order["submission_retry_safe"] = True
        order.pop("submission_retry_blocked", None)
        order["submission_updated_at"] = time.time()
        order["submission_not_found_count"] = 0
        order["submission_last_not_found_at"] = 0.0
        self._record_order_rejection(
            "",
            str(error),
            rate_limit_retry_after=delay,
            track_shape=False,
        )

    def _submission_snapshot_by_link(self, link_id: str) -> tuple[dict | None, bool]:
        if not link_id:
            logger.error(
                "Pending submission lookup has no client order ID symbol=%s",
                self.config.get("symbol"),
            )
            return None, False
        getter = getattr(self.client, "get_order_by_link", None)
        if not callable(getter):
            return None, False
        try:
            response = getter(self.config["symbol"], link_id)
        except Exception as exc:
            logger.warning(
                "Query pending submission by client order ID failed symbol=%s link_id=%s msg=%s",
                self.config.get("symbol"),
                link_id,
                exc,
            )
            return None, False
        if not isinstance(response, dict):
            logger.error(
                "Pending submission lookup returned an invalid response object "
                "symbol=%s link_id=%s",
                self.config.get("symbol"),
                link_id,
            )
            return None, False
        if response.get("retCode") != 0:
            return None, False
        result = response.get("result")
        if result == {}:
            return None, True
        if not isinstance(result, dict):
            logger.error(
                "Pending submission lookup returned no authoritative result object "
                "symbol=%s link_id=%s",
                self.config.get("symbol"),
                link_id,
            )
            return None, False

        snapshot = result
        if "list" in result:
            items = result.get("list")
            if not isinstance(items, list):
                logger.error(
                    "Pending submission lookup returned an invalid result list "
                    "symbol=%s link_id=%s",
                    self.config.get("symbol"),
                    link_id,
                )
                return None, False
            if not items:
                return None, True
            if len(items) != 1 or not isinstance(items[0], dict):
                logger.error(
                    "Pending submission lookup returned an ambiguous result list "
                    "symbol=%s link_id=%s count=%s",
                    self.config.get("symbol"),
                    link_id,
                    len(items),
                )
                return None, False
            snapshot = items[0]

        if not snapshot.get("orderId"):
            logger.error(
                "Pending submission lookup returned a non-empty result without an order ID "
                "symbol=%s link_id=%s",
                self.config.get("symbol"),
                link_id,
            )
            return None, False
        snapshot_link_id = str(snapshot.get("orderLinkId", "") or "")
        if snapshot_link_id != link_id:
            logger.error(
                "Pending submission lookup did not prove the requested client order ID "
                "symbol=%s expected=%s actual=%s",
                self.config.get("symbol"),
                link_id,
                snapshot_link_id or "missing",
            )
            return None, False
        return snapshot, True

    def _submission_history_by_link(self, links: set[str]) -> dict[str, dict]:
        if not links or not hasattr(self.client, "get_order_history"):
            return {}
        try:
            response = self.client.get_order_history(self.config["symbol"], limit=100)
        except Exception as exc:
            logger.warning(
                "Fetch order history for pending submissions failed symbol=%s msg=%s",
                self.config.get("symbol"),
                exc,
            )
            return {}
        if response.get("retCode") != 0:
            return {}
        return {
            str(item.get("orderLinkId", "") or ""): item
            for item in response.get("result", {}).get("list", [])
            if str(item.get("orderLinkId", "") or "") in links
        }

    def _retry_pending_submission(self, order: dict) -> str:
        if order.get("submission_retry_blocked"):
            return "pending"
        order_type = str(order.get("order_type") or "Limit")
        safe_retry = bool(order.get("submission_retry_safe"))
        if order_type.lower() == "market" and not safe_retry:
            # An exchange may allow a client order ID to be reused after the
            # original market order has already filled. Resubmitting an unknown
            # market write can therefore double the position even with the same
            # ID. Keep reconciling; an operator can explicitly stop/restart if
            # the exchange ultimately proves the original write never existed.
            order["status"] = "SUBMIT_UNRESOLVED"
            order["submission_retry_blocked"] = True
            order["submission_error"] = (
                "Automatic retry disabled for an unconfirmed market order"
            )
            self._persist_state()
            return "pending"
        attempts = int(order.get("submission_attempts", 1) or 1)
        if attempts >= SUBMISSION_MAX_RETRIES and not safe_retry:
            order["status"] = "SUBMIT_UNRESOLVED"
            return "pending"
        order["submission_attempts"] = attempts + 1
        order["submission_not_found_count"] = 0
        order["submission_last_not_found_at"] = 0.0
        order["submission_updated_at"] = time.time()
        order["status"] = "SUBMITTING"
        order.pop("submission_retry_safe", None)
        self._persist_state()
        try:
            result = self.client.place_order(
                symbol=self.config["symbol"],
                side=str(order["side"]),
                qty=str(order["qty"]),
                price=str(order["price"]) if order_type.lower() == "limit" else None,
                order_type=order_type,
                reduce_only=bool(order.get("reduce_only", False)),
                order_link_id=str(order["link_id"]),
                time_in_force=(
                    "PostOnly"
                    if order_type.lower() == "limit"
                    and order.get("time_in_force") == "PostOnly"
                    else None
                ),
            )
        except Exception as exc:
            if isinstance(exc, ExchangeRateLimitError) or is_exchange_rate_limit_message(exc):
                self._mark_submission_rate_limited(
                    order,
                    exc,
                    retry_after=(
                        exc.retry_after if isinstance(exc, ExchangeRateLimitError) else None
                    ),
                )
                return "pending"
            if self._is_uncertain_submission_exception(exc) or self._is_duplicate_client_order_rejection(
                str(exc)
            ):
                self._mark_submission_unknown(order, exc)
                return "pending"
            order["status"] = "SUBMIT_REJECTED"
            order["submission_error"] = str(exc)
            self._persist_state()
            return "rejected"
        if result.get("retCode") == 0:
            if self._confirm_pending_submission(order, result.get("result", {})):
                self._persist_state()
                return "confirmed"
            if order.get("accepted_shape_mismatch"):
                return "mismatch"
        message = str(result.get("retMsg") or "submission not confirmed")
        if self._is_rate_limit_result(result):
            self._mark_submission_rate_limited(order, message)
            return "pending"
        if (
            result.get("retCode") == 0
            or self._is_duplicate_client_order_rejection(message)
            or self._is_uncertain_submission_result(result)
        ):
            self._mark_submission_unknown(order, message)
            return "pending"
        order["status"] = "SUBMIT_REJECTED"
        order["submission_error"] = message
        self._persist_state()
        return "rejected"

    def _resolve_pending_submissions(self, open_orders: list[dict]) -> bool:
        pending = {
            link_id: order
            for link_id, order in self.active_orders.items()
            if order.get("submission_pending")
        }
        if not pending:
            return False

        changed = False
        open_by_link = {
            str(item.get("orderLinkId", "") or ""): item for item in open_orders
        }
        for link_id, order in list(pending.items()):
            snapshot = open_by_link.get(link_id)
            if snapshot and self._confirm_pending_submission(order, snapshot):
                pending.pop(link_id, None)
                changed = True
            elif order.get("accepted_shape_mismatch"):
                pending.pop(link_id, None)
                changed = True

        not_found_check_time = time.time()
        for link_id, order in list(pending.items()):
            snapshot, authoritative = self._submission_snapshot_by_link(link_id)
            if snapshot and self._confirm_pending_submission(order, snapshot):
                pending.pop(link_id, None)
                changed = True
                continue
            if order.get("accepted_shape_mismatch"):
                pending.pop(link_id, None)
                changed = True
                continue
            if authoritative:
                last_check = float(order.get("submission_last_not_found_at", 0) or 0)
                if (
                    not_found_check_time - last_check
                    >= SUBMISSION_NOT_FOUND_CHECK_INTERVAL_SECONDS
                ):
                    order["submission_not_found_count"] = int(
                        order.get("submission_not_found_count", 0) or 0
                    ) + 1
                    order["submission_last_not_found_at"] = not_found_check_time
                    changed = True

        history_by_link = self._submission_history_by_link(set(pending))
        for link_id, order in list(pending.items()):
            snapshot = history_by_link.get(link_id)
            if snapshot and self._confirm_pending_submission(order, snapshot):
                pending.pop(link_id, None)
                changed = True
            elif order.get("accepted_shape_mismatch"):
                pending.pop(link_id, None)
                changed = True

        now = time.time()
        for link_id, order in list(pending.items()):
            updated_at = float(order.get("submission_updated_at", 0) or 0)
            if now - updated_at < SUBMISSION_RETRY_SECONDS:
                continue
            if int(order.get("submission_not_found_count", 0) or 0) < SUBMISSION_REQUIRED_NOT_FOUND_CHECKS:
                continue
            outcome = self._retry_pending_submission(order)
            if outcome == "confirmed":
                pending.pop(link_id, None)
                changed = True
            elif outcome == "rejected":
                self.active_orders.pop(link_id, None)
                pending.pop(link_id, None)
                changed = True
                self._queue_exact_replacement(
                    order,
                    str(order.get("submission_error") or "pending submission rejected"),
                )
                logger.error(
                    "Pending submission was definitively rejected after exchange absence checks "
                    "symbol=%s link_id=%s msg=%s",
                    self.config.get("symbol"),
                    link_id,
                    order.get("submission_error"),
                )
            elif outcome == "mismatch":
                pending.pop(link_id, None)
                changed = True

        pending_count = sum(
            1 for order in self.active_orders.values() if order.get("submission_pending")
        )
        if pending_count:
            self.trigger_message = (
                f"{pending_count} order submission(s) are unconfirmed; "
                "reconciling by client order ID without creating duplicates."
            )
            self._mark_fast_poll()
        if changed or pending_count:
            self._persist_state()
        return changed

    def _resolve_opening_submission(self, open_orders: list[dict]) -> bool:
        order = self.opening_order
        if not order or not order.get("submission_pending"):
            return bool(order)

        link_id = str(order.get("link_id", "") or "")
        open_snapshot = next(
            (
                item
                for item in open_orders
                if str(item.get("orderLinkId", "") or "") == link_id
            ),
            None,
        )
        if open_snapshot and self._confirm_pending_submission(order, open_snapshot):
            self._persist_state()
            return True
        if order.get("accepted_shape_mismatch"):
            return False

        snapshot, authoritative = self._submission_snapshot_by_link(link_id)
        if snapshot and self._confirm_pending_submission(order, snapshot):
            self._persist_state()
            return True
        if order.get("accepted_shape_mismatch"):
            return False

        history_snapshot = self._submission_history_by_link({link_id}).get(link_id)
        if history_snapshot and self._confirm_pending_submission(order, history_snapshot):
            self._persist_state()
            return True
        if order.get("accepted_shape_mismatch"):
            return False

        now = time.time()
        if authoritative:
            last_check = float(order.get("submission_last_not_found_at", 0) or 0)
            if now - last_check >= SUBMISSION_NOT_FOUND_CHECK_INTERVAL_SECONDS:
                order["submission_not_found_count"] = int(
                    order.get("submission_not_found_count", 0) or 0
                ) + 1
                order["submission_last_not_found_at"] = now

        updated_at = float(order.get("submission_updated_at", 0) or 0)
        if (
            now - updated_at >= SUBMISSION_RETRY_SECONDS
            and int(order.get("submission_not_found_count", 0) or 0)
            >= SUBMISSION_REQUIRED_NOT_FOUND_CHECKS
        ):
            outcome = self._retry_pending_submission(order)
            if outcome == "confirmed":
                return True
            if outcome == "rejected":
                rejection = str(
                    order.get("submission_error") or "opening remainder was rejected"
                )
                if self._qty_reaches_accounting_step(self.opening_filled_qty):
                    self._fail_opening_completion(rejection)
                    return False
                self.waiting_initial_order = False
                self.opening_order = None
                self.running = False
                self.trigger_message = (
                    "Opening order was definitively rejected after submission recovery checks; "
                    "the grid was not deployed."
                )
                self._clear_restore_refresh_state()
                self._persist_state()
                return False

        self.trigger_message = (
            "Opening order submission is unconfirmed; reconciling by client order ID "
            "without opening another position."
        )
        self._mark_fast_poll()
        self._persist_state()
        return False

    def _adopt_exchange_grid_orders(self, open_orders: list[dict]) -> bool:
        known_order_ids = {
            str(order.get("order_id", ""))
            for order in self.active_orders.values()
            if str(order.get("order_id", ""))
        }
        adopted = False
        strict_ownership = bool(self.config.get("strict_order_ownership", False))
        ownership_conflicts: list[dict] = []
        for item in open_orders:
            order_id = str(item.get("orderId", "") or "")
            link_id = str(item.get("orderLinkId", "") or "")
            parsed = self._parse_grid_link_id(link_id)
            existing = self.active_orders.get(link_id)
            if existing:
                if existing.get("submission_pending") and self._confirm_pending_submission(
                    existing, item
                ):
                    known_order_ids.add(order_id)
                    adopted = True
                elif existing.get("accepted_shape_mismatch"):
                    known_order_ids.add(order_id)
                    adopted = True
                continue
            if not order_id or order_id in known_order_ids:
                continue

            if strict_ownership and self._is_grid_managed_link(link_id):
                ownership_conflicts.append(
                    {
                        "order_id": order_id,
                        "link_id": link_id,
                        "side": str(item.get("side") or ""),
                        "price": str(item.get("price", "0")),
                        "qty": str(item.get("qty", "0")),
                        "reduce_only": self._truthy(item.get("reduceOnly", False)),
                    }
                )
                continue
            if not parsed:
                continue

            level_idx, side_from_link = parsed
            side = str(item.get("side") or side_from_link)
            self.active_orders[link_id] = {
                "link_id": link_id,
                "order_id": order_id,
                "level_idx": level_idx,
                "side": side,
                "price": str(item.get("price", "0")),
                "qty": str(item.get("qty", "0")),
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "PostOnly" if str(item.get("timeInForce", "")) == "PostOnly" else "GTC",
                "reduce_only": self._truthy(item.get("reduceOnly", False)),
                "entry_price": None,
                "processed_fill_qty": 0.0,
                "processed_fill_volume": 0.0,
                "processed_fill_fee": 0.0,
            }
            known_order_ids.add(order_id)
            adopted = True
            logger.warning(
                "Adopted exchange grid order missing from local state symbol=%s order_id=%s link_id=%s",
                self.config.get("symbol"),
                order_id,
                link_id,
            )
        ownership_changed = ownership_conflicts != self.ownership_conflicts
        self.ownership_conflicts = ownership_conflicts
        if ownership_conflicts:
            self.trigger_message = (
                f"Detected {len(ownership_conflicts)} exchange grid order(s) that are not in "
                "this run's write-ahead ledger; new order placement is paused."
            )
            if ownership_changed:
                logger.error(
                    "Unowned exchange grid orders detected symbol=%s orders=%s",
                    self.config.get("symbol"),
                    ownership_conflicts,
                )
        elif ownership_changed and self.trigger_message.startswith("Detected "):
            self.trigger_message = ""
        if adopted or ownership_changed:
            self._persist_state()
        return adopted or ownership_changed

    def _sync_active_order_from_exchange_snapshot(self, order: dict, snapshot: dict) -> bool:
        """Refresh mutable status while rejecting changes to the accepted order shape."""
        if not order.get("accepted_shape_mismatch"):
            mismatch_reason = self._accepted_shape_mismatch_reason(order, snapshot)
            if mismatch_reason:
                logger.error(
                    "Confirmed order changed immutable exchange shape symbol=%s link_id=%s "
                    "reason=%s",
                    self.config.get("symbol"),
                    order.get("link_id"),
                    mismatch_reason,
                )
                self._record_accepted_shape_mismatch(order, snapshot, mismatch_reason)
                return True

        updates: dict[str, Any] = {}
        status = snapshot.get("orderStatus") or snapshot.get("status")
        current_status = str(order.get("status") or "")
        if status and not (
            current_status.lower() == "open" and str(status).upper() in {"NEW", "OPEN"}
        ):
            updates["status"] = str(status)

        changed = False
        for key, value in updates.items():
            if str(order.get(key)) != str(value):
                order[key] = value
                changed = True
        return changed

    def _handle_order_execution_snapshot(self, order: dict, snapshot: dict) -> bool:
        stats = self._authoritative_execution_stats(order, snapshot)
        if not stats or stats["qty"] <= 0:
            return False
        return self._record_execution_delta(order, stats)

    def _reconcile_exchange_open_orders(self, open_orders: list[dict] | None = None) -> bool:
        open_orders = (
            self._fetch_open_orders()
            if open_orders is None
            else self._validate_open_order_snapshot(open_orders)
        )
        changed = self._adopt_exchange_grid_orders(open_orders)
        changed = self._resolve_pending_submissions(open_orders) or changed
        open_order_ids = {str(item.get("orderId", "")) for item in open_orders}
        open_orders_by_id = {str(item.get("orderId", "")): item for item in open_orders}
        if self._stopping or self.manual_stop_pending or self.risk_shutdown_pending:
            return changed

        for order in list(self.active_orders.values()):
            order_id = str(order.get("order_id", "") or "")
            snapshot = open_orders_by_id.get(order_id)
            if not snapshot:
                continue
            if self._sync_active_order_from_exchange_snapshot(order, snapshot):
                changed = True
            status = str(snapshot.get("orderStatus") or snapshot.get("status") or "")
            if not self._is_partial_status(status):
                continue
            if self._handle_order_execution_snapshot(order, snapshot):
                changed = True

        closed_links = [
            link_id
            for link_id, order in list(self.active_orders.items())
            if not order.get("submission_pending")
            and str(order.get("order_id", "")) not in open_order_ids
        ]
        for link_id in closed_links:
            if self._stopping or link_id not in self.active_orders:
                continue
            order = self.active_orders[link_id]
            order_snapshot = self._get_order_snapshot(order)
            status = self._order_status_from_snapshot(order_snapshot)
            if self._is_cancelled_status(status):
                self._handle_cancelled_order(link_id, order, snapshot=order_snapshot)
                changed = True
                continue
            if self._is_filled_status(status):
                changed = (
                    self._handle_confirmed_closed_order(
                        link_id,
                        order,
                        allow_estimate=not hasattr(self.client, "get_order_trades"),
                    )
                    or changed
                )
                continue
            if self._is_partial_status(status):
                snapshot = self._get_order_snapshot(order)
                if self._handle_order_execution_snapshot(order, snapshot):
                    changed = True
                self._mark_fast_poll()
                continue
            if status == "UNKNOWN":
                # Unknown may be a temporary exchange/API gap. Only mutate state if trades prove a fill.
                changed = (
                    self._handle_confirmed_closed_order(link_id, order, allow_estimate=False)
                    or changed
                )
                continue

            logger.info(
                "Grid order absent from open orders but not terminal symbol=%s order_id=%s link_id=%s status=%s",
                self.config.get("symbol"),
                order.get("order_id"),
                link_id,
                status,
            )
        return changed

    def _handle_cancelled_order(
        self,
        link_id: str,
        order: dict,
        *,
        snapshot: dict | None = None,
    ):
        fallback_qty = float(order["qty"])
        effective_snapshot = (
            snapshot if snapshot is not None else self._get_order_snapshot(order)
        )
        stats = self._authoritative_execution_stats(
            order,
            effective_snapshot,
        )

        snapshot_qty = Decimal(
            str(
                self._float_field(
                    effective_snapshot,
                    "executedQty",
                    "cumExecQty",
                    "cumQty",
                    "cum_exec_qty",
                )
            )
        )
        if stats and stats["qty"] > 0:
            confirmed_qty = min(float(stats["qty"]), fallback_qty)
            self._record_execution_delta(order, {**stats, "qty": confirmed_qty})

        processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
        if hasattr(self.client, "get_order_trades") and (
            snapshot_qty > processed_qty + self._qty_tolerance_decimal()
        ):
            order["status"] = "RECONCILING_EXECUTION"
            self.trigger_message = (
                "A cancelled order has a confirmed partial fill; waiting for exact "
                "exchange trades and commission before replacing the remainder."
            )
            self._mark_fast_poll()
            self._persist_state()
            return

        planned_qty = Decimal(str(order.get("qty", 0) or 0))
        filled_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))

        self.active_orders.pop(link_id, None)

        remaining_qty = self._normalized_qty_decimal(
            max(Decimal("0"), planned_qty - filled_qty),
            self.qty_step,
        )
        if self._qty_reaches_accounting_step(remaining_qty):
            replacement = {
                **order,
                "qty": self._fq(remaining_qty),
                "processed_fill_qty": 0.0,
                "processed_fill_volume": 0.0,
                "processed_fill_fee": 0.0,
            }
            self._replace_cancelled_order(replacement)
        self._persist_state()

    def _handle_confirmed_closed_order(self, link_id: str, order: dict, *, allow_estimate: bool) -> bool:
        snapshot = self._get_order_snapshot(order)
        handled = self._handle_closed_order(
            order,
            allow_estimate=allow_estimate,
            snapshot=snapshot,
        )
        if handled:
            planned_qty = Decimal(str(order.get("qty", 0) or 0))
            processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
            if processed_qty + self._qty_tolerance_decimal() >= planned_qty:
                self.active_orders.pop(link_id, None)
            else:
                self._mark_fast_poll()
            self._persist_state()
            return True
        logger.info(
            "Grid order closed without confirmed fill symbol=%s order_id=%s link_id=%s",
            self.config.get("symbol"),
            order.get("order_id"),
            link_id,
        )
        return False

    def _position_size(self, side: str) -> float:
        total = sum(
            (
                position["qty"]
                for position in self._validated_position_rows()
                if position["side"] == side
            ),
            Decimal("0"),
        )
        return float(total)

    def _actual_position_net_qty(self) -> float:
        return sum(self._signed_qty(item["side"], item["qty"]) for item in self._position_snapshots())

    def _actual_grid_position_net_qty(self) -> float:
        actual_grid_qty = self._actual_position_net_qty() - self._baseline_position_net_qty()
        direction = self.config["direction"]
        if direction == "long":
            return max(0.0, actual_grid_qty)
        if direction == "short":
            return min(0.0, actual_grid_qty)
        return actual_grid_qty

    def _validated_position_rows(self) -> list[dict]:
        symbol = str(self.config.get("symbol") or "").upper()
        response = self.client.get_positions(symbol)
        return validate_position_response(response, symbol=symbol)

    def _position_snapshots(self) -> list[dict]:
        positions = []
        for position in self._validated_position_rows():
            qty = position["qty"]
            if qty <= 0 or not self._qty_reaches_accounting_step(qty):
                continue
            entry_price = position["entry_price"] or Decimal("0")
            positions.append(
                {
                    "side": position["side"],
                    "qty": float(qty),
                    "entry_price": float(entry_price),
                }
            )
        return positions

    def _capture_baseline_position(self, open_side: str):
        if self._qty_reaches_accounting_step(self.baseline_position_qty) or self.baseline_position_side:
            return

        positions = self._position_snapshots()
        if not positions:
            return

        direction = self.config["direction"]
        if direction == "neutral":
            raise RuntimeError(
                "Existing position detected; neutral grid cannot isolate old positions in one-way mode"
            )

        opposite = [position for position in positions if position["side"] != open_side]
        if opposite:
            summary = ", ".join(f"{item['side']} {item['qty']:g}" for item in positions)
            raise RuntimeError(
                f"Existing {self.config['symbol']} position would be offset by this {direction} grid: {summary}"
            )

        total_qty = sum(position["qty"] for position in positions)
        if not self._qty_reaches_accounting_step(total_qty):
            return
        weighted_entry = sum(position["qty"] * position["entry_price"] for position in positions)
        self.baseline_position_side = open_side
        self.baseline_position_qty = total_qty
        self.baseline_position_entry_price = weighted_entry / total_qty if weighted_entry > 0 else 0.0

    def _migrate_baseline_position_from_exchange(self):
        if not self._allow_restore_baseline_migration:
            return
        if self._qty_reaches_accounting_step(self.baseline_position_qty) or self.baseline_position_side:
            return

        grid_net_qty = self._grid_position_net_qty()
        if not self._qty_reaches_accounting_step(grid_net_qty):
            return

        positions = self._position_snapshots()
        actual_net_qty = sum(self._signed_qty(item["side"], item["qty"]) for item in positions)
        baseline_net_qty = actual_net_qty - grid_net_qty
        if not self._qty_reaches_accounting_step(baseline_net_qty):
            return
        if baseline_net_qty * grid_net_qty <= 0:
            return

        baseline_side = "Buy" if baseline_net_qty > 0 else "Sell"
        baseline_qty = abs(baseline_net_qty)
        matching_positions = [item for item in positions if item["side"] == baseline_side]
        weighted_entry = sum(item["qty"] * item["entry_price"] for item in matching_positions)
        total_matching_qty = sum(item["qty"] for item in matching_positions)

        self.baseline_position_side = baseline_side
        self.baseline_position_qty = baseline_qty
        self.baseline_position_entry_price = (
            weighted_entry / total_matching_qty if weighted_entry > 0 and total_matching_qty > 0 else 0.0
        )

    @staticmethod
    def _signed_qty(side: str, qty: float) -> float:
        return qty if side == "Buy" else -qty if side == "Sell" else 0.0

    def _baseline_position_net_qty(self) -> float:
        return self._signed_qty(self.baseline_position_side, self.baseline_position_qty)

    def _grid_position_net_qty(self) -> float:
        if self.config["direction"] == "long":
            return max(0.0, self.grid_position_net_qty)
        if self.config["direction"] == "short":
            return min(0.0, self.grid_position_net_qty)
        return self.grid_position_net_qty

    def _derive_grid_position_net_qty(self) -> float:
        direction = self.config["direction"]
        if direction in {"long", "short"}:
            reduce_side = "Sell" if direction == "long" else "Buy"
            reduce_qty = sum(
                max(
                    0.0,
                    float(order.get("qty", 0) or 0)
                    - float(order.get("processed_fill_qty", 0) or 0),
                )
                for order in self.active_orders.values()
                if order.get("side") == reduce_side and order.get("reduce_only")
            )
            if reduce_qty > 0:
                return reduce_qty if direction == "long" else -reduce_qty

        if direction == "long":
            net_qty = self.initial_qty
            for order in self.filled_orders:
                qty = float(order.get("qty", 0) or 0)
                if order.get("side") == "Buy" and not order.get("reduce_only"):
                    net_qty += qty
                elif order.get("side") == "Sell" and order.get("reduce_only"):
                    net_qty -= qty
            return max(0.0, net_qty)

        if direction == "short":
            net_qty = -self.initial_qty
            for order in self.filled_orders:
                qty = float(order.get("qty", 0) or 0)
                if order.get("side") == "Sell" and not order.get("reduce_only"):
                    net_qty -= qty
                elif order.get("side") == "Buy" and order.get("reduce_only"):
                    net_qty += qty
            return min(0.0, net_qty)

        net_qty = 0.0
        for order in self.filled_orders:
            qty = float(order.get("qty", 0) or 0)
            net_qty += self._signed_qty(str(order.get("side") or ""), qty)
        return net_qty

    def _set_grid_position_net_qty(self, value: Decimal | float | str):
        position = Decimal(str(value))
        normalized = self._normalized_qty_decimal(
            abs(position),
            self._qty_step_decimal(),
        )
        if position < 0:
            normalized = -normalized
        self.grid_position_net_qty = float(normalized)

    def _set_initial_grid_position(self, side: str, qty: float):
        self._set_grid_position_net_qty(self._signed_qty(side, qty))

    def _apply_grid_position_fill(self, order: dict, qty: float):
        direction = self.config["direction"]
        side = order.get("side")
        reduce_only = bool(order.get("reduce_only"))
        position = Decimal(str(self.grid_position_net_qty))
        fill_qty = Decimal(str(qty))
        if direction == "long":
            if side == "Buy" and not reduce_only:
                position += fill_qty
            elif side == "Sell" and reduce_only:
                position -= fill_qty
            position = max(Decimal("0"), position)
        elif direction == "short":
            if side == "Sell" and not reduce_only:
                position -= fill_qty
            elif side == "Buy" and reduce_only:
                position += fill_qty
            position = min(Decimal("0"), position)
        else:
            position += Decimal(str(self._signed_qty(str(side or ""), qty)))
        self._set_grid_position_net_qty(position)

    def _apply_market_reduce_to_grid_position(
        self,
        side: str,
        qty: float,
        *,
        lot_ledger_updated: bool = False,
    ):
        self._apply_grid_position_fill({"side": side, "reduce_only": True}, qty)
        if not lot_ledger_updated:
            self.reduce_lots_complete = False

    def _sync_grid_position_with_exchange(self) -> bool:
        if self.config["direction"] not in {"long", "short"}:
            return True

        if self._halt_if_baseline_breached():
            return False

        actual_grid_qty = self._actual_grid_position_net_qty()
        self._last_actual_grid_position_net_qty = actual_grid_qty
        local_grid_qty = self._grid_position_net_qty()
        if not self._qty_reaches_accounting_step(actual_grid_qty - local_grid_qty):
            self._position_mismatch_seen_at = 0.0
            self._position_mismatch_signature = None
            if self.trigger_message.startswith("Position ledger mismatch"):
                self.trigger_message = ""
                self._persist_state()
            return True

        signature = (
            round(local_grid_qty, 8),
            round(actual_grid_qty, 8),
            round(self._baseline_position_net_qty(), 8),
        )
        now = time.time()
        if self._position_mismatch_signature != signature:
            self._position_mismatch_signature = signature
            self._position_mismatch_seen_at = now
        if now - self._position_mismatch_seen_at < POSITION_SYNC_GRACE_SECONDS:
            logger.info(
                "Delaying position mismatch alert until order ledger catches up "
                "symbol=%s local=%s actual=%s active_orders=%s",
                self.config.get("symbol"),
                local_grid_qty,
                actual_grid_qty,
                len(self.active_orders),
            )
            self._mark_fast_poll(POSITION_SYNC_GRACE_SECONDS)
            return False

        message = (
            f"Position ledger mismatch: grid ledger {self._fq(abs(local_grid_qty))}, "
            f"exchange grid portion {self._fq(abs(actual_grid_qty))}; "
            "automatic coverage repair is paused and no position ownership was changed."
        )
        if self.trigger_message != message:
            self.trigger_message = message
            logger.error(
                "Unexplained position mismatch; refusing to rewrite grid ownership "
                "symbol=%s local=%s actual=%s baseline=%s",
                self.config.get("symbol"),
                local_grid_qty,
                actual_grid_qty,
                self._baseline_position_net_qty(),
            )
            self._persist_state()
        self._mark_fast_poll()
        return False

    def _reconcile_grid_position_protection(self):
        if (
            self.config["direction"] not in {"long", "short"}
            or self.initial_grid_deployment_pending
            or self._stopping
            or self.pending_reduce_action
            or self._paused_replacements_block_reconciliation()
            or self.risk_shutdown_pending
            or self.manual_stop_pending
            or self.ownership_conflicts
        ):
            return

        if not self._sync_grid_position_with_exchange():
            return
        if self._stopping:
            return

        if not self._qty_reaches_accounting_step(self._grid_position_qty()):
            if not self._repair_flat_open_side_grid():
                self._clear_completed_repair_message()
            return

        if self._repair_missing_reduce_protection_from_ledger():
            return

        if self._repair_open_side_coverage_from_lots():
            return

        if self._repair_missing_reduce_at_boundary():
            return
        self._clear_completed_repair_message()

    def _clear_completed_repair_message(self) -> bool:
        if not self.trigger_message.startswith(COMPLETED_REPAIR_MESSAGE_PREFIXES):
            return False
        self.trigger_message = ""
        self._persist_state()
        return True

    @staticmethod
    def _lot_add(lots: dict[int, dict[str, Decimal]], level_idx: int, qty: Decimal, entry_price: Decimal):
        if qty <= 0:
            return
        lot = lots.setdefault(level_idx, {"qty": Decimal("0"), "entry_value": Decimal("0")})
        lot["qty"] += qty
        lot["entry_value"] += qty * entry_price

    @staticmethod
    def _normalize_reduce_lots(raw_lots: dict) -> dict[str, dict[str, float]]:
        normalized: dict[str, dict[str, float]] = {}
        for raw_level, raw_lot in (raw_lots or {}).items():
            try:
                level_idx = int(raw_level)
                qty = float((raw_lot or {}).get("qty") or 0)
                entry_value = float((raw_lot or {}).get("entry_value") or 0)
            except (TypeError, ValueError):
                continue
            if qty <= 0:
                continue
            normalized[str(level_idx)] = {"qty": qty, "entry_value": entry_value}
        return normalized

    @staticmethod
    def _normalize_level_qtys(raw_qtys: dict) -> dict[str, float]:
        normalized: dict[str, float] = {}
        for raw_level, raw_qty in (raw_qtys or {}).items():
            try:
                level_idx = int(raw_level)
                qty = float(raw_qty or 0)
            except (TypeError, ValueError):
                continue
            if qty > 0:
                normalized[str(level_idx)] = qty
        return normalized

    def _reduce_lot_decimal_map(self) -> dict[int, dict[str, Decimal]]:
        lots: dict[int, dict[str, Decimal]] = {}
        for raw_level, raw_lot in self.reduce_lots_by_level.items():
            try:
                level_idx = int(raw_level)
                qty = Decimal(str(raw_lot.get("qty", 0) or 0))
                entry_value = Decimal(str(raw_lot.get("entry_value", 0) or 0))
            except Exception:
                continue
            if qty > 0:
                lots[level_idx] = {"qty": qty, "entry_value": entry_value}
        return lots

    def _set_reduce_lot_decimal_map(self, lots: dict[int, dict[str, Decimal]]):
        self.reduce_lots_by_level = {
            str(level_idx): {
                "qty": float(lot["qty"]),
                "entry_value": float(lot["entry_value"]),
            }
            for level_idx, lot in sorted(lots.items())
            if lot.get("qty", Decimal("0")) > 0
        }

    def _lot_remove(self, lots: dict[int, dict[str, Decimal]], level_idx: int, qty: Decimal) -> bool:
        if qty <= 0:
            return True
        lot = lots.get(level_idx)
        if not lot or qty - lot["qty"] > self._qty_tolerance_decimal():
            return False
        remaining = lot["qty"] - qty
        if remaining <= self._qty_tolerance_decimal():
            lots.pop(level_idx, None)
            return True
        average_entry = lot["entry_value"] / lot["qty"] if lot["qty"] > 0 else Decimal("0")
        lot["qty"] = remaining
        lot["entry_value"] -= average_entry * qty
        return True

    def _reset_reduce_lots_from_pending_targets(self, entry_price: float):
        if self.config["direction"] not in {"long", "short"} or not self._pending_targets:
            self.reduce_lots_by_level = {}
            self.reduce_lots_complete = False
            return

        lots: dict[int, dict[str, Decimal]] = {}
        entry = Decimal(str(entry_price or 0))
        try:
            plan = self._validated_pending_target_plan()
        except Exception:
            self.reduce_lots_by_level = {}
            self.reduce_lots_complete = False
            raise
        for spec in plan:
            if not spec["reduce_only"]:
                continue
            self._lot_add(
                lots,
                int(spec["level_idx"]),
                Decimal(str(spec["qty_text"])),
                entry,
            )
        self._set_reduce_lot_decimal_map(lots)
        self.reduce_lots_complete = True

    def _record_reduce_lot_fill(
        self,
        order: dict,
        qty: float,
        price: float,
    ) -> float | None:
        if not self.reduce_lots_complete or self.config["direction"] not in {"long", "short"}:
            return None

        try:
            level_idx = int(order.get("level_idx", 0) or 0)
            qty_decimal = Decimal(str(qty))
            price_decimal = Decimal(str(price))
        except Exception:
            self.reduce_lots_complete = False
            return None

        lots = self._reduce_lot_decimal_map()
        direction = self.config["direction"]
        side = order.get("side")
        reduce_only = bool(order.get("reduce_only"))
        realized_gross: Decimal | None = None

        if direction == "short":
            if side == "Sell" and not reduce_only:
                self._lot_add(lots, level_idx, qty_decimal, price_decimal)
            elif side == "Buy" and reduce_only:
                lot = lots.get(level_idx)
                if (
                    not lot
                    or qty_decimal - lot["qty"] > self._qty_tolerance_decimal()
                ):
                    self.reduce_lots_complete = False
                    return None
                average_entry = lot["entry_value"] / lot["qty"]
                realized_gross = (average_entry - price_decimal) * qty_decimal
                if not self._lot_remove(lots, level_idx, qty_decimal):
                    self.reduce_lots_complete = False
                    return None
        elif direction == "long":
            if side == "Buy" and not reduce_only:
                self._lot_add(lots, level_idx, qty_decimal, price_decimal)
            elif side == "Sell" and reduce_only:
                lot = lots.get(level_idx)
                if (
                    not lot
                    or qty_decimal - lot["qty"] > self._qty_tolerance_decimal()
                ):
                    self.reduce_lots_complete = False
                    return None
                average_entry = lot["entry_value"] / lot["qty"]
                realized_gross = (price_decimal - average_entry) * qty_decimal
                if not self._lot_remove(lots, level_idx, qty_decimal):
                    self.reduce_lots_complete = False
                    return None

        self._set_reduce_lot_decimal_map(lots)
        return float(realized_gross) if realized_gross is not None else None

    def _record_market_reduce_lot_fill(
        self,
        side: str,
        qty: float,
        price: float,
    ) -> float | None:
        direction = self.config.get("direction")
        expected_side = "Sell" if direction == "long" else "Buy" if direction == "short" else ""
        if (
            not self.reduce_lots_complete
            or not expected_side
            or side != expected_side
        ):
            return None

        lots = self._reduce_lot_decimal_map()
        ledger_qty = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if abs(ledger_qty - grid_qty) > self._qty_tolerance_decimal():
            self.reduce_lots_complete = False
            return None

        remaining = Decimal(str(qty))
        exit_price = Decimal(str(price))
        if remaining - ledger_qty > self._qty_tolerance_decimal():
            self.reduce_lots_complete = False
            return None

        realized_gross = Decimal("0")
        for level_idx in sorted(list(lots)):
            if remaining <= self._qty_tolerance_decimal():
                break
            lot = lots.get(level_idx)
            if not lot or lot["qty"] <= 0:
                continue
            consumed = min(remaining, lot["qty"])
            average_entry = lot["entry_value"] / lot["qty"]
            if direction == "long":
                realized_gross += (exit_price - average_entry) * consumed
            else:
                realized_gross += (average_entry - exit_price) * consumed
            if not self._lot_remove(lots, level_idx, consumed):
                self.reduce_lots_complete = False
                return None
            remaining -= consumed

        if remaining > self._qty_tolerance_decimal():
            self.reduce_lots_complete = False
            return None
        self._set_reduce_lot_decimal_map(lots)
        return float(realized_gross)

    def _reduce_target_for_level(self, level_idx: int) -> tuple[str, float] | None:
        direction = self.config["direction"]
        if direction == "short":
            if 0 <= level_idx < len(self.grid_levels):
                return "Buy", self.grid_levels[level_idx]
        elif direction == "long":
            if 0 <= level_idx + 1 < len(self.grid_levels):
                return "Sell", self.grid_levels[level_idx + 1]
        return None

    def _initial_reduce_lots_by_level(self) -> dict[int, dict[str, Decimal]] | None:
        direction = self.config["direction"]
        if direction not in {"long", "short"}:
            return {}
        if self.initial_qty < self.min_qty or self.initial_entry_price <= 0:
            return {}

        profit_targets, _ = self._target_orders_for_price(self.initial_entry_price)
        allocated_qtys = self._allocate_qtys(self.initial_qty, len(profit_targets))
        if len(allocated_qtys) != len(profit_targets):
            return None

        lots: dict[int, dict[str, Decimal]] = {}
        entry_price = Decimal(str(self.initial_entry_price))
        for target, allocated_qty in zip(profit_targets, allocated_qtys, strict=True):
            level_idx = int(target[0])
            target_qty = self.target_qty_by_level.get(str(level_idx), allocated_qty)
            self._lot_add(lots, level_idx, Decimal(str(target_qty)), entry_price)
        return lots

    def _bootstrap_reduce_lots_from_legacy_state(self):
        if self.reduce_lots_complete or self.reduce_lots_by_level:
            return
        if self.config["direction"] not in {"long", "short"}:
            return
        lots, reason = self._reduce_lots_from_fill_ledger()
        if lots is None:
            logger.info(
                "Reduce lot ledger unavailable from legacy state symbol=%s reason=%s",
                self.config.get("symbol"),
                reason,
            )
            return
        ledger_qty = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if self._qty_reaches_accounting_step(ledger_qty - grid_qty):
            logger.info(
                "Reduce lot ledger from legacy state does not match grid position symbol=%s ledger_qty=%s grid_qty=%s",
                self.config.get("symbol"),
                float(ledger_qty),
                float(grid_qty),
            )
            return
        self._set_reduce_lot_decimal_map(lots)
        self.reduce_lots_complete = True

    def _reduce_lots_from_fill_ledger(self) -> tuple[dict[int, dict[str, Decimal]] | None, str]:
        if self.config["direction"] not in {"long", "short"}:
            return {}, ""

        reduce_fill_count = sum(1 for order in self.filled_orders if order.get("reduce_only"))
        if int(self.completed_pairs or 0) > reduce_fill_count:
            return None, "fill history is truncated"

        lots = self._initial_reduce_lots_by_level()
        if lots is None:
            return None, "initial allocation is unavailable"

        direction = self.config["direction"]
        for order in self.filled_orders:
            try:
                level_idx = int(order.get("level_idx", 0) or 0)
                qty = Decimal(str(order.get("qty", 0) or 0))
                price = Decimal(str(order.get("price", 0) or 0))
            except Exception:
                return None, "fill ledger contains invalid values"
            side = order.get("side")
            reduce_only = bool(order.get("reduce_only"))

            if direction == "short":
                if side == "Sell" and not reduce_only:
                    self._lot_add(lots, level_idx, qty, price)
                elif side == "Buy" and reduce_only:
                    if not self._lot_remove(lots, level_idx, qty):
                        return None, "short reduce fill has no matching open lot"
            elif direction == "long":
                if side == "Buy" and not reduce_only:
                    self._lot_add(lots, level_idx, qty, price)
                elif side == "Sell" and reduce_only:
                    if not self._lot_remove(lots, level_idx, qty):
                        return None, "long reduce fill has no matching open lot"

        return lots, ""

    def _inferred_reduce_entry_price(self, level_idx: int) -> Decimal:
        direction = self.config["direction"]
        try:
            if direction == "short" and 0 <= level_idx + 1 < len(self.grid_levels):
                return Decimal(str(self.grid_levels[level_idx + 1]))
            if direction == "long" and 0 <= level_idx < len(self.grid_levels):
                return Decimal(str(self.grid_levels[level_idx]))
        except Exception:
            return Decimal("0")
        return Decimal("0")

    def _reduce_lots_from_exchange_open_orders(self) -> tuple[dict[int, dict[str, Decimal]] | None, str]:
        """Rebuild protection lots from live exchange reduce-only orders.

        This is deliberately stricter than a total-quantity normalization: every
        reduce order must be a current exchange order with a grid link and the
        correct grid-level price. No exchange orders are changed here.
        """
        if self.config["direction"] not in {"long", "short"}:
            return {}, ""

        reduce_side = self._reduce_side()
        if not reduce_side:
            return None, "reduce side is unavailable"

        grid_qty = Decimal(str(self._grid_position_qty()))
        if not self._qty_reaches_accounting_step(grid_qty):
            return {}, ""

        try:
            open_orders = self._fetch_open_orders()
        except Exception as exc:
            return None, f"exchange open orders unavailable: {exc}"

        self._adopt_exchange_grid_orders(open_orders)
        local_by_id = {
            str(order.get("order_id", "") or ""): order
            for order in self.active_orders.values()
            if order.get("order_id")
        }

        lots: dict[int, dict[str, Decimal]] = {}
        total = Decimal("0")
        for item in open_orders:
            if not self._truthy(item.get("reduceOnly", False)):
                continue
            side = str(item.get("side") or "")
            if side != reduce_side:
                continue

            link_id = str(item.get("orderLinkId", "") or "")
            parsed = self._parse_grid_link_id(link_id)
            if not parsed:
                return None, f"active reduce order {item.get('orderId')} is not a grid order"
            level_idx, side_from_link = parsed
            if side_from_link != reduce_side:
                return None, f"active reduce order {item.get('orderId')} link side does not match reduce side"

            target = self._reduce_target_for_level(level_idx)
            if not target or target[0] != reduce_side:
                return None, f"active reduce order {item.get('orderId')} has invalid grid level"
            try:
                if self._fp(float(item.get("price", 0) or 0)) != self._fp(float(target[1])):
                    return None, f"active reduce order {item.get('orderId')} price does not match grid level"
                original_qty = Decimal(str(item.get("qty", 0) or 0))
                executed_qty = Decimal(
                    str(item.get("executedQty", item.get("cumExecQty", 0)) or 0)
                )
                qty = original_qty - executed_qty
            except Exception:
                return None, f"active reduce order {item.get('orderId')} contains invalid values"
            if qty <= self._qty_tolerance_decimal():
                continue
            if not self._qty_reaches_accounting_step(qty):
                return None, f"active reduce order {item.get('orderId')} remaining quantity is invalid"

            local_order = self.active_orders.get(link_id) or local_by_id.get(str(item.get("orderId", "") or ""))
            try:
                entry_price = Decimal(str((local_order or {}).get("entry_price") or 0))
            except Exception:
                entry_price = Decimal("0")
            if entry_price <= 0:
                entry_price = self._inferred_reduce_entry_price(level_idx)
            if entry_price <= 0:
                return None, f"active reduce order {item.get('orderId')} entry price is unavailable"

            self._lot_add(lots, level_idx, qty, entry_price)
            total += qty

        if not lots:
            return None, "no active exchange reduce protection orders"
        if self._qty_reaches_accounting_step(total - grid_qty):
            return None, f"active exchange reduce qty {float(total)} does not match grid qty {float(grid_qty)}"
        return lots, ""

    def _restore_reduce_lots_from_exchange_open_orders(self, source_reason: str = "") -> bool:
        lots, reason = self._reduce_lots_from_exchange_open_orders()
        if lots is None:
            signature = ("exchange-open-reduce-ledger-unavailable", reason)
            if reason and self._should_log_reduce_warning(signature):
                logger.warning(
                    "Exchange reduce protection ledger unavailable symbol=%s reason=%s",
                    self.config.get("symbol"),
                    reason,
                )
            return False

        self._set_reduce_lot_decimal_map(lots)
        self.reduce_lots_complete = True
        self.trigger_message = (
            "Reduce protection ledger rebuilt from current exchange reduce-only orders; "
            "no orders were changed."
        )
        logger.warning(
            "Rebuilt reduce protection ledger from exchange open orders symbol=%s levels=%s source_reason=%s",
            self.config.get("symbol"),
            sorted(lots),
            source_reason,
        )
        self._persist_state()
        return True

    def _reduce_lots_for_repair(self) -> tuple[dict[int, dict[str, Decimal]] | None, str]:
        if self.reduce_lots_complete:
            return self._reduce_lot_decimal_map(), ""

        lots, reason = self._reduce_lots_from_fill_ledger()
        if lots is None:
            if self._restore_reduce_lots_from_exchange_open_orders(reason):
                return self._reduce_lot_decimal_map(), ""
            return None, reason

        expected_total = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if self._qty_reaches_accounting_step(expected_total - grid_qty):
            mismatch_reason = (
                f"fill ledger qty {float(expected_total)} does not match grid qty {float(grid_qty)}"
            )
            if self._restore_reduce_lots_from_exchange_open_orders(mismatch_reason):
                return self._reduce_lot_decimal_map(), ""
        return lots, reason

    def _should_log_reduce_warning(self, signature: tuple, interval: float = 60.0) -> bool:
        now = time.time()
        last_logged_at = self._reduce_warning_at_by_signature.get(signature, 0.0)
        if now - last_logged_at >= interval:
            self._reduce_warning_at_by_signature[signature] = now
            return True
        return False

    def _repair_missing_reduce_protection_from_ledger(self) -> bool:
        lots, reason = self._reduce_lots_for_repair()
        if lots is None:
            signature = ("ledger-unavailable", reason)
            if reason and self._should_log_reduce_warning(signature):
                logger.warning(
                    "Reduce-only protection ledger unavailable symbol=%s reason=%s",
                    self.config.get("symbol"),
                    reason,
                )
            return False

        expected_total = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if self._qty_reaches_accounting_step(expected_total - grid_qty):
            logger.warning(
                "Reduce-only protection ledger does not match grid position symbol=%s ledger_qty=%s grid_qty=%s",
                self.config.get("symbol"),
                float(expected_total),
                float(grid_qty),
            )
            return False

        placed_count = 0
        placed_qty = Decimal("0")
        for level_idx in sorted(lots):
            lot = lots[level_idx]
            target = self._reduce_target_for_level(level_idx)
            if not target:
                continue
            reduce_side, reduce_price = target
            active_qty = Decimal(str(self._active_order_remaining_qty(reduce_side, level_idx, True)))
            deficit = lot["qty"] - active_qty
            if deficit <= 0 or not self._qty_reaches_accounting_step(deficit):
                continue
            entry_price = lot["entry_value"] / lot["qty"] if lot["qty"] > 0 else Decimal("0")
            placed = self._place(
                reduce_side,
                reduce_price,
                level_idx,
                reduce_only=True,
                qty_override=float(deficit),
                entry_price=float(entry_price) if entry_price > 0 else None,
                allow_duplicate=True,
            )
            if placed:
                placed_count += 1
                placed_qty += deficit

        if placed_count:
            self.trigger_message = (
                f"Repaired {placed_count} missing reduce-only protection order(s) "
                f"from fill ledger: {self._fq(float(placed_qty))}"
            )
            logger.warning(
                "Repaired missing reduce-only protection from fill ledger symbol=%s orders=%s qty=%s",
                self.config.get("symbol"),
                placed_count,
                self._fq(float(placed_qty)),
            )
            self._persist_state()
            return True
        return False

    def _grid_position_entry_price(self) -> float | None:
        direction = self.config.get("direction")
        position_side = "Buy" if direction == "long" else "Sell" if direction == "short" else ""
        if not position_side:
            return None
        for position in self._position_snapshots():
            if position.get("side") == position_side and float(position.get("entry_price") or 0) > 0:
                return float(position["entry_price"])
        if self.initial_entry_price > 0:
            return self.initial_entry_price
        return None

    def _boundary_reduce_target_for_missing(self) -> tuple[str, float, int] | None:
        direction = self.config.get("direction")
        if direction == "short":
            return "Buy", float(self.config["lower_price"]), 0
        if direction == "long":
            level_idx = max(0, len(self.grid_levels) - 2)
            return "Sell", float(self.config["upper_price"]), level_idx
        return None

    def _open_side_for_direction(self) -> str:
        direction = self.config.get("direction")
        if direction == "short":
            return "Sell"
        if direction == "long":
            return "Buy"
        return ""

    def _open_price_for_level(self, level_idx: int) -> float | None:
        if level_idx < 0 or level_idx + 1 >= len(self.grid_levels):
            return None
        if self.config.get("direction") == "short":
            return float(self.grid_levels[level_idx + 1])
        if self.config.get("direction") == "long":
            return float(self.grid_levels[level_idx])
        return None

    def _target_open_qty_for_level(self, level_idx: int) -> float:
        raw_qty = (
            self.target_qty_by_level.get(str(level_idx))
            or self.config.get("grid_order_qty")
            or self.config.get("qty_per_grid")
            or 0
        )
        steps = self._qty_to_steps(float(raw_qty))
        if steps <= 0:
            return 0.0
        return self._steps_to_qty(steps)

    def _repair_flat_open_side_grid(self) -> bool:
        side = self._open_side_for_direction()
        if not side or not self.grid_ready or len(self.grid_levels) < 2:
            return False

        placed_count = 0
        placed_qty = Decimal("0")
        for level_idx in range(len(self.grid_levels) - 1):
            price = self._open_price_for_level(level_idx)
            if price is None:
                continue
            target_qty = self._target_open_qty_for_level(level_idx)
            if target_qty < self.min_qty:
                continue
            deficit = self._active_order_remaining_qty_deficit(side, level_idx, False, target_qty)
            if deficit < self.min_qty:
                continue
            link_id = self._place(
                side,
                price,
                level_idx,
                reduce_only=False,
                qty_override=deficit,
                allow_duplicate=self._has_active_order(side, level_idx, False),
            )
            if link_id:
                placed_count += 1
                placed_qty += Decimal(str(deficit))

        if placed_count:
            self.trigger_message = (
                f"Restored {placed_count} flat-grid open order(s): {self._fq(float(placed_qty))}"
            )
            logger.warning(
                "Restored flat-grid open side coverage symbol=%s side=%s orders=%s qty=%s",
                self.config.get("symbol"),
                side,
                placed_count,
                self._fq(float(placed_qty)),
            )
            self._persist_state()
            return True
        return False

    def _repair_open_side_coverage_from_lots(self) -> bool:
        side = self._open_side_for_direction()
        if (
            not side
            or not self.grid_ready
            or len(self.grid_levels) < 2
            or self.config.get("direction") not in {"long", "short"}
        ):
            return False

        lots, reason = self._reduce_lots_for_repair()
        if lots is None:
            signature = ("open-coverage-ledger-unavailable", reason)
            if reason and self._should_log_reduce_warning(signature):
                logger.warning(
                    "Open-side grid coverage ledger unavailable symbol=%s reason=%s",
                    self.config.get("symbol"),
                    reason,
                )
            return False

        lot_total = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if self._qty_reaches_accounting_step(lot_total - grid_qty):
            signature = ("open-coverage-ledger-mismatch", float(lot_total), float(grid_qty))
            if self._should_log_reduce_warning(signature):
                logger.warning(
                    "Open-side grid coverage skipped because lot ledger mismatches position symbol=%s ledger_qty=%s grid_qty=%s",
                    self.config.get("symbol"),
                    float(lot_total),
                    float(grid_qty),
                )
            return False

        coverage = self.grid_coverage_snapshot()
        if coverage.get("excess_by_level"):
            excess_qty = sum(
                Decimal(str(item.get("excess_qty", 0) or 0))
                for item in coverage["excess_by_level"]
            )
            self.trigger_message = (
                "Open-side coverage has excess level quantity; automatic top-up is paused "
                "until the mixed ledger mismatch is reviewed."
            )
            signature = (
                "open-coverage-excess",
                tuple(
                    (int(item.get("level", 0) or 0), str(item.get("excess_qty", 0) or 0))
                    for item in coverage["excess_by_level"]
                ),
            )
            if self._should_log_reduce_warning(signature):
                logger.warning(
                    "Open-side coverage top-up blocked by excess level quantity symbol=%s excess_qty=%s excess=%s",
                    self.config.get("symbol"),
                    float(excess_qty),
                    coverage["excess_by_level"],
                )
            self._persist_state()
            return True

        placed_count = 0
        placed_qty = Decimal("0")
        for level_idx in range(len(self.grid_levels) - 1):
            target_qty = Decimal(str(self._target_open_qty_for_level(level_idx)))
            if target_qty < Decimal(str(self.min_qty)):
                continue

            lot_qty = Decimal("0")
            if level_idx in lots:
                lot_qty = lots[level_idx]["qty"]
            open_target = target_qty - lot_qty
            if open_target < Decimal(str(self.min_qty)):
                continue

            active_open = Decimal(str(self._active_order_remaining_qty(side, level_idx, False)))
            deficit = open_target - active_open
            if deficit < Decimal(str(self.min_qty)):
                continue

            price = self._open_price_for_level(level_idx)
            if price is None:
                continue
            link_id = self._place(
                side,
                price,
                level_idx,
                reduce_only=False,
                qty_override=float(deficit),
                allow_duplicate=self._has_active_order(side, level_idx, False),
            )
            if link_id:
                placed_count += 1
                placed_qty += deficit

        if placed_count:
            self.trigger_message = (
                f"Restored {placed_count} open-side grid coverage order(s): "
                f"{self._fq(float(placed_qty))}"
            )
            logger.warning(
                "Restored open-side grid coverage from lot ledger symbol=%s side=%s orders=%s qty=%s",
                self.config.get("symbol"),
                side,
                placed_count,
                self._fq(float(placed_qty)),
            )
            self._persist_state()
            return True
        return False

    def _repair_missing_reduce_at_boundary(self) -> bool:
        # A boundary order has no trustworthy grid-level ownership. Keep this
        # legacy safeguard explicit so normal grids never create guessed lots.
        if not bool(self.config.get("boundary_reduce_fallback", False)):
            return False

        reduce_side = self._reduce_side()
        target = self._boundary_reduce_target_for_missing()
        if not reduce_side or not target:
            return False

        grid_qty = Decimal(str(self._grid_position_qty()))
        active_reduce_qty = Decimal(str(self._active_reduce_qty(reduce_side)))
        missing_qty = grid_qty - active_reduce_qty
        if missing_qty <= 0 or not self._qty_reaches_accounting_step(missing_qty):
            return False

        side, price, level_idx = target
        placed = self._place(
            side,
            price,
            level_idx,
            reduce_only=True,
            qty_override=float(missing_qty),
            entry_price=self._grid_position_entry_price(),
            allow_duplicate=True,
            tag="boundary_reduce_fallback",
        )
        if not placed:
            self._mark_fast_poll()
            return False

        self.trigger_message = (
            f"Placed boundary reduce-only fallback: {side} {self._fq(float(missing_qty))} "
            f"at {self._fp(price)} because reduce ledger is incomplete."
        )
        logger.warning(
            "Placed boundary reduce-only fallback symbol=%s side=%s price=%s qty=%s active_reduce_qty=%s grid_qty=%s",
            self.config.get("symbol"),
            side,
            self._fp(price),
            self._fq(float(missing_qty)),
            self._fq(float(active_reduce_qty)),
            self._fq(float(grid_qty)),
        )
        self._persist_state()
        return True

    def _cancel_excess_reduce_protection_by_level(self, reduce_side: str, excess_by_level: list[dict]) -> bool:
        cancelled_count = 0
        cancelled_qty = Decimal("0")
        open_orders = self._fetch_open_orders()
        for excess in excess_by_level:
            level_idx = int(excess.get("level", 0) or 0)
            remaining_excess = Decimal(str(excess.get("excess_qty", 0) or 0))
            if remaining_excess < Decimal(str(self.min_qty)):
                continue
            candidates = [
                (link_id, order)
                for link_id, order in list(self.active_orders.items())
                if order.get("side") == reduce_side
                and order.get("reduce_only")
                and int(order.get("level_idx", 0) or 0) == level_idx
            ]
            for link_id, order in candidates:
                if remaining_excess < Decimal(str(self.min_qty)):
                    break
                order_qty = Decimal(str(order.get("qty", 0) or 0))
                try:
                    outcome = self._cancel_managed_order_once(
                        order,
                        open_orders,
                        place_counter=True,
                    )
                    if outcome != "done":
                        self.trigger_message = (
                            "Misplaced reduce-only cancellation is unconfirmed; retaining the "
                            "order ledger and retrying before rebuilding protection."
                        )
                        self._mark_fast_poll()
                        self._persist_state()
                        return True
                    self.active_orders.pop(link_id, None)
                    processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
                    cancelled_remaining = max(Decimal("0"), order_qty - processed_qty)
                    cancelled_count += 1
                    cancelled_qty += cancelled_remaining
                    remaining_excess -= cancelled_remaining
                except Exception as exc:
                    logger.warning(
                        "Failed to cancel excess reduce protection symbol=%s level=%s order_id=%s msg=%s",
                        self.config.get("symbol"),
                        level_idx,
                        order.get("order_id", ""),
                        exc,
                    )
                    self._mark_fast_poll()
                    self._persist_state()
                    return True

        if cancelled_count:
            self.trigger_message = (
                f"Cancelled {cancelled_count} misplaced reduce-only order(s); "
                "rebuilding level protection next poll."
            )
            logger.warning(
                "Cancelled misplaced reduce-only protection symbol=%s orders=%s qty=%s",
                self.config.get("symbol"),
                cancelled_count,
                self._fq(float(cancelled_qty)),
            )
            self._mark_fast_poll()
            self._persist_state()
            return True
        return False

    def _handle_reduce_protection_level_risk(self) -> bool:
        snapshot = self.reduce_protection_snapshot()
        if not snapshot.get("has_risk"):
            return False

        if not snapshot.get("ledger_ok", True):
            reason = snapshot.get("ledger_reason") or "reduce protection ledger is incomplete"
            self.trigger_message = (
                f"Reduce protection risk: {reason}; manual review required "
                "instead of placing guessed boundary orders."
            )
            signature = ("reduce-protection-ledger", reason)
            if self._should_log_reduce_warning(signature):
                logger.warning(
                    "Reduce protection ledger risk symbol=%s reason=%s",
                    self.config.get("symbol"),
                    reason,
                )
            self._persist_state()
            return True

        reduce_side = self._reduce_side()
        if snapshot.get("excess_by_level"):
            return self._cancel_excess_reduce_protection_by_level(
                reduce_side,
                list(snapshot.get("excess_by_level") or []),
            )

        if snapshot.get("missing_by_level"):
            if self._repair_missing_reduce_protection_from_ledger():
                return True
            missing_qty = sum(float(item.get("missing_qty", 0) or 0) for item in snapshot["missing_by_level"])
            self.trigger_message = (
                f"Reduce-only protection has level gaps: {self._fq(missing_qty)}; "
                "waiting for safe reduce capacity."
            )
            signature = ("reduce-protection-level-gap", self._fq(missing_qty))
            if self._should_log_reduce_warning(signature):
                logger.warning(
                    "Reduce protection level gaps symbol=%s missing_qty=%s gaps=%s",
                    self.config.get("symbol"),
                    self._fq(missing_qty),
                    snapshot["missing_by_level"],
                )
            self._persist_state()
            return True

        return False

    def _warn_missing_reduce_protection(self):
        grid_qty = self._grid_position_qty()
        if not self._qty_reaches_accounting_step(grid_qty):
            return

        reduce_side = self._reduce_side()
        if not reduce_side:
            return

        active_reduce_qty = self._active_reduce_qty(reduce_side)
        missing_qty = grid_qty - active_reduce_qty
        if not self._qty_reaches_accounting_step(missing_qty):
            return

        self.trigger_message = (
            f"Reduce-only protection is short by {self._fq(missing_qty)}; "
            "waiting for complete order/fill ledger instead of placing guessed boundary orders."
        )
        signature = (
            "missing-reduce-protection",
            reduce_side,
            self._fq(grid_qty),
            self._fq(active_reduce_qty),
            self._fq(missing_qty),
        )
        if self._should_log_reduce_warning(signature):
            logger.warning(
                "Reduce-only protection missing; not placing guessed boundary order symbol=%s side=%s grid_qty=%s active_reduce_qty=%s missing_qty=%s",
                self.config.get("symbol"),
                reduce_side,
                self._fq(grid_qty),
                self._fq(active_reduce_qty),
                self._fq(missing_qty),
            )
        self._persist_state()

    def _grid_position_qty(self) -> float:
        return abs(self._grid_position_net_qty())

    def _safe_grid_close_qty(self, position_side: str) -> float:
        position_qty = self._position_size(position_side)
        if not self._qty_reaches_accounting_step(position_qty):
            return 0.0

        available_qty = position_qty
        if self.baseline_position_side == position_side:
            available_qty = max(0.0, position_qty - self.baseline_position_qty)

        close_qty = min(available_qty, self._grid_position_qty())
        return close_qty if self._qty_reaches_accounting_step(close_qty) else 0.0

    def _expected_position_net_qty(self) -> float:
        return self._baseline_position_net_qty() + self._grid_position_net_qty()

    def estimate_grid_unrealized_pnl(self, mark_price: float) -> float:
        direction = self.config["direction"]
        grid_qty = self._grid_position_qty()
        if direction not in {"long", "short"} or not self._qty_reaches_accounting_step(grid_qty):
            return 0.0

        if self.reduce_lots_complete:
            lots = self._reduce_lot_decimal_map()
            ledger_qty = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
            grid_qty_decimal = Decimal(str(grid_qty))
            if abs(ledger_qty - grid_qty_decimal) <= self._qty_tolerance_decimal():
                entry_value = sum(
                    (lot["entry_value"] for lot in lots.values()),
                    Decimal("0"),
                )
                mark_value = Decimal(str(mark_price)) * ledger_qty
                if direction == "long":
                    return float(mark_value - entry_value)
                return float(entry_value - mark_value)

        reduce_side = "Sell" if direction == "long" else "Buy"
        remaining = grid_qty
        pnl = 0.0
        for order in self.active_orders.values():
            if not order.get("reduce_only") or order.get("side") != reduce_side:
                continue
            if not self._qty_reaches_accounting_step(remaining):
                break
            order_qty = Decimal(str(order.get("qty", 0) or 0))
            processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
            order_remaining = max(Decimal("0"), order_qty - processed_qty)
            if not self._qty_reaches_accounting_step(order_remaining):
                continue
            qty = min(remaining, float(order_remaining))
            entry_price = float(order.get("entry_price") or self.initial_entry_price or order.get("price") or 0)
            if direction == "long":
                pnl += (mark_price - entry_price) * qty
            else:
                pnl += (entry_price - mark_price) * qty
            remaining -= qty

        if self._qty_reaches_accounting_step(remaining) and self.initial_entry_price > 0:
            if direction == "long":
                pnl += (mark_price - self.initial_entry_price) * remaining
            else:
                pnl += (self.initial_entry_price - mark_price) * remaining
        return pnl

    def _authoritative_execution_stats(
        self,
        order: dict,
        snapshot: dict | None = None,
        *,
        allow_estimate: bool = False,
    ) -> dict | None:
        fallback_qty = float(order.get("qty", 0) or 0)
        fallback_price = float(order.get("price", 0) or self.current_price or 0)
        trade_stats = self._get_trade_stats(
            str(order.get("order_id", "") or ""),
            fallback_price,
            fallback_qty,
            allow_estimate=allow_estimate,
            liquidity_hint=self._order_liquidity_hint(order),
        )
        if (
            trade_stats
            and hasattr(self.client, "get_order_trades")
            and not allow_estimate
            and not bool(trade_stats.get("fee_conversion_complete", True))
        ):
            self.trigger_message = (
                "Waiting for exact exchange fee conversion before finalizing a fill."
            )
            self._mark_fast_poll()
            trade_stats = None
        elif self.trigger_message.startswith("Waiting for exact exchange fee conversion"):
            self.trigger_message = ""

        snapshot = {**order, **(snapshot or {})}
        executed_qty = self._float_field(
            snapshot,
            "executedQty",
            "cumExecQty",
            "cumQty",
            "cum_exec_qty",
        )
        snapshot_stats = None
        if executed_qty > 0:
            snapshot_stats = self._execution_stats_from_order_snapshot(
                snapshot,
                fallback_price,
                fallback_qty,
                liquidity_hint=self._order_liquidity_hint(order),
            )

        snapshot_is_ahead = snapshot_stats and (
            not trade_stats
            or Decimal(str(snapshot_stats.get("qty", 0) or 0))
            > Decimal(str(trade_stats.get("qty", 0) or 0)) + self._qty_tolerance_decimal()
        )
        if snapshot_is_ahead:
            # Order snapshots prove quantity but do not contain the final
            # per-execution commission. When a trade endpoint exists, wait for
            # that authoritative page instead of permanently booking an
            # estimate that cannot be corrected after the counter order is
            # placed and the source order leaves active_orders.
            if trade_stats:
                return trade_stats
            if hasattr(self.client, "get_order_trades") and not allow_estimate:
                return None
            return snapshot_stats
        return trade_stats

    def _record_market_reduce_execution(self, action: dict, stats: dict) -> bool:
        delta_stats = self._fill_delta_stats(action, stats)
        if not delta_stats:
            return False
        self._mark_order_fill_processed(action, stats)

        qty = float(delta_stats["qty"])
        price = float(delta_stats["price"])
        entry_price = float(action.get("entry_price", 0) or 0)
        ledger_gross_profit = self._record_market_reduce_lot_fill(
            str(action.get("side") or ""),
            qty,
            price,
        )
        gross_profit = ledger_gross_profit if ledger_gross_profit is not None else 0.0
        if ledger_gross_profit is None and entry_price > 0:
            if self.config["direction"] == "long" and action.get("side") == "Sell":
                gross_profit = (price - entry_price) * qty
            elif self.config["direction"] == "short" and action.get("side") == "Buy":
                gross_profit = (entry_price - price) * qty

        self._apply_market_reduce_to_grid_position(
            str(action.get("side") or ""),
            qty,
            lot_ledger_updated=ledger_gross_profit is not None,
        )
        recorded = self._record_trade_value(
            price,
            qty,
            gross_profit,
            volume=delta_stats["volume"],
            fee=delta_stats["fee"],
            fee_asset=delta_stats["fee_asset"],
            fee_source=delta_stats["fee_source"],
        )
        self.filled_orders.append(
            {
                "side": action.get("side", ""),
                "price": price,
                "qty": qty,
                "level_idx": -1,
                "volume": round(recorded["volume"], 4),
                "fee": round(recorded["fee"], 4),
                "fee_asset": recorded["fee_asset"],
                "fee_source": recorded["fee_source"],
                "fee_conversion_source": delta_stats.get(
                    "fee_conversion_source",
                    "",
                ),
                "maker_count": delta_stats.get("maker_count", 0),
                "taker_count": delta_stats.get("taker_count", 0),
                "liquidity": self._liquidity_label(delta_stats),
                "gross_profit": round(recorded["gross_profit"], 4),
                "profit": round(recorded["net_profit"], 4),
                "time": time.time(),
                "reduce_only": True,
                "reason": action.get("reason", ""),
                "tag": action.get("tag", "market_reduce"),
            }
        )
        self.filled_count += 1
        self._persist_state()
        return True

    def _resolve_pending_reduce_submission(self, action: dict) -> bool:
        if not action.get("submission_pending"):
            return True

        link_id = str(action.get("link_id", "") or "")
        open_orders = self._fetch_open_orders()
        open_snapshot = next(
            (
                item
                for item in open_orders
                if str(item.get("orderLinkId", "") or "") == link_id
            ),
            None,
        )
        if open_snapshot and self._confirm_pending_submission(action, open_snapshot):
            self._persist_state()
            return True
        if action.get("accepted_shape_mismatch"):
            return True

        snapshot, authoritative = self._submission_snapshot_by_link(link_id)
        if snapshot and self._confirm_pending_submission(action, snapshot):
            self._persist_state()
            return True
        if action.get("accepted_shape_mismatch"):
            return True

        history_snapshot = self._submission_history_by_link({link_id}).get(link_id)
        if history_snapshot and self._confirm_pending_submission(action, history_snapshot):
            self._persist_state()
            return True
        if action.get("accepted_shape_mismatch"):
            return True

        now = time.time()
        if authoritative:
            last_check = float(action.get("submission_last_not_found_at", 0) or 0)
            if now - last_check >= SUBMISSION_NOT_FOUND_CHECK_INTERVAL_SECONDS:
                action["submission_not_found_count"] = int(
                    action.get("submission_not_found_count", 0) or 0
                ) + 1
                action["submission_last_not_found_at"] = now

        if (
            now - float(action.get("submission_updated_at", 0) or 0)
            >= SUBMISSION_RETRY_SECONDS
            and int(action.get("submission_not_found_count", 0) or 0)
            >= SUBMISSION_REQUIRED_NOT_FOUND_CHECKS
        ):
            outcome = self._retry_pending_submission(action)
            if outcome == "confirmed":
                return True
            if outcome == "mismatch":
                return True
            if outcome == "rejected":
                message = str(action.get("submission_error") or "Reduce-only market order rejected")
                self.pending_reduce_action = None
                self._persist_state()
                raise RuntimeError(message)

        self._mark_fast_poll()
        self._persist_state()
        return False

    def _resolve_pending_reduce_action(self) -> bool:
        action = self.pending_reduce_action
        if not action:
            return True
        if not self._resolve_pending_reduce_submission(action):
            return False

        snapshot = self._get_order_snapshot(action)
        status = self._order_status_from_snapshot(snapshot)
        if status == "UNKNOWN":
            status = self._order_status_from_snapshot(action)
        stats = self._authoritative_execution_stats(action, snapshot)
        if stats and stats.get("qty", 0) > 0:
            self._record_market_reduce_execution(action, stats)

        planned_qty = Decimal(str(action.get("qty", 0) or 0))
        processed_qty = Decimal(str(action.get("processed_fill_qty", 0) or 0))
        fully_accounted = processed_qty + self._qty_tolerance_decimal() >= planned_qty
        terminal = self._is_filled_status(status) or self._is_cancelled_status(status)
        if fully_accounted or (terminal and not self._is_filled_status(status)):
            self.pending_reduce_action = None
            self._persist_state()
            return True

        if self._is_filled_status(status):
            self.trigger_message = (
                "Reduce-only market order is filled; waiting for authoritative execution "
                "quantity before changing the position ledger."
            )
        else:
            self.trigger_message = "Reduce-only market order is pending exchange confirmation."
        self._mark_fast_poll()
        self._persist_state()
        return False

    def _place_reduce_market(self, side: str, qty: float, reason: str, *, tag: str = "market_reduce") -> str:
        if self.pending_reduce_action:
            self._resolve_pending_reduce_action()
            if self.pending_reduce_action:
                return str(
                    self.pending_reduce_action.get("order_id")
                    or self.pending_reduce_action.get("link_id")
                    or ""
                )

        capped_qty = Decimal(str(self._cap_reduce_order_qty(side, qty)))
        if self.max_market_qty > 0:
            capped_qty = min(capped_qty, Decimal(str(self.max_market_qty)))
        qty_text = self._market_order_qty_text(float(capped_qty), reduce_only=True)
        if not qty_text:
            logger.warning(
                "Skipped reduce-only market order that is invalid under exchange market-lot rules "
                "symbol=%s side=%s requested=%s reason=%s",
                self.config.get("symbol"),
                side,
                qty,
                reason,
            )
            return ""

        link_id = f"repair_{side[0]}_{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
        self.pending_reduce_action = {
            "link_id": link_id,
            "order_id": "",
            "side": side,
            # A market order does not need a quote to submit. Actual execution
            # price is reconciled from trades/order state before P&L is changed.
            "price": str(self.current_price or 0),
            "qty": qty_text,
            "status": "SUBMITTING",
            "order_type": "Market",
            "time_in_force": "IOC",
            "reduce_only": True,
            "entry_price": self._grid_position_entry_price(),
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
            "submission_pending": True,
            "submission_attempts": 1,
            "submission_not_found_count": 0,
            "submission_last_not_found_at": 0.0,
            "submission_updated_at": time.time(),
            "reason": reason,
            "tag": tag,
        }
        self._persist_state()
        try:
            result = self.client.place_order(
                symbol=self.config["symbol"],
                side=side,
                qty=qty_text,
                order_type="Market",
                reduce_only=True,
                order_link_id=link_id,
            )
        except Exception as exc:
            if self._is_uncertain_submission_exception(exc):
                self._mark_submission_unknown(self.pending_reduce_action, exc)
                self.trigger_message = (
                    "Reduce-only market submission is unconfirmed; checking the original "
                    "client order ID without sending another close."
                )
                self._persist_state()
                return link_id
            self.pending_reduce_action = None
            self._persist_state()
            raise
        if result.get("retCode") != 0:
            message = str(
                result.get("retMsg") or f"Failed to place reduce-only repair order: {reason}"
            )
            if self._is_uncertain_submission_result(result):
                self._mark_submission_unknown(self.pending_reduce_action, message)
                return link_id
            self.pending_reduce_action = None
            self._persist_state()
            raise RuntimeError(message)
        if not self._confirm_pending_submission(
            self.pending_reduce_action, result.get("result", {})
        ):
            if self.pending_reduce_action.get("accepted_shape_mismatch"):
                self._resolve_pending_reduce_action()
                return link_id
            self._mark_submission_unknown(
                self.pending_reduce_action,
                "successful reduce-only market response did not confirm the original client order ID",
            )
            return link_id
        order_id = str(self.pending_reduce_action.get("order_id", "") or "")
        self._resolve_pending_reduce_action()
        self.trigger_message = f"Safety reduce submitted {side} {qty_text}: {reason}"
        logger.warning(self.trigger_message)
        self._mark_fast_poll()
        self._persist_state()
        return order_id or link_id

    def _place_market_open(self, side: str, qty: float) -> bool:
        qty_text = self._market_order_qty_text(qty, reduce_only=False)
        if not qty_text:
            raise RuntimeError(f"Initial market order quantity {qty} is below the exchange minimum")
        notional_price = self.current_mark_price or self.current_price
        if not self._meets_min_notional(str(notional_price), qty_text):
            raise RuntimeError(
                f"Initial market order notional {self._limit_notional(str(notional_price), qty_text)} "
                f"is below the exchange minimum {self.min_notional}"
            )
        formatted_qty = Decimal(str(qty_text))
        requested_qty = self._normalized_qty_decimal(qty, self.qty_step)
        market_tolerance = max(
            abs(Decimal(str(self.market_qty_step))) * Decimal("1e-9"),
            Decimal("1e-18"),
        )
        if (
            self._position_sizing_mode() == "fixed_grid_qty"
            and abs(formatted_qty - requested_qty) > market_tolerance
        ):
            raise RuntimeError(
                "The fixed per-grid quantity cannot be represented exactly by the exchange "
                "market-order quantity step; use a compatible quantity or a limit opening order"
            )
        if self.max_market_qty > 0:
            max_market_qty = Decimal(str(self.max_market_qty))
            if formatted_qty > max_market_qty:
                raise RuntimeError(
                    f"Initial market order quantity {qty_text} exceeds the exchange single-order "
                    f"market maximum {self.max_market_qty}; reduce the per-grid quantity or use a "
                    "limit opening order"
                )
        link_id = f"init_{side[0]}_{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
        self.initial_side = side
        self.waiting_initial_order = True
        self.opening_order = {
            "link_id": link_id,
            "order_id": "",
            "side": side,
            "price": str(self.current_price),
            "qty": qty_text,
            "status": "SUBMITTING",
            "order_type": "Market",
            "time_in_force": "IOC",
            "reduce_only": False,
            "submission_pending": True,
            "submission_attempts": 1,
            "submission_not_found_count": 0,
            "submission_last_not_found_at": 0.0,
            "submission_updated_at": time.time(),
        }
        try:
            self._persist_state()
        except Exception:
            self.opening_order = None
            self.waiting_initial_order = False
            raise

        try:
            result = self.client.place_order(
                symbol=self.config["symbol"],
                side=side,
                qty=qty_text,
                order_type="Market",
                reduce_only=False,
                order_link_id=link_id,
            )
        except Exception as exc:
            if isinstance(exc, ExchangeRateLimitError) or is_exchange_rate_limit_message(exc):
                self._mark_submission_rate_limited(
                    self.opening_order,
                    exc,
                    retry_after=(
                        exc.retry_after if isinstance(exc, ExchangeRateLimitError) else None
                    ),
                )
                self.trigger_message = (
                    "Initial market opening is rate limited; the original client order ID "
                    "is retained and will be retried only after cooldown and authoritative "
                    "exchange absence checks."
                )
                self._persist_state()
                return False
            if self._is_uncertain_submission_exception(exc):
                self._mark_submission_unknown(self.opening_order, exc)
                self.trigger_message = (
                    "Initial market order submission is unconfirmed; checking the original "
                    "client order ID without opening another position."
                )
                self._persist_state()
                return False
            self.opening_order = None
            self.waiting_initial_order = False
            self._persist_state()
            raise
        if result.get("retCode") != 0:
            message = str(result.get("retMsg") or "Failed to place initial market order")
            if self._is_rate_limit_result(result):
                self._mark_submission_rate_limited(self.opening_order, message)
                self.trigger_message = (
                    "Initial market opening is rate limited; the original client order ID "
                    "is retained and will be retried only after cooldown and authoritative "
                    "exchange absence checks."
                )
                self._persist_state()
                return False
            if self._is_uncertain_submission_result(result):
                self._mark_submission_unknown(self.opening_order, message)
                return False
            self.opening_order = None
            self.waiting_initial_order = False
            self._persist_state()
            raise RuntimeError(message)
        if not self._confirm_pending_submission(self.opening_order, result.get("result", {})):
            if self.opening_order.get("accepted_shape_mismatch"):
                raise RuntimeError(self.trigger_message)
            self._mark_submission_unknown(
                self.opening_order,
                "successful opening response did not confirm the original client order ID",
            )
            self.trigger_message = (
                "Initial market order acknowledgement did not confirm the original client "
                "order ID; checking that identity without opening another position."
            )
            self._persist_state()
            return False

        stats = self._authoritative_execution_stats(
            self.opening_order,
            result.get("result", {}) or {},
            allow_estimate=not hasattr(self.client, "get_order_trades"),
        )
        planned_qty = Decimal(str(self.opening_order.get("qty", 0) or 0))
        confirmed_qty = Decimal(str((stats or {}).get("qty", 0) or 0))
        if not stats or confirmed_qty + self._qty_tolerance_decimal() < planned_qty:
            self.trigger_message = (
                "Initial market order was accepted; waiting for the exchange to confirm "
                "the full execution quantity and price before deploying the grid."
            )
            self._mark_fast_poll()
            self._persist_state()
            return False
        self._record_opening_execution_delta(self.opening_order, stats)
        self.opening_order = None
        self.waiting_initial_order = False
        self.initialization_in_progress = True
        self._mark_fast_poll()
        self._persist_state()
        return True

    def _warn_reduce_limit_unplaced(self, side: str, price_text: str, qty: str, reason: str):
        self.trigger_message = (
            f"Reduce-only limit protection was not placed for {side} {qty} at {price_text}; "
            "keeping the lot ledger and retrying/reconciling without market-closing."
        )
        logger.warning(
            "Reduce-only limit protection not placed symbol=%s side=%s price=%s qty=%s reason=%s",
            self.config.get("symbol"),
            side,
            price_text,
            qty,
            reason,
        )
        self._persist_state()

    def _place_limit_open(self, side: str, qty: float, price: float, *, post_only: bool = True):
        qty_text = self._order_qty_text(qty, reduce_only=False)
        if not qty_text:
            raise RuntimeError(f"Initial limit order quantity {qty} is below the exchange minimum")
        price_text = self._fp(price)
        if not self._meets_min_notional(price_text, qty_text):
            raise RuntimeError(
                f"Initial limit order notional {self._limit_notional(price_text, qty_text)} "
                f"is below the exchange minimum {self.min_notional}"
            )
        link_id = f"open_{side[0]}_{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
        self.initial_side = side
        self.waiting_initial_order = True
        order_label = "post-only" if post_only else "limit"
        self.trigger_message = f"Waiting for {order_label} opening order at {price_text}"
        self.opening_order = {
            "link_id": link_id,
            "order_id": "",
            "side": side,
            "price": price_text,
            "qty": qty_text,
            "status": "SUBMITTING",
            "order_type": "Limit",
            "time_in_force": "PostOnly" if post_only else "GTC",
            "reduce_only": False,
            "submission_pending": True,
            "submission_attempts": 1,
            "submission_not_found_count": 0,
            "submission_last_not_found_at": 0.0,
            "submission_updated_at": time.time(),
        }
        try:
            self._persist_state()
        except Exception:
            self.opening_order = None
            self.waiting_initial_order = False
            raise

        try:
            result = self.client.place_order(
                symbol=self.config["symbol"],
                side=side,
                qty=qty_text,
                price=price_text,
                order_type="Limit",
                reduce_only=False,
                order_link_id=link_id,
                time_in_force="PostOnly" if post_only else None,
            )
        except Exception as exc:
            if isinstance(exc, ExchangeRateLimitError) or is_exchange_rate_limit_message(exc):
                self._mark_submission_rate_limited(
                    self.opening_order,
                    exc,
                    retry_after=(
                        exc.retry_after if isinstance(exc, ExchangeRateLimitError) else None
                    ),
                )
                self.trigger_message = (
                    f"{order_label.title()} opening is rate limited; the original client "
                    "order ID is retained and will be retried only after cooldown and "
                    "authoritative exchange absence checks."
                )
                self._persist_state()
                return
            if self._is_uncertain_submission_exception(exc):
                self._mark_submission_unknown(self.opening_order, exc)
                self.trigger_message = (
                    f"{order_label.title()} opening order submission is unconfirmed; "
                    "checking the original client order ID without opening another position."
                )
                self._persist_state()
                return
            self.opening_order = None
            self.waiting_initial_order = False
            self._persist_state()
            raise
        if result.get("retCode") != 0:
            message = str(result.get("retMsg") or "Failed to place initial limit order")
            if self._is_rate_limit_result(result):
                self._mark_submission_rate_limited(self.opening_order, message)
                self.trigger_message = (
                    f"{order_label.title()} opening is rate limited; the original client "
                    "order ID is retained and will be retried only after cooldown and "
                    "authoritative exchange absence checks."
                )
                self._persist_state()
                return
            if self._is_uncertain_submission_result(result):
                self._mark_submission_unknown(self.opening_order, message)
                return
            self.opening_order = None
            self.waiting_initial_order = False
            self._persist_state()
            raise RuntimeError(message)
        if not self._confirm_pending_submission(self.opening_order, result.get("result", {})):
            if self.opening_order.get("accepted_shape_mismatch"):
                raise RuntimeError(self.trigger_message)
            self._mark_submission_unknown(
                self.opening_order,
                "successful opening response did not confirm the original client order ID",
            )
            self.trigger_message = (
                f"{order_label.title()} opening order acknowledgement did not confirm the "
                "original client order ID; checking that identity without opening another position."
            )
            self._persist_state()
            return
        self._mark_fast_poll()
        self._persist_state()

    def _initial_limit_price(self, side: str, current_price: float, *, post_only: bool = True) -> float:
        tick = float(self.tick_size)
        maker_safe_price = current_price - tick if side == "Buy" else current_price + tick
        configured_price = self.config.get("initial_order_price")
        if configured_price is not None and not post_only:
            return float(configured_price)

        if configured_price is not None:
            configured_price = float(configured_price)
            if side == "Buy" and configured_price < current_price:
                return configured_price
            if side == "Sell" and configured_price > current_price:
                return configured_price
            logger.warning(
                "Initial post-only price would cross market symbol=%s side=%s configured=%s current=%s; using maker-safe price=%s",
                self.config.get("symbol"),
                side,
                configured_price,
                current_price,
                maker_safe_price,
            )

        return maker_safe_price

    def _place(
        self,
        side: str,
        price: float,
        level_idx: int,
        reduce_only: bool,
        qty_override: float | None = None,
        entry_price: float | None = None,
        allow_duplicate: bool = False,
        tag: str | None = None,
        link_id_override: str | None = None,
    ) -> Optional[str]:
        raw_qty = float(qty_override) if qty_override is not None else float(self.config["qty_per_grid"])

        qty = self._order_qty_text(raw_qty, reduce_only=reduce_only)
        if not qty:
            logger.warning(
                "Skipped grid order below exchange quantity precision symbol=%s side=%s level=%s requested=%s reduce_only=%s",
                self.config.get("symbol"),
                side,
                level_idx,
                raw_qty,
                reduce_only,
            )
            return None
        price_text = self._fp(price)
        shape_key = self._order_shape_key(side, price_text, qty, reduce_only)
        if self._rate_limit_remaining() > 0 or self._order_shape_retry_remaining(shape_key) > 0:
            return None
        if not reduce_only and not self._meets_min_notional(price_text, qty):
            message = (
                f"Grid order is below exchange minimum notional: {side} {qty} at {price_text} "
                f"is {self._limit_notional(price_text, qty)}, minimum {self.min_notional}; "
                "the exact counter task remains queued for aggregation instead of submitting "
                "an invalid or oversized order."
            )
            self.trigger_message = message
            signature = ("minimum-notional", shape_key)
            if self._should_log_reduce_warning(signature):
                logger.warning("%s symbol=%s", message, self.config.get("symbol"))
            self._persist_state()
            return None
        link_id = str(
            link_id_override
            or f"g_{level_idx}_{side[0]}_{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
        )

        if link_id in self.active_orders:
            existing = self.active_orders[link_id]
            if (
                str(existing.get("side")) == side
                and int(existing.get("level_idx", -1)) == level_idx
                and bool(existing.get("reduce_only")) == reduce_only
                and str(existing.get("price")) == price_text
                and str(existing.get("qty")) == qty
            ):
                return link_id
            raise RuntimeError(f"Grid client order ID {link_id} already has a different order shape")

        if not allow_duplicate and self._has_active_order(side, level_idx, reduce_only):
            return None
        if (
            self._stopping
            or self.manual_stop_pending
            or self.risk_shutdown_pending
            or self.ownership_conflicts
        ):
            return None

        def submit_limit(use_post_only: bool):
            return self.client.place_order(
                symbol=self.config["symbol"],
                side=side,
                qty=qty,
                price=price_text,
                order_type="Limit",
                reduce_only=reduce_only,
                order_link_id=link_id,
                time_in_force="PostOnly" if use_post_only else None,
            )

        # Reduce-only orders are safety exits; never let maker-only rules prevent them.
        use_post_only = bool(self.config.get("grid_order_post_only", False)) and not reduce_only
        pending_order = self._pending_limit_order_state(
            link_id=link_id,
            level_idx=level_idx,
            side=side,
            price=price_text,
            qty=qty,
            reduce_only=reduce_only,
            entry_price=entry_price,
            time_in_force="PostOnly" if use_post_only else "GTC",
            tag=tag,
        )
        self.active_orders[link_id] = pending_order
        try:
            # The local write-ahead record must be durable before the exchange request.
            self._persist_state()
        except Exception:
            self.active_orders.pop(link_id, None)
            raise

        try:
            result = submit_limit(use_post_only)
        except Exception as exc:
            if self._is_uncertain_submission_exception(exc):
                self._mark_submission_unknown(pending_order, exc)
                logger.warning(
                    "Grid order response is uncertain; preserving client order ID for reconciliation "
                    "symbol=%s side=%s price=%s qty=%s reduce_only=%s link_id=%s msg=%s",
                    self.config.get("symbol"),
                    side,
                    price_text,
                    qty,
                    reduce_only,
                    link_id,
                    exc,
                )
                return link_id

            self.active_orders.pop(link_id, None)
            self._persist_state()
            retry_after = exc.retry_after if isinstance(exc, ExchangeRateLimitError) else None
            self._record_order_rejection(
                shape_key,
                str(exc),
                rate_limit_retry_after=retry_after,
                track_shape=not bool(link_id_override),
            )
            if reduce_only:
                self._warn_reduce_limit_unplaced(side, price_text, qty, str(exc))
            else:
                logger.warning(
                    "Place order failed side=%s price=%s reduce_only=%s msg=%s",
                    side,
                    price_text,
                    reduce_only,
                    exc,
                )
            return None

        if result.get("retCode") != 0:
            message = str(result.get("retMsg") or "rejected")
            if self._is_uncertain_submission_result(result):
                self._mark_submission_unknown(pending_order, message)
                return link_id
            self.active_orders.pop(link_id, None)
            self._persist_state()
            self._record_order_rejection(
                shape_key,
                message,
                track_shape=not bool(link_id_override),
            )
            if reduce_only:
                self._warn_reduce_limit_unplaced(side, price_text, qty, message)
            else:
                logger.warning(
                    "Place order failed side=%s price=%s reduce_only=%s msg=%s",
                    side,
                    price_text,
                    reduce_only,
                    message,
                )
            return None

        if not self._confirm_pending_submission(pending_order, result.get("result", {})):
            if pending_order.get("accepted_shape_mismatch"):
                return link_id
            self._mark_submission_unknown(
                pending_order,
                "successful response did not confirm the original client order ID",
            )
            logger.warning(
                "Grid order acknowledgement did not confirm the original client order ID; "
                "preserving that identity "
                "symbol=%s link_id=%s",
                self.config.get("symbol"),
                link_id,
            )
            return link_id

        self._clear_order_rejection(shape_key)
        self._mark_fast_poll()
        self._persist_state()
        return link_id

    def _supports_batch_orders(self) -> bool:
        # Post Only grids need per-order rejection handling, so keep them on the safer path.
        return hasattr(self.client, "place_orders") and not bool(self.config.get("grid_order_post_only", False))

    def _place_batch_limit_orders(self, order_specs: list[dict[str, Any]]) -> list[str]:
        if self.ownership_conflicts or self._rate_limit_remaining() > 0:
            return []
        planned = []
        planned_keys: set[tuple[str, int, bool]] = set()

        for spec in order_specs:
            side = str(spec["side"])
            level_idx = int(spec["level_idx"])
            reduce_only = bool(spec.get("reduce_only", False))
            allow_duplicate = bool(spec.get("allow_duplicate", False))
            if not allow_duplicate and self._has_active_order(side, level_idx, reduce_only):
                continue
            planned_key = (side, level_idx, reduce_only)
            if not allow_duplicate and planned_key in planned_keys:
                raise RuntimeError(
                    "Duplicate grid target detected before batch submission: "
                    f"{side} level {level_idx} reduce_only={reduce_only}"
                )
            if not allow_duplicate:
                planned_keys.add(planned_key)
            if self._stopping:
                break

            raw_qty = float(spec.get("qty_override") or self.config["qty_per_grid"])

            qty = self._order_qty_text(raw_qty, reduce_only=reduce_only)
            if not qty:
                logger.warning(
                    "Skipped batch grid order below exchange quantity precision symbol=%s side=%s level=%s requested=%s reduce_only=%s",
                    self.config.get("symbol"),
                    side,
                    level_idx,
                    raw_qty,
                    reduce_only,
                )
                continue
            price_text = self._fp(float(spec["price"]))
            if not reduce_only and not self._meets_min_notional(price_text, qty):
                message = (
                    f"Grid order is below exchange minimum notional: {side} {qty} at "
                    f"{price_text} is {self._limit_notional(price_text, qty)}, minimum "
                    f"{self.min_notional}; no invalid batch request was submitted."
                )
                self.trigger_message = message
                signature = (
                    "minimum-notional",
                    self._order_shape_key(side, price_text, qty, reduce_only),
                )
                if self._should_log_reduce_warning(signature):
                    logger.warning("%s symbol=%s", message, self.config.get("symbol"))
                self._persist_state()
                continue
            link_id = (
                f"g_{level_idx}_{side[0]}_"
                f"{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
            )
            planned.append(
                {
                    "request": {
                        "symbol": self.config["symbol"],
                        "side": side,
                        "qty": qty,
                        "price": price_text,
                        "order_type": "Limit",
                        "reduce_only": reduce_only,
                        "order_link_id": link_id,
                        "time_in_force": None,
                    },
                    "state": self._pending_limit_order_state(
                        link_id=link_id,
                        level_idx=level_idx,
                        side=side,
                        price=price_text,
                        qty=qty,
                        reduce_only=reduce_only,
                        entry_price=spec.get("entry_price"),
                        time_in_force="GTC",
                        tag=spec.get("tag"),
                    ),
                    "shape_key": self._order_shape_key(
                        side,
                        price_text,
                        qty,
                        reduce_only,
                    ),
                    "fallback": spec,
                }
            )

        placed_links: list[str] = []

        def remember_link(link_id: str | None) -> None:
            if link_id and link_id not in placed_links:
                placed_links.append(link_id)

        def fallback_single(item: dict[str, Any], message: str) -> None:
            state = item["state"]
            self.active_orders.pop(str(state["link_id"]), None)
            self._persist_state()
            if is_exchange_rate_limit_message(message):
                self._record_order_rejection(
                    str(item["shape_key"]),
                    message,
                    rate_limit_retry_after=ORDER_REJECTION_BACKOFF_MAX_SECONDS,
                )
                return
            fallback = item["fallback"]
            logger.warning(
                "Batch order item definitively failed; falling back to single order "
                "symbol=%s side=%s price=%s msg=%s",
                self.config.get("symbol"),
                fallback.get("side"),
                fallback.get("price"),
                message,
            )
            link_id = self._place(
                str(fallback["side"]),
                float(fallback["price"]),
                int(fallback["level_idx"]),
                reduce_only=bool(fallback.get("reduce_only", False)),
                qty_override=float(fallback.get("qty_override") or self.config["qty_per_grid"]),
                entry_price=fallback.get("entry_price"),
                allow_duplicate=bool(fallback.get("allow_duplicate", False)),
                tag=fallback.get("tag"),
            )
            remember_link(link_id)

        def mark_unknown(items: list[dict[str, Any]], message: str) -> None:
            now = time.time()
            for item in items:
                state = item["state"]
                state["status"] = "SUBMIT_UNKNOWN"
                state["submission_error"] = message
                state["submission_updated_at"] = now
                state["submission_not_found_count"] = 0
                state["submission_last_not_found_at"] = 0.0
                remember_link(str(state["link_id"]))
            self._mark_fast_poll()
            self._persist_state()

        for start in range(0, len(planned), BATCH_ORDER_CHUNK_SIZE):
            chunk = planned[start : start + BATCH_ORDER_CHUNK_SIZE]
            if (
                self._stopping
                or self.manual_stop_pending
                or self.risk_shutdown_pending
                or self._rate_limit_remaining() > 0
            ):
                break

            for item in chunk:
                state = item["state"]
                self.active_orders[str(state["link_id"])] = state
            try:
                # Persist every client order ID before the exchange sees the batch.
                self._persist_state()
            except Exception:
                for item in chunk:
                    self.active_orders.pop(str(item["state"]["link_id"]), None)
                raise

            try:
                result = self.client.place_orders([item["request"] for item in chunk])
            except Exception as exc:
                message = str(exc)
                if self._is_uncertain_submission_exception(
                    exc
                ) or self._is_duplicate_client_order_rejection(message):
                    logger.warning(
                        "Batch order response is uncertain; preserving original client order IDs "
                        "symbol=%s orders=%s msg=%s",
                        self.config.get("symbol"),
                        len(chunk),
                        message,
                    )
                    mark_unknown(chunk, message)
                    with contextlib.suppress(Exception):
                        self._reconcile_exchange_open_orders()
                    continue

                if isinstance(exc, ExchangeRateLimitError) or is_exchange_rate_limit_message(
                    message
                ):
                    retry_after = (
                        exc.retry_after
                        if isinstance(exc, ExchangeRateLimitError)
                        else ORDER_REJECTION_BACKOFF_MAX_SECONDS
                    )
                    for item in chunk:
                        state = item["state"]
                        self.active_orders.pop(str(state["link_id"]), None)
                        self._record_order_rejection(
                            str(item["shape_key"]),
                            message,
                            rate_limit_retry_after=retry_after,
                        )
                    break

                logger.warning(
                    "Batch order request was definitively rejected; using single-order fallback "
                    "symbol=%s msg=%s",
                    self.config.get("symbol"),
                    message,
                )
                for item in chunk:
                    fallback_single(item, message)
                continue

            if result.get("retCode") != 0:
                message = str(result.get("retMsg") or "Batch order failed")
                if self._is_duplicate_client_order_rejection(
                    message
                ) or self._is_uncertain_submission_result(result):
                    mark_unknown(chunk, message)
                    with contextlib.suppress(Exception):
                        self._reconcile_exchange_open_orders()
                    continue
                if is_exchange_rate_limit_message(message):
                    for item in chunk:
                        fallback_single(item, message)
                    break
                for item in chunk:
                    fallback_single(item, message)
                continue

            result_items = result.get("result", {}).get("list", []) or []
            if not isinstance(result_items, list):
                result_items = []
            identity_ambiguous = len(result_items) != len(chunk)
            seen_response_links: set[str] = set()
            if not identity_ambiguous:
                for index, order_result in enumerate(result_items):
                    if not isinstance(order_result, dict):
                        identity_ambiguous = True
                        break
                    snapshot = order_result.get("result", {}) or {}
                    if not isinstance(snapshot, dict):
                        identity_ambiguous = True
                        break
                    response_link = str(snapshot.get("orderLinkId", "") or "")
                    expected_link = str(chunk[index]["state"]["link_id"])
                    if order_result.get("retCode") in (0, "0") and not response_link:
                        identity_ambiguous = True
                        break
                    if response_link and (
                        response_link != expected_link
                        or response_link in seen_response_links
                    ):
                        identity_ambiguous = True
                        break
                    if response_link:
                        seen_response_links.add(response_link)

            if identity_ambiguous:
                message = "batch response client-order identities were incomplete or misaligned"
                for order_result in result_items:
                    if not isinstance(order_result, dict):
                        continue
                    item_message = str(order_result.get("retMsg") or "")
                    if self._is_rate_limit_result(
                        order_result
                    ) or is_exchange_rate_limit_message(item_message):
                        self._record_order_rejection(
                            "",
                            item_message or message,
                            rate_limit_retry_after=ORDER_REJECTION_BACKOFF_MAX_SECONDS,
                            track_shape=False,
                        )
                        break
                logger.warning(
                    "Batch order response identities are ambiguous; preserving every original "
                    "client order ID symbol=%s orders=%s",
                    self.config.get("symbol"),
                    len(chunk),
                )
                mark_unknown(chunk, message)
                with contextlib.suppress(Exception):
                    self._reconcile_exchange_open_orders()
                continue

            rejected: list[tuple[dict[str, Any], str]] = []
            unconfirmed: list[dict[str, Any]] = []
            for index, item in enumerate(chunk):
                state = item["state"]
                if index >= len(result_items):
                    unconfirmed.append(item)
                    continue

                order_result = result_items[index]
                if not isinstance(order_result, dict):
                    unconfirmed.append(item)
                    continue
                if order_result.get("retCode") == 0:
                    if self._confirm_pending_submission(state, order_result.get("result", {})):
                        self._clear_order_rejection(str(item["shape_key"]))
                        remember_link(str(state["link_id"]))
                    elif state.get("accepted_shape_mismatch"):
                        remember_link(str(state["link_id"]))
                    else:
                        unconfirmed.append(item)
                    continue

                message = str(order_result.get("retMsg") or "Batch order item rejected")
                if self._is_duplicate_client_order_rejection(
                    message
                ) or self._is_uncertain_submission_result(order_result):
                    unconfirmed.append(item)
                else:
                    rejected.append((item, message))

            if unconfirmed:
                mark_unknown(unconfirmed, "batch response did not confirm every client order ID")
                with contextlib.suppress(Exception):
                    self._reconcile_exchange_open_orders()
            else:
                self._persist_state()

            for item, message in rejected:
                fallback_single(item, message)

        if placed_links:
            self._mark_fast_poll()
            self._persist_state()
        return placed_links

    def _is_trigger_hit(self, current_price: float) -> bool:
        trigger_price = self.config.get("trigger_price")
        if trigger_price is None:
            return True

        direction = self.config["direction"]
        trigger_price = float(trigger_price)
        if direction == "long":
            return current_price <= trigger_price
        if direction == "short":
            return current_price >= trigger_price
        return self.config["lower_price"] <= current_price <= self.config["upper_price"]

    def _deploy_initial_grid(self, current_price: float):
        levels = self.grid_levels
        direction = self.config["direction"]

        self.waiting_trigger = False
        self.trigger_message = ""

        open_side = ""
        if direction == "long":
            open_side = "Buy"
        elif direction == "short":
            open_side = "Sell"

        initial_order_type = str(self.config.get("initial_order_type", "market")).lower().strip()
        limit_open_price = None
        reference_price = current_price
        if direction in {"long", "short"} and initial_order_type in {"post_only", "limit"}:
            limit_open_price = self._initial_limit_price(
                open_side,
                current_price,
                post_only=initial_order_type == "post_only",
            )
            reference_price = limit_open_price

        range_check_price = reference_price if limit_open_price is not None else current_price
        range_label = "Reference price" if limit_open_price is not None else "Current price"
        if not (levels[0] < range_check_price < levels[-1]):
            raise RuntimeError(
                f"{range_label} {range_check_price} must stay inside the configured range "
                f"{levels[0]} - {levels[-1]}"
            )

        if direction in {"long", "short"}:
            self._capture_baseline_position(open_side)

        total_qty = self._prepare_pending_targets(reference_price)

        if direction in {"long", "short"}:
            self._begin_opening_progress(open_side, total_qty)
            if initial_order_type in {"post_only", "limit"}:
                self._place_limit_open(
                    open_side,
                    total_qty,
                    limit_open_price,
                    post_only=initial_order_type == "post_only",
                )
                return
            if not self._place_market_open(open_side, total_qty):
                return
            self._prepare_pending_targets_after_opening_fill(
                self.initial_entry_price or current_price,
                self.initial_qty,
            )
            self._reset_reduce_lots_from_pending_targets(self.initial_entry_price or current_price)
            self._deploy_pending_targets()

        if direction == "neutral":
            self.initialization_in_progress = True
            self._persist_state()
            self._deploy_pending_targets()

    def _prepare_pending_targets_after_opening_fill(
        self,
        fill_price: float,
        fill_qty: float,
    ) -> None:
        if self._position_sizing_mode() != "fixed_grid_qty":
            self._prepare_pending_targets(fill_price, fill_qty)
            return

        if not self._pending_targets:
            raise RuntimeError(
                "The fixed-grid opening plan is unavailable; no grid order was submitted"
            )
        target_qty = self._opening_target_qty_decimal()
        confirmed_qty = self._normalized_qty_decimal(fill_qty, self.qty_step)
        if abs(confirmed_qty - target_qty) > self._qty_tolerance_decimal():
            raise RuntimeError(
                "The fixed-grid opening quantity does not match its original plan: "
                f"target={target_qty} confirmed={confirmed_qty}"
            )
        # Keep the pre-submission target identities and exact per-level quantity.
        # If the market crosses levels while the opening order executes, those
        # original GTC legs catch up naturally instead of redistributing inventory.
        self.config["derived_total_qty"] = float(target_qty)

    def _target_orders_for_price(self, reference_price: float) -> tuple[list[tuple[int, float, str]], list[tuple[int, float, str]]]:
        levels = self.grid_levels
        direction = self.config["direction"]
        if direction == "long":
            profit_targets = [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > reference_price
            ]
            add_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < reference_price
            ]
        elif direction == "short":
            profit_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < reference_price
            ]
            add_targets = [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > reference_price
            ]
        else:
            profit_targets = []
            add_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < reference_price
            ] + [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > reference_price
            ]
        return profit_targets, add_targets

    def _prepare_pending_targets(self, reference_price: float, total_qty_override: float | None = None) -> float:
        direction = self.config["direction"]
        profit_targets, add_targets = self._target_orders_for_price(reference_price)
        target_count = len(profit_targets) if direction in {"long", "short"} else len(add_targets)
        if target_count <= 0:
            raise RuntimeError("No valid grid targets were found around current price")

        sizing_mode = self._position_sizing_mode()
        if total_qty_override is not None:
            raw_total_qty = float(total_qty_override)
            total_steps = self._qty_to_steps(raw_total_qty)
            allocated_qtys = self._allocate_qtys(raw_total_qty, target_count)
        elif sizing_mode == "fixed_grid_qty":
            per_grid_qty = float(self.config.get("grid_order_qty") or 0)
            per_grid_steps = self._qty_to_steps(per_grid_qty)
            if per_grid_steps <= 0:
                raise RuntimeError("grid_order_qty is too small for this symbol")
            allocated_qtys = [self._steps_to_qty(per_grid_steps) for _ in range(target_count)]
            total_steps = per_grid_steps * target_count
            raw_total_qty = self._steps_to_qty(total_steps)
        else:
            raw_total_qty = self._calc_total_qty(reference_price)
            total_steps = self._qty_to_steps(raw_total_qty)
            allocated_qtys = self._allocate_qtys(raw_total_qty, target_count)
        if total_steps < target_count:
            raise RuntimeError("Total investment is too small for this symbol and grid count")
        if len(allocated_qtys) != target_count:
            raise RuntimeError(
                "Grid target allocation count is inconsistent; no exchange order was submitted"
            )

        total_qty = self._steps_to_qty(total_steps)
        qty_per_grid = total_qty / target_count
        fallback_steps = self._qty_to_steps(qty_per_grid)
        fallback_qty = self._steps_to_qty(max(1, fallback_steps))
        target_qty_by_level: dict[str, float] = {}
        if direction in {"long", "short"}:
            for target, allocated_qty in zip(profit_targets, allocated_qtys, strict=True):
                target_qty_by_level[str(target[0])] = allocated_qty
            for target in add_targets:
                target_qty_by_level.setdefault(str(target[0]), fallback_qty)
        else:
            for target, allocated_qty in zip(add_targets, allocated_qtys, strict=True):
                target_qty_by_level[str(target[0])] = allocated_qty

        self.config["active_grid_count"] = target_count
        self.config["derived_total_qty"] = total_qty
        self.config["qty_per_grid"] = qty_per_grid
        self.target_qty_by_level = target_qty_by_level

        self._pending_targets = {
            "reference_price": float(reference_price),
            "profit_targets": profit_targets,
            "add_targets": add_targets,
            "allocated_qtys": allocated_qtys,
            "allocated_qty_by_level": target_qty_by_level,
            "qty_per_grid": qty_per_grid,
        }
        # Validate the complete limit-order plan before a directional market
        # opening can create exchange exposure.
        self._validated_pending_target_plan()
        self._persist_state()
        return total_qty

    def _validated_pending_target_plan(self, qty_scale: float = 1.0) -> list[dict[str, Any]]:
        if not self._pending_targets:
            raise RuntimeError("No pending grid targets were prepared")

        pending = self._pending_targets
        direction = self.config["direction"]
        try:
            rounded_grid_prices = [Decimal(self._fp(level)) for level in self.grid_levels]
        except Exception as exc:
            raise RuntimeError("Grid prices cannot be represented by exchange precision") from exc
        if any(
            current <= previous
            for previous, current in pairwise(rounded_grid_prices)
        ):
            raise RuntimeError(
                "Configured grid levels collapse to duplicate exchange prices; "
                "reduce the grid count or widen the price range"
            )
        try:
            scale = Decimal(str(qty_scale))
        except Exception as exc:
            raise RuntimeError("Invalid pending grid quantity scale") from exc
        if not scale.is_finite() or scale <= 0:
            raise RuntimeError("Pending grid quantity scale must be finite and greater than zero")

        def normalize_targets(name: str) -> list[dict[str, Any]]:
            raw_targets = pending.get(name)
            if not isinstance(raw_targets, list):
                raise RuntimeError(f"Pending grid {name} must be a list")
            normalized: list[dict[str, Any]] = []
            seen: set[tuple[int, str]] = set()
            for position, target in enumerate(raw_targets):
                if not isinstance(target, (list, tuple)) or len(target) != 3:
                    raise RuntimeError(f"Pending grid {name}[{position}] is malformed")
                try:
                    level_idx = int(target[0])
                    price = Decimal(str(target[1]))
                except Exception as exc:
                    raise RuntimeError(f"Pending grid {name}[{position}] has invalid values") from exc
                side = str(target[2])
                if level_idx < 0 or level_idx >= max(0, len(self.grid_levels) - 1):
                    raise RuntimeError(f"Pending grid {name}[{position}] has an invalid level")
                if not price.is_finite() or price <= 0:
                    raise RuntimeError(f"Pending grid {name}[{position}] has an invalid price")
                if side not in {"Buy", "Sell"}:
                    raise RuntimeError(f"Pending grid {name}[{position}] has an invalid side")
                identity = (level_idx, side)
                if identity in seen:
                    raise RuntimeError(
                        f"Pending grid {name} contains duplicate {side} level {level_idx}"
                    )
                seen.add(identity)
                normalized.append(
                    {
                        "level_idx": level_idx,
                        "price": float(price),
                        "price_text": self._fp(float(price)),
                        "side": side,
                    }
                )
            return normalized

        profit_targets = normalize_targets("profit_targets")
        add_targets = normalize_targets("add_targets")

        reference_price = pending.get("reference_price")
        if reference_price not in (None, ""):
            try:
                reference = Decimal(str(reference_price))
            except Exception as exc:
                raise RuntimeError("Pending grid reference price is invalid") from exc
            if not reference.is_finite() or reference <= 0:
                raise RuntimeError("Pending grid reference price is invalid")
            expected_profit, expected_add = self._target_orders_for_price(float(reference))

            def signature(targets: list[dict[str, Any]]) -> list[tuple[int, str, str]]:
                return [
                    (int(target["level_idx"]), str(target["price_text"]), str(target["side"]))
                    for target in targets
                ]

            expected_profit_signature = [
                (int(level_idx), self._fp(float(price)), str(side))
                for level_idx, price, side in expected_profit
            ]
            expected_add_signature = [
                (int(level_idx), self._fp(float(price)), str(side))
                for level_idx, price, side in expected_add
            ]
            if signature(profit_targets) != expected_profit_signature:
                raise RuntimeError("Pending profit targets do not match the prepared grid")
            if signature(add_targets) != expected_add_signature:
                raise RuntimeError("Pending add targets do not match the prepared grid")

        raw_allocations = pending.get("allocated_qtys")
        if not isinstance(raw_allocations, list):
            raise RuntimeError("Pending grid allocated quantities must be a list")
        allocation_targets = profit_targets if direction in {"long", "short"} else add_targets
        if len(raw_allocations) != len(allocation_targets):
            raise RuntimeError(
                "Pending grid target and quantity counts differ; no exchange order was submitted"
            )

        allocated_qtys: list[Decimal] = []
        for position, raw_qty in enumerate(raw_allocations):
            try:
                qty = Decimal(str(raw_qty)) * scale
            except Exception as exc:
                raise RuntimeError(
                    f"Pending grid allocated quantity {position} is invalid"
                ) from exc
            if not qty.is_finite() or qty <= 0:
                raise RuntimeError(
                    f"Pending grid allocated quantity {position} must be greater than zero"
                )
            allocated_qtys.append(qty)

        try:
            qty_per_grid = Decimal(str(pending.get("qty_per_grid"))) * scale
        except Exception as exc:
            raise RuntimeError("Pending grid fallback quantity is invalid") from exc
        if not qty_per_grid.is_finite() or qty_per_grid <= 0:
            raise RuntimeError("Pending grid fallback quantity must be greater than zero")

        raw_qty_by_level = pending.get("allocated_qty_by_level") or {}
        if not isinstance(raw_qty_by_level, dict):
            raise RuntimeError("Pending grid level quantities must be an object")
        allocated_qty_by_level: dict[int, Decimal] = {}
        valid_levels = {
            int(target["level_idx"]) for target in profit_targets + add_targets
        }
        for raw_level, raw_qty in raw_qty_by_level.items():
            try:
                level_idx = int(raw_level)
                qty = Decimal(str(raw_qty)) * scale
            except Exception as exc:
                raise RuntimeError("Pending grid level quantity is invalid") from exc
            if level_idx not in valid_levels or not qty.is_finite() or qty <= 0:
                raise RuntimeError("Pending grid level quantity is inconsistent with its targets")
            allocated_qty_by_level[level_idx] = qty

        plan: list[dict[str, Any]] = []
        plan_keys: set[tuple[str, int, bool]] = set()

        def append_plan(target: dict[str, Any], raw_qty: Decimal, *, reduce_only: bool):
            # Every initial quantity must support a complete grid round trip.
            # A reduce-only residual may be accepted below the normal minimum,
            # but its later non-reduce counter order would then be impossible.
            qty_text = self._order_qty_text(float(raw_qty), reduce_only=False)
            if not qty_text:
                raise RuntimeError(
                    "A pending grid order is below exchange quantity precision or minimum: "
                    f"{target['side']} level {target['level_idx']} requested={raw_qty}"
                )
            open_price_text = self._round_trip_open_price_text(int(target["level_idx"]))
            if not self._meets_min_notional(open_price_text, qty_text):
                raise RuntimeError(
                    "A pending grid round trip is below the exchange minimum notional: "
                    f"{target['side']} level {target['level_idx']} "
                    f"notional={self._limit_notional(open_price_text, qty_text)} "
                    f"minimum={self.min_notional}; increase the per-grid quantity or investment"
                )
            plan_key = (str(target["side"]), int(target["level_idx"]), reduce_only)
            if plan_key in plan_keys:
                raise RuntimeError(
                    "Pending grid plan contains a duplicate order target: "
                    f"{target['side']} level {target['level_idx']} reduce_only={reduce_only}"
                )
            plan_keys.add(plan_key)
            plan.append(
                {
                    "side": str(target["side"]),
                    "price": float(target["price"]),
                    "price_text": str(target["price_text"]),
                    "level_idx": int(target["level_idx"]),
                    "reduce_only": reduce_only,
                    "qty_override": float(qty_text),
                    "qty_text": qty_text,
                    "entry_price": self.initial_entry_price if reduce_only else None,
                }
            )

        if direction in {"long", "short"}:
            for target, allocated_qty in zip(profit_targets, allocated_qtys, strict=True):
                append_plan(target, allocated_qty, reduce_only=True)
            for target in add_targets:
                append_plan(
                    target,
                    allocated_qty_by_level.get(int(target["level_idx"]), qty_per_grid),
                    reduce_only=False,
                )
        else:
            for target, allocated_qty in zip(add_targets, allocated_qtys, strict=True):
                append_plan(target, allocated_qty, reduce_only=False)

        if not plan:
            raise RuntimeError("Pending grid plan has no exchange orders")
        return plan

    @staticmethod
    def _initial_grid_plan_key(spec: dict[str, Any]) -> str:
        return "|".join(
            (
                str(spec["side"]),
                str(int(spec["level_idx"])),
                "1" if bool(spec.get("reduce_only")) else "0",
            )
        )

    def _initial_grid_target_count(self) -> int:
        if not self.initial_grid_deployment_pending:
            return 0
        pending = self._pending_targets or {}
        total_count = len(pending.get("add_targets") or [])
        if self.config.get("direction") in {"long", "short"}:
            total_count += len(pending.get("profit_targets") or [])
        return total_count

    def _initial_grid_shape_error(self, spec: dict[str, Any], order: dict[str, Any]) -> str:
        if order.get("accepted_shape_mismatch"):
            return str(order.get("accepted_shape_mismatch"))
        try:
            if int(order.get("level_idx", -1)) != int(spec["level_idx"]):
                return (
                    f"level expected={spec['level_idx']} "
                    f"actual={order.get('level_idx')}"
                )
        except (TypeError, ValueError):
            return f"level expected={spec['level_idx']} actual={order.get('level_idx')}"
        if str(order.get("order_type") or "Limit").lower() != "limit":
            return f"order type expected=Limit actual={order.get('order_type')}"
        expected = {
            "side": str(spec["side"]),
            "price": str(spec["price_text"]),
            "qty": str(spec["qty_text"]),
            "reduce_only": bool(spec["reduce_only"]),
            "order_type": "Limit",
        }
        actual = {
            "side": order.get("side"),
            "price": order.get("price"),
            "qty": order.get("qty"),
            "reduceOnly": order.get("reduce_only"),
        }
        return self._accepted_shape_mismatch_reason(expected, actual)

    def _initial_grid_deployment_entry(
        self,
        spec: dict[str, Any],
        link_id: str,
    ) -> dict[str, Any]:
        return {
            "link_id": str(link_id),
            "side": str(spec["side"]),
            "level_idx": int(spec["level_idx"]),
            "price": str(spec["price_text"]),
            "qty": str(spec["qty_text"]),
            "reduce_only": bool(spec["reduce_only"]),
        }

    def _initial_grid_missing_plan(
        self,
        plan: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
        plan_by_key = {self._initial_grid_plan_key(spec): spec for spec in plan}
        if len(plan_by_key) != len(plan):
            raise RuntimeError("Initial grid deployment plan contains duplicate target identities")

        for key, entry in self.initial_grid_deployment_ledger.items():
            spec = plan_by_key.get(key)
            if spec is None:
                raise RuntimeError(
                    f"Initial grid deployment ledger contains unexpected target {key}"
                )
            if not str(entry.get("link_id") or ""):
                raise RuntimeError(
                    f"Initial grid deployment ledger target {key} has no client order ID"
                )
            shape_error = self._initial_grid_shape_error(spec, entry)
            if shape_error:
                raise RuntimeError(
                    f"Initial grid deployment ledger shape differs for {key}: {shape_error}"
                )
            active = self.active_orders.get(str(entry["link_id"]))
            if active:
                shape_error = self._initial_grid_shape_error(spec, active)
                if shape_error:
                    raise RuntimeError(
                        f"Initial grid active order differs for {key}: {shape_error}"
                    )

        active_by_plan_key: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for link_id, order in self.active_orders.items():
            try:
                key = self._initial_grid_plan_key(order)
            except (KeyError, TypeError, ValueError):
                continue
            spec = plan_by_key.get(key)
            if spec is None:
                continue
            if key in self.initial_grid_deployment_ledger:
                # Once the original target is durably submitted, later partial
                # fills and exact remainder/counter orders belong to the normal
                # execution ledger. They must not be mistaken for a second
                # initial target or compared with the original full quantity.
                evolved_spec = {**spec, "qty_text": str(order.get("qty", ""))}
                shape_error = self._initial_grid_shape_error(evolved_spec, order)
                try:
                    evolved_qty = Decimal(str(order.get("qty", 0) or 0))
                except Exception:
                    evolved_qty = Decimal("0")
                if shape_error or evolved_qty <= 0:
                    raise RuntimeError(
                        "Initial grid evolved order differs from its submitted target "
                        f"{key}: {shape_error or 'quantity is not positive'}"
                    )
                continue
            shape_error = self._initial_grid_shape_error(spec, order)
            if shape_error:
                raise RuntimeError(
                    f"Initial grid active order differs for {key}: {shape_error}"
                )
            active_by_plan_key.setdefault(key, []).append((str(link_id), order))

        for key, matches in active_by_plan_key.items():
            if len(matches) > 1:
                raise RuntimeError(
                    f"Initial grid deployment has duplicate active orders for target {key}"
                )
            if key not in self.initial_grid_deployment_ledger:
                link_id, _ = matches[0]
                self.initial_grid_deployment_ledger[key] = self._initial_grid_deployment_entry(
                    plan_by_key[key],
                    link_id,
                )

        missing = [
            spec
            for spec in plan
            if self._initial_grid_plan_key(spec)
            not in self.initial_grid_deployment_ledger
        ]
        return plan_by_key, missing

    def _initial_grid_rate_limit_retry_remaining(
        self,
        missing: list[dict[str, Any]],
    ) -> float:
        remaining = self._rate_limit_remaining()
        for spec in missing:
            shape_key = self._order_shape_key(
                str(spec["side"]),
                str(spec["price_text"]),
                str(spec["qty_text"]),
                bool(spec["reduce_only"]),
            )
            retry = self._order_rejection_backoff.get(shape_key) or {}
            if is_exchange_rate_limit_message(retry.get("message")):
                remaining = max(remaining, self._order_shape_retry_remaining(shape_key))
        return remaining

    def _set_initial_grid_deployment_waiting_message(
        self,
        total_count: int,
        retry_remaining: float,
    ) -> None:
        submitted_count = len(self.initial_grid_deployment_ledger)
        wait_text = (
            f"; retrying after {int(retry_remaining + 0.999)} second(s)"
            if retry_remaining > 0
            else ""
        )
        self.trigger_message = (
            "Initial grid deployment is incomplete because the exchange rate-limited "
            f"order placement: retained {submitted_count}/{total_count} exact target(s)"
            f"{wait_text}. The confirmed position and existing orders are unchanged; "
            "only the missing original targets will be resumed."
        )
        self._persist_state()

    def _fail_initial_grid_deployment(self, reason: Any) -> None:
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger.clear()
        self.initialization_in_progress = False
        self.initialization_failed = True
        self.manual_stop_pending = True
        self.grid_ready = False
        if self._qty_reaches_accounting_step(self._grid_position_qty()):
            self.trigger_message = (
                f"Opening fill was recorded, but grid deployment failed: {reason}. "
                "Managed grid orders will be cancelled; the confirmed position is retained "
                "for review and will not be market-closed automatically."
            )
        else:
            self.trigger_message = (
                f"Initial grid deployment failed: {reason}. Managed grid orders will be "
                "cancelled without submitting a position-changing fallback order."
            )
        self._mark_fast_poll()
        self._persist_state()

    def _deploy_pending_targets(self, qty_scale: float = 1.0) -> bool:
        plan = self._validated_pending_target_plan(qty_scale)
        if not self.initial_grid_deployment_pending:
            self.initial_grid_deployment_pending = True
            self.initial_grid_deployment_ledger = {}
            self.initialization_in_progress = True
            self.initialization_failed = False
            self.grid_ready = False
            # This state is durable before the first grid limit order is submitted.
            self._persist_state()

        plan_by_key, missing = self._initial_grid_missing_plan(plan)
        self._persist_state()
        retry_remaining = self._initial_grid_rate_limit_retry_remaining(missing)
        if missing and retry_remaining > 0:
            self._set_initial_grid_deployment_waiting_message(len(plan), retry_remaining)
            return False

        placed_links: list[str] = []
        if missing and self._supports_batch_orders():
            placed_links.extend(self._place_batch_limit_orders(missing))
        elif missing:
            for spec in missing:
                link_id = self._place(
                    str(spec["side"]),
                    float(spec["price"]),
                    int(spec["level_idx"]),
                    reduce_only=bool(spec["reduce_only"]),
                    qty_override=float(spec["qty_override"]),
                    entry_price=spec.get("entry_price"),
                )
                if link_id:
                    placed_links.append(link_id)

        for link_id in set(placed_links):
            order = self.active_orders.get(link_id)
            if not order:
                raise RuntimeError(
                    f"Initial grid client order ID {link_id} is missing from the durable ledger"
                )
            key = self._initial_grid_plan_key(order)
            spec = plan_by_key.get(key)
            if spec is None:
                raise RuntimeError(
                    f"Initial grid accepted an unexpected target for client order ID {link_id}"
                )
            shape_error = self._initial_grid_shape_error(spec, order)
            if shape_error:
                raise RuntimeError(
                    "Initial grid deployment accepted an order with a different exchange "
                    f"shape: {shape_error}"
                )
            self.initial_grid_deployment_ledger[key] = self._initial_grid_deployment_entry(
                spec,
                link_id,
            )

        _, missing = self._initial_grid_missing_plan(plan)
        if missing:
            retry_remaining = self._initial_grid_rate_limit_retry_remaining(missing)
            if retry_remaining > 0:
                self._set_initial_grid_deployment_waiting_message(len(plan), retry_remaining)
                return False
            raise RuntimeError(
                "Initial grid deployment is incomplete: "
                f"prepared {len(plan)} order(s), exchange confirmed or retained "
                f"{len(self.initial_grid_deployment_ledger)}; the grid was not marked ready"
            )

        self.grid_ready = True
        self.waiting_initial_order = False
        self.initialization_in_progress = False
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger = {}
        self.trigger_message = ""
        self._pending_targets = None
        self._persist_state()
        return True

    def _risk_hit(self, current_price: float) -> bool:
        direction = self.config["direction"]
        stop_loss = self.config.get("stop_loss_price")
        take_profit = self.config.get("take_profit_price")

        if direction == "long":
            if stop_loss is not None and current_price <= float(stop_loss):
                self.trigger_message = f"Stop loss hit at {current_price}"
                return True
            if take_profit is not None and current_price >= float(take_profit):
                self.trigger_message = f"Take profit hit at {current_price}"
                return True
        elif direction == "short":
            if stop_loss is not None and current_price >= float(stop_loss):
                self.trigger_message = f"Stop loss hit at {current_price}"
                return True
            if take_profit is not None and current_price <= float(take_profit):
                self.trigger_message = f"Take profit hit at {current_price}"
                return True
        else:
            if stop_loss is not None and current_price <= float(stop_loss):
                self.trigger_message = f"Stop loss hit at {current_price}"
                return True
            if take_profit is not None and current_price >= float(take_profit):
                self.trigger_message = f"Take profit hit at {current_price}"
                return True

        return False

    def _close_all_positions(self) -> bool:
        if self.pending_reduce_action and not self._resolve_pending_reduce_action():
            return False

        direction = self.config["direction"]
        if direction in {"long", "short"}:
            position_side = "Buy" if direction == "long" else "Sell"
        else:
            grid_net_qty = self._grid_position_net_qty()
            if not self._qty_reaches_accounting_step(grid_net_qty):
                return True
            position_side = "Buy" if grid_net_qty > 0 else "Sell"

        close_side = "Sell" if position_side == "Buy" else "Buy"
        size = self._safe_grid_close_qty(position_side)
        if not self._qty_reaches_accounting_step(size):
            if not self._qty_reaches_accounting_step(self._grid_position_qty()):
                return True
            self.trigger_message = (
                "Risk close paused because exchange position and the grid-owned ledger do not "
                "provide an authoritative close quantity."
            )
            self._persist_state()
            return False

        self._place_reduce_market(
            close_side,
            size,
            "stop-loss/take-profit grid shutdown",
            tag="risk_close",
        )
        if self.pending_reduce_action and not self._resolve_pending_reduce_action():
            return False
        return not self._qty_reaches_accounting_step(self._grid_position_qty())

    async def _shutdown_with_close(self) -> bool:
        self.risk_shutdown_pending = True
        self._stopping = True
        try:
            if not self._cancel_managed_orders_once():
                self.trigger_message = (
                    "Risk shutdown is waiting for managed limit orders to reach terminal "
                    "exchange status before closing the remaining grid position."
                )
                self._mark_fast_poll()
                self._persist_state()
                return False
            if not self._close_all_positions():
                self._mark_fast_poll()
                self._persist_state()
                return False

            actual_delta = self._actual_position_net_qty() - self._baseline_position_net_qty()
            if self._qty_reaches_accounting_step(actual_delta):
                self.trigger_message = (
                    "Risk shutdown close is exchange-terminal, but the actual position still "
                    "differs from the protected baseline; no guessed follow-up order was sent."
                )
                self._mark_fast_poll()
                self._persist_state()
                return False

            self.running = False
            self.grid_ready = False
            self.risk_shutdown_pending = False
            self.paused_replacements.clear()
            self._clear_restore_refresh_state()
            self._persist_state()
            return True
        except Exception as exc:
            logger.exception("Risk shutdown failed: %s", exc)
            self.trigger_message = f"Risk shutdown failed safely and will retry: {exc}"
            self._mark_fast_poll()
            self._persist_state()
            return False

    async def _run_loop(self):
        while self.running:
            try:
                if self.manual_stop_pending:
                    self._stopping = True
                    if self.pending_reduce_action:
                        self._resolve_pending_reduce_action()
                    if self._cancel_managed_orders_once() and not self.pending_reduce_action:
                        self.running = False
                        self.grid_ready = False
                        self.manual_stop_pending = False
                        self.risk_shutdown_pending = False
                        self.paused_replacements.clear()
                        self._clear_restore_refresh_state()
                        self._persist_state()
                        break
                    self.trigger_message = (
                        "Manual stop recovery is reconciling managed orders; normal grid "
                        "placement remains disabled."
                    )
                    self._mark_fast_poll()
                    self._persist_state()
                    await self._sleep_until_next_poll()
                    continue

                if self.risk_shutdown_pending:
                    if await self._shutdown_with_close():
                        break
                    await self._sleep_until_next_poll()
                    continue

                if self.pending_reduce_action and not self._resolve_pending_reduce_action():
                    await self._sleep_until_next_poll()
                    continue

                rate_limit_remaining = self._rate_limit_remaining()
                if rate_limit_remaining > 0:
                    if self.initial_grid_deployment_pending:
                        self._set_initial_grid_deployment_waiting_message(
                            self._initial_grid_target_count(),
                            rate_limit_remaining,
                        )
                    else:
                        self.trigger_message = (
                            "Exchange rate limit reached; normal grid requests are paused for "
                            f"{int(rate_limit_remaining + 0.999)} second(s) without dropping any ledger work."
                        )
                        self._persist_state()
                    await self._sleep_until_next_poll()
                    continue
                if self.trigger_message.startswith("Exchange rate limit reached;"):
                    self.trigger_message = ""
                    self._persist_state()

                if self.restore_refresh_pending:
                    if time.time() < self.restore_refresh_retry_after:
                        await self._sleep_until_next_poll()
                        continue
                    if not self._complete_restore_refresh():
                        self._persist_state()
                        await self._sleep_until_next_poll()
                        continue
                    self._persist_state()

                self.current_price = self._get_current_price()

                if self.waiting_trigger and self._is_trigger_hit(self.current_price):
                    self._deploy_initial_grid(self.current_price)

                if self.waiting_initial_order:
                    await self._check_initial_order()

                if self.initial_grid_deployment_pending:
                    retry_remaining = self._rate_limit_remaining()
                    if retry_remaining > 0:
                        await self._sleep_until_next_poll()
                        continue
                    try:
                        deployment_complete = self._deploy_pending_targets()
                    except Exception as exc:
                        self._fail_initial_grid_deployment(exc)
                        await self._sleep_until_next_poll()
                        continue
                    if not deployment_complete:
                        await self._sleep_until_next_poll()
                        continue

                if self.grid_ready and self._risk_hit(self.current_price):
                    if await self._shutdown_with_close():
                        break
                    await self._sleep_until_next_poll()
                    continue

                if self.grid_ready:
                    await self._check_fills()
                    self._resume_paused_replacements()
                    self._reconcile_grid_position_protection()

                await self._sleep_until_next_poll()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if isinstance(exc, ExchangeRateLimitError) or is_exchange_rate_limit_message(
                    exc
                ):
                    retry_after = (
                        exc.retry_after
                        if isinstance(exc, ExchangeRateLimitError)
                        else ORDER_REJECTION_BACKOFF_MAX_SECONDS
                    )
                    self._record_order_rejection(
                        "",
                        str(exc),
                        rate_limit_retry_after=retry_after,
                        track_shape=False,
                    )
                    remaining = self._rate_limit_remaining()
                    self.trigger_message = (
                        "Exchange rate limit reached; normal grid requests are paused for "
                        f"{int(remaining + 0.999)} second(s) without dropping any ledger work."
                    )
                    logger.warning(
                        "Exchange rate limit paused grid polling symbol=%s retry_after=%.3f msg=%s",
                        self.config.get("symbol"),
                        remaining,
                        exc,
                    )
                    self._persist_state()
                    await self._sleep_until_next_poll()
                    continue
                logger.exception("Grid polling failed: %s", exc)
                await asyncio.sleep(1 if self.waiting_initial_order else 5)

    def _repair_boundary_position(self):
        if not bool(self.config.get("boundary_market_repair", False)):
            return

        now = time.time()
        if self._boundary_repair_in_progress or now < self._boundary_repair_retry_after:
            return

        direction = self.config["direction"]
        lower = float(self.config["lower_price"])
        upper = float(self.config["upper_price"])

        if direction == "short" and self.current_price <= lower:
            position_side = "Sell"
            close_side = "Buy"
            reason = f"short grid below lower boundary {lower}"
        elif direction == "long" and self.current_price >= upper:
            position_side = "Buy"
            close_side = "Sell"
            reason = f"long grid above upper boundary {upper}"
        else:
            return

        position_qty = self._safe_grid_close_qty(position_side)
        if not self._qty_reaches_accounting_step(position_qty):
            return

        self._boundary_repair_in_progress = True
        try:
            if not self._cancel_stale_reduce_orders(close_side):
                self._boundary_repair_retry_after = time.time() + 2
                return
            refreshed_qty = self._safe_grid_close_qty(position_side)
            if self._qty_reaches_accounting_step(refreshed_qty):
                self._place_reduce_market(close_side, refreshed_qty, reason)
                self._boundary_repair_retry_after = time.time() + 2
        finally:
            self._boundary_repair_in_progress = False

    def _cancel_stale_reduce_orders(self, side: str) -> bool:
        open_orders = self._fetch_open_orders()
        all_terminal = True
        for link_id, order in list(self.active_orders.items()):
            if order.get("side") != side or not order.get("reduce_only"):
                continue

            try:
                outcome = self._cancel_managed_order_once(order, open_orders)
                if outcome == "done":
                    self.active_orders.pop(link_id, None)
                else:
                    all_terminal = False
            except Exception as exc:
                all_terminal = False
                logger.warning(
                    "Failed to cancel stale reduce order before boundary repair symbol=%s order_id=%s msg=%s",
                    self.config.get("symbol"),
                    order.get("order_id", ""),
                    exc,
                )
        self._persist_state()
        return all_terminal

    def _replace_unfilled_opening_order(self, status: str):
        order = self.opening_order or {}
        side = order.get("side") or self.initial_side
        qty = float(order.get("qty") or self.initial_qty or 0)
        if not side or qty < self.min_qty:
            self.waiting_initial_order = False
            self.opening_order = None
            self.running = False
            self.trigger_message = "Opening order closed without fills and retry quantity is too small."
            self._clear_restore_refresh_state()
            self._persist_state()
            return

        current_price = self._get_current_price()
        self.current_price = current_price
        if not self._in_grid_range(current_price):
            self.waiting_initial_order = False
            self.opening_order = None
            self.running = False
            self.trigger_message = (
                "Opening order closed without fills and price is outside grid range; "
                "please review before restarting."
            )
            self._clear_restore_refresh_state()
            self._persist_state()
            return

        self.waiting_initial_order = False
        self.opening_order = None
        post_only = str(order.get("time_in_force") or "") == "PostOnly"
        retry_price = self._initial_limit_price(side, current_price, post_only=post_only)
        try:
            self._place_limit_open(side, qty, retry_price, post_only=post_only)
        except Exception as exc:
            if self.opening_order:
                self.grid_ready = False
                self.waiting_initial_order = True
                self.trigger_message = (
                    f"Opening order replacement may already be on the exchange: {exc}. "
                    "The original replacement client order ID is retained and will be "
                    "reconciled without submitting another opening order."
                )
                self._mark_fast_poll()
                with contextlib.suppress(Exception):
                    self._persist_state()
                return
            self.running = False
            self.grid_ready = False
            self.waiting_initial_order = False
            self.opening_order = None
            self.trigger_message = (
                f"Opening order replacement failed before any confirmed fill: {exc}. "
                "The grid was stopped without submitting another opening order."
            )
            self._clear_restore_refresh_state()
            self._persist_state()
            return
        order_label = "Post-only" if post_only else "Limit"
        self.trigger_message = (
            f"{order_label} opening order ended as {status} without fills; "
            f"replaced at {self.opening_order['price']}."
        )
        self._persist_state()

    def _fail_opening_completion(self, reason: str) -> None:
        self.waiting_initial_order = False
        self.opening_order = None
        self.initialization_in_progress = False
        self.initial_grid_deployment_pending = False
        self.initial_grid_deployment_ledger.clear()
        self.initialization_failed = True
        self.manual_stop_pending = True
        self.grid_ready = False
        self.trigger_message = (
            f"Opening fill was recorded, but the fixed-grid opening could not be completed: "
            f"{reason}. The confirmed position is retained for review and will not be "
            "market-closed automatically."
        )
        self._mark_fast_poll()
        self._persist_state()

    def _continue_fixed_grid_opening(self, status: str) -> bool:
        target_qty = self._opening_target_qty_decimal()
        filled_qty = self._normalized_qty_decimal(self.opening_filled_qty, self.qty_step)
        remaining_qty = self._normalized_qty_decimal(
            max(Decimal("0"), target_qty - filled_qty),
            self.qty_step,
        )
        if remaining_qty <= self._qty_tolerance_decimal():
            return True

        order = dict(self.opening_order or {})
        side = str(order.get("side") or self.initial_side)
        order_type = str(order.get("order_type") or "Limit").lower()
        post_only = str(order.get("time_in_force") or "") == "PostOnly"
        self.waiting_initial_order = False
        self.opening_order = None

        try:
            if order_type == "market":
                completed = self._place_market_open(side, float(remaining_qty))
                if not completed:
                    self.trigger_message = (
                        f"Initial market order ended as {status} after a partial fill; "
                        f"waiting for the remaining {self._fq(float(remaining_qty))} opening "
                        "quantity without changing the per-grid plan."
                    )
                    self._persist_state()
                    return False
                return True

            current_price = self._get_current_price()
            self.current_price = current_price
            retry_price = self._initial_limit_price(
                side,
                current_price,
                post_only=post_only,
            )
            if not (self.grid_levels[0] < retry_price < self.grid_levels[-1]):
                raise RuntimeError(
                    f"replacement price {retry_price} is outside the configured grid range"
                )
            self._place_limit_open(
                side,
                float(remaining_qty),
                retry_price,
                post_only=post_only,
            )
        except Exception as exc:
            if self.opening_order:
                self.waiting_initial_order = True
                self.grid_ready = False
                self.trigger_message = (
                    f"Opening remainder may already be on the exchange: {exc}. The retained "
                    "client order ID will be reconciled without another submission."
                )
                self._mark_fast_poll()
                with contextlib.suppress(Exception):
                    self._persist_state()
                return False
            self._fail_opening_completion(str(exc))
            return False

        order_label = "Post-only" if post_only else "Limit"
        self.trigger_message = (
            f"{order_label} opening order ended as {status} after a partial fill; "
            f"refilling the exact remaining {self.opening_order['qty']} before deploying "
            "the unchanged fixed-grid plan."
        )
        self._persist_state()
        return False

    async def _check_initial_order(self):
        if not self.opening_order:
            return

        open_orders = self._fetch_open_orders()
        if self.opening_order.get("submission_pending"):
            if not self._resolve_opening_submission(open_orders):
                return
            if not self.opening_order:
                return

        open_order_ids = {str(item.get("orderId", "")) for item in open_orders}
        order_id = self.opening_order["order_id"]
        if order_id in open_order_ids:
            return

        planned_qty = float(self.opening_order["qty"])
        snapshot = {**self.opening_order, **self._get_order_snapshot(self.opening_order)}
        status = self._order_status_from_snapshot(snapshot)
        stats = self._authoritative_execution_stats(self.opening_order, snapshot)
        if not stats or stats["qty"] <= 0:
            if str(self.opening_order.get("order_type", "")).lower() == "market":
                if self._is_filled_status(status):
                    self.trigger_message = (
                        "Initial market order is filled; waiting for authoritative execution "
                        "quantity and price before deploying the grid."
                    )
                    self._mark_fast_poll()
                    self._persist_state()
                    return
                if self._is_cancelled_status(status):
                    self.waiting_initial_order = False
                    self.opening_order = None
                    self.running = False
                    self.trigger_message = (
                        f"Initial market order ended as {status} without a confirmed fill; "
                        "the grid was not deployed."
                    )
                    self._clear_restore_refresh_state()
                    self._persist_state()
                    return
            if self._is_filled_status(status):
                self.trigger_message = (
                    "Opening order is filled; waiting for authoritative execution quantity "
                    "and price before deploying the grid."
                )
                self._mark_fast_poll()
                self._persist_state()
                return
            if status == "UNKNOWN":
                logger.info(
                    "Opening order absent from open orders but status is unknown; waiting symbol=%s order_id=%s",
                    self.config.get("symbol"),
                    order_id,
                )
                self._mark_fast_poll()
                return
            if self._is_cancelled_status(status):
                self._replace_unfilled_opening_order(status)
                return
            logger.info(
                "Opening order absent from open orders but not terminal symbol=%s order_id=%s status=%s",
                self.config.get("symbol"),
                order_id,
                status,
            )
            self._mark_fast_poll()
            return

        confirmed_qty = Decimal(str(stats.get("qty", 0) or 0))
        planned_qty_decimal = Decimal(str(planned_qty))
        execution_complete = (
            confirmed_qty + self._qty_tolerance_decimal() >= planned_qty_decimal
        )
        partial_execution_is_terminal = self._is_cancelled_status(status)
        if not execution_complete and not partial_execution_is_terminal:
            self.trigger_message = (
                f"Opening order partially filled {self._fq(stats['qty'])}/{self._fq(planned_qty)}; "
                "waiting for final order status before deploying grid."
            )
            self._mark_fast_poll()
            self._persist_state()
            return

        # The exchange fill is authoritative even if the subsequent grid plan
        # cannot be deployed. Record ownership before any validation that can
        # fail so a real position can never disappear from the local ledger.
        self._record_opening_execution_delta(self.opening_order, stats)

        if self._position_sizing_mode() == "fixed_grid_qty":
            target_qty = self._opening_target_qty_decimal()
            filled_qty = self._normalized_qty_decimal(
                self.opening_filled_qty,
                self.qty_step,
            )
            if filled_qty + self._qty_tolerance_decimal() < target_qty:
                if not self._continue_fixed_grid_opening(status):
                    return
                filled_qty = self._normalized_qty_decimal(
                    self.opening_filled_qty,
                    self.qty_step,
                )
            if abs(filled_qty - target_qty) > self._qty_tolerance_decimal():
                self._fail_opening_completion(
                    f"target quantity {target_qty} differs from confirmed quantity {filled_qty}"
                )
                return

        self.opening_order = None
        self.waiting_initial_order = False
        self.initialization_in_progress = True
        self._persist_state()

        try:
            self._prepare_pending_targets_after_opening_fill(
                self.initial_entry_price,
                self.initial_qty,
            )
            self._reset_reduce_lots_from_pending_targets(self.initial_entry_price)
            self._deploy_pending_targets()
        except Exception as exc:
            self._fail_initial_grid_deployment(exc)
            return
        self._persist_state()

    async def _check_fills(self):
        # Counter orders can be placed and filled before the next polling tick.
        # Reconcile a few rounds so protection checks see a stable order ledger.
        for _ in range(3):
            changed = self._reconcile_exchange_open_orders()
            if not changed or self._stopping:
                break

    def _handle_closed_order(
        self,
        order: dict,
        *,
        allow_estimate: bool | None = None,
        snapshot: dict | None = None,
    ) -> bool:
        if allow_estimate is None:
            allow_estimate = not hasattr(self.client, "get_order_trades")
        stats = self._authoritative_execution_stats(
            order,
            snapshot,
            allow_estimate=allow_estimate,
        )
        if not stats or stats["qty"] <= 0:
            return False

        self._record_execution_delta(order, stats)
        return True

    def _queued_replacement_order_plan(self, order: dict) -> dict | None:
        mode = str(order.get("replacement_mode") or "counter_order")
        if mode == "same_order":
            return {
                "side": str(order.get("side") or ""),
                "price": float(order.get("price", 0) or 0),
                "level_idx": int(order.get("level_idx", 0) or 0),
                "reduce_only": bool(order.get("reduce_only")),
                "qty": float(order.get("qty", 0) or 0),
                "entry_price": order.get("entry_price"),
            }
        if mode == "planned_order":
            raw_plan = order.get("replacement_plan") or {}
            return {
                "side": str(raw_plan.get("side") or ""),
                "price": float(raw_plan.get("price", 0) or 0),
                "level_idx": int(raw_plan.get("level_idx", 0) or 0),
                "reduce_only": bool(raw_plan.get("reduce_only")),
                "qty": float(raw_plan.get("qty", 0) or 0),
                "entry_price": raw_plan.get("entry_price"),
            }
        return self._counter_order_plan(order)

    def _bind_fixed_grid_replacement_to_level_coverage(self, order: dict) -> bool:
        if (
            order.get("replacement_mode") != "same_order"
            or order.get("reduce_only")
            or self._position_sizing_mode() != "fixed_grid_qty"
            or self.config.get("direction") not in {"long", "short"}
            or str(order.get("side") or "") != self._open_side_for_direction()
        ):
            return False

        order["replacement_mode"] = "planned_order"
        order["replacement_plan"] = {
            "side": str(order["side"]),
            "price": float(order["price"]),
            "level_idx": int(order["level_idx"]),
            "reduce_only": False,
            "qty": str(order["qty"]),
            "entry_price": order.get("entry_price"),
        }
        return True

    def _paused_replacements_block_reconciliation(self) -> bool:
        for order in self.paused_replacements:
            try:
                plan = self._queued_replacement_order_plan(order)
                side = str((plan or {}).get("side") or "")
                price = float((plan or {}).get("price", 0) or 0)
                qty = float((plan or {}).get("qty", 0) or 0)
            except Exception:
                return True
            if not plan or not side or price <= 0 or qty <= 0:
                return True
            if bool(plan.get("reduce_only")):
                return True

            qty_text = self._order_qty_text(qty, reduce_only=False)
            if qty_text and self._meets_min_notional(self._fp(price), qty_text):
                return True
        return False

    def _coalesce_nonreduce_counter_replacements(self) -> bool:
        changed = False
        retained: list[dict] = []
        groups: dict[tuple[str, str, int, bool], dict] = {}

        for order in self.paused_replacements:
            changed = self._bind_fixed_grid_replacement_to_level_coverage(order) or changed
            replacement_link_id = str(order.get("replacement_link_id") or "")
            if replacement_link_id and replacement_link_id in self.active_orders:
                if order.get("completed_pair_counted"):
                    self.active_orders[replacement_link_id][
                        "completed_pair_counted"
                    ] = True
                changed = True
                continue
            plan = self._queued_replacement_order_plan(order)
            if not plan or bool(plan.get("reduce_only")):
                retained.append(order)
                continue

            key = (
                str(plan["side"]),
                self._fp(float(plan["price"])),
                int(plan["level_idx"]),
                False,
            )
            source_links = list(order.get("replacement_source_links") or [])
            source_link = str(order.get("link_id") or "")
            if source_link and source_link not in source_links:
                source_links.append(source_link)
            order["replacement_source_links"] = source_links

            existing = groups.get(key)
            if existing is None:
                groups[key] = order
                retained.append(order)
                continue

            existing_plan = self._queued_replacement_order_plan(existing) or {}
            total_qty = Decimal(str(existing_plan.get("qty", 0) or 0)) + Decimal(
                str(plan.get("qty", 0) or 0)
            )
            total_qty_text = self._fq(total_qty)
            existing["qty"] = total_qty_text
            existing["replacement_mode"] = "planned_order"
            existing["replacement_plan"] = {
                "side": str(plan["side"]),
                "price": float(plan["price"]),
                "level_idx": int(plan["level_idx"]),
                "reduce_only": False,
                "qty": total_qty_text,
                "entry_price": plan.get("entry_price"),
            }
            # A client order ID is an immutable identity for one exact order
            # shape. Once fragments change the quantity, retire any ID that
            # may have been used by a definitive rejection and persist a fresh
            # identity before the next exchange write.
            existing["replacement_link_id"] = (
                f"g_{int(plan['level_idx'])}_{str(plan['side'])[0]}_"
                f"{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
            )
            existing["replacement_retry_after"] = 0.0
            existing["replacement_retry_attempts"] = 0
            existing_sources = list(existing.get("replacement_source_links") or [])
            for link_id in source_links:
                if link_id and link_id not in existing_sources:
                    existing_sources.append(link_id)
            existing["replacement_source_links"] = existing_sources
            changed = True

        if changed:
            self.paused_replacements = retained
            self._persist_state()
        return changed

    def _resume_paused_replacements(self):
        if not self.paused_replacements or self._stopping:
            return

        self._coalesce_nonreduce_counter_replacements()
        pending = list(self.paused_replacements)

        def dequeue(order: dict) -> None:
            self.paused_replacements = [
                queued for queued in self.paused_replacements if queued is not order
            ]

        def durable_replacement_link(order: dict, side: str, level_idx: int) -> str:
            link_id = str(order.get("replacement_link_id") or "")
            if link_id:
                return link_id
            link_id = (
                f"g_{level_idx}_{side[0]}_"
                f"{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
            )
            order["replacement_link_id"] = link_id
            # Persist the retry identity before any exchange write. The queue
            # remains present until that same identity is in active_orders.
            self._persist_state()
            return link_id

        def preserve_completion_marker(order: dict, link_id: str) -> None:
            if order.get("completed_pair_counted") and link_id in self.active_orders:
                self.active_orders[link_id]["completed_pair_counted"] = True

        def retry_ready(order: dict, now: float) -> bool:
            if now < float(order.get("replacement_retry_after", 0) or 0):
                return False
            return self._rate_limit_remaining() <= 0

        def defer_retry(order: dict, now: float) -> None:
            attempts = int(order.get("replacement_retry_attempts", 0) or 0) + 1
            order["replacement_retry_attempts"] = attempts
            order["replacement_retry_after"] = now + min(
                30.0,
                NORMAL_POLL_SECONDS * (2 ** min(attempts, 4)),
            )

        for order in pending:
            if order.get("replacement_mode") == "same_order":
                now = time.time()
                if not retry_ready(order, now):
                    continue
                side = str(order["side"])
                level_idx = int(order["level_idx"])
                replacement_link_id = durable_replacement_link(order, side, level_idx)
                if replacement_link_id in self.active_orders:
                    preserve_completion_marker(order, replacement_link_id)
                    dequeue(order)
                    self._persist_state()
                    continue
                placed = self._place(
                    side,
                    float(order["price"]),
                    level_idx,
                    reduce_only=bool(order.get("reduce_only")),
                    qty_override=float(order["qty"]),
                    entry_price=order.get("entry_price"),
                    allow_duplicate=True,
                    tag=order.get("tag"),
                    link_id_override=replacement_link_id,
                )
                if placed:
                    preserve_completion_marker(order, replacement_link_id)
                    dequeue(order)
                    self._persist_state()
                else:
                    defer_retry(order, now)
                continue
            now = time.time()
            if not retry_ready(order, now):
                continue
            if not self._should_place_counter_order_now(order):
                continue
            plan = self._counter_order_plan(order)
            if plan:
                replacement_link_id = durable_replacement_link(
                    order,
                    str(plan["side"]),
                    int(plan["level_idx"]),
                )
                if replacement_link_id in self.active_orders:
                    dequeue(order)
                    self._persist_state()
                    continue
            if self._place_counter_order(order):
                dequeue(order)
                self._persist_state()
            else:
                defer_retry(order, now)
        covered_replacements = {
            id(order)
            for order in self.paused_replacements
            if self._fixed_grid_open_replacement_is_covered(order)
        }
        if covered_replacements:
            self.paused_replacements = [
                order
                for order in self.paused_replacements
                if id(order) not in covered_replacements
            ]
        self.trigger_message = (
            ""
            if not self.paused_replacements
            else (
                f"{len(self.paused_replacements)} grid replacement order(s) are still queued; "
                "retrying safely."
            )
        )
        self._persist_state()

    def _order_liquidity_hint(self, order: dict) -> str:
        if str(order.get("order_type", "")).lower() == "market":
            return "taker"
        return "maker" if order.get("time_in_force") == "PostOnly" else "taker"

    @staticmethod
    def _float_field(item: dict, *keys: str) -> float:
        for key in keys:
            value = item.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _order_status_from_snapshot(snapshot: dict | None) -> str:
        if not snapshot:
            return "UNKNOWN"
        status = snapshot.get("orderStatus") or snapshot.get("status")
        return str(status or "UNKNOWN").upper()

    def _get_order_status(self, order: dict) -> str:
        return self._order_status_from_snapshot(self._get_order_snapshot(order))

    @staticmethod
    def _order_snapshot_identity_mismatch_reason(order: dict, snapshot: dict) -> str:
        if not isinstance(snapshot, dict):
            return "result is not an object"

        expected_order_id = str(order.get("order_id", "") or "")
        expected_link_id = str(order.get("link_id", "") or "")
        if not expected_order_id and not expected_link_id:
            return "local order has no exchange or client identity"

        if expected_order_id:
            actual_order_id = str(snapshot.get("orderId", "") or "")
            if actual_order_id != expected_order_id:
                return (
                    f"order ID expected={expected_order_id} "
                    f"actual={actual_order_id or 'missing'}"
                )
        if expected_link_id:
            actual_link_id = str(
                snapshot.get("orderLinkId", "")
                or snapshot.get("order_link_id", "")
                or ""
            )
            if actual_link_id != expected_link_id:
                return (
                    f"client order ID expected={expected_link_id} "
                    f"actual={actual_link_id or 'missing'}"
                )
        return ""

    def _validated_terminal_order_snapshot(
        self,
        order: dict,
        snapshot: dict,
        *,
        source: str,
    ) -> dict:
        identity_reason = self._order_snapshot_identity_mismatch_reason(order, snapshot)
        if identity_reason:
            logger.error(
                "Rejected untrusted order snapshot symbol=%s order_id=%s link_id=%s "
                "source=%s reason=%s",
                self.config.get("symbol"),
                order.get("order_id"),
                order.get("link_id"),
                source,
                identity_reason,
            )
            return {}

        shape_reason = self._accepted_shape_mismatch_reason(order, snapshot)
        if shape_reason:
            logger.error(
                "Terminal order snapshot changed immutable exchange shape "
                "symbol=%s order_id=%s link_id=%s source=%s reason=%s",
                self.config.get("symbol"),
                order.get("order_id"),
                order.get("link_id"),
                source,
                shape_reason,
            )
            self._record_accepted_shape_mismatch(order, snapshot, shape_reason)
            return {}
        return snapshot

    def _get_order_snapshot(self, order: dict) -> dict:
        if not hasattr(self.client, "get_order"):
            return self._get_order_from_history(order)
        try:
            resp = self.client.get_order(self.config["symbol"], str(order.get("order_id", "")))
        except Exception as exc:
            logger.warning("Fetch order status failed order_id=%s msg=%s", order.get("order_id"), exc)
            return self._get_order_from_history(order)
        if not isinstance(resp, dict):
            logger.error(
                "Fetch order status returned an invalid response object order_id=%s",
                order.get("order_id"),
            )
            return self._get_order_from_history(order)
        if resp.get("retCode") != 0:
            logger.warning(
                "Fetch order status rejected order_id=%s msg=%s",
                order.get("order_id"),
                resp.get("retMsg"),
            )
            return self._get_order_from_history(order)

        raw_snapshot = resp.get("result")
        if not isinstance(raw_snapshot, dict) or not raw_snapshot:
            return self._get_order_from_history(order)
        snapshot = self._validated_terminal_order_snapshot(
            order,
            raw_snapshot,
            source="direct order lookup",
        )
        if not snapshot:
            if order.get("accepted_shape_mismatch"):
                return {}
            return self._get_order_from_history(order)
        if self._order_status_from_snapshot(snapshot) == "UNKNOWN":
            history_snapshot = self._get_order_from_history(order)
            if history_snapshot:
                return history_snapshot
        return snapshot

    def _get_order_status_from_history(self, order: dict) -> str:
        return self._order_status_from_snapshot(self._get_order_from_history(order))

    def _get_order_from_history(self, order: dict) -> dict:
        if not hasattr(self.client, "get_order_history"):
            return {}

        order_id = str(order.get("order_id", "") or "")
        link_id = str(order.get("link_id", "") or "")
        try:
            resp = self.client.get_order_history(self.config["symbol"], limit=1000)
        except Exception as exc:
            logger.warning("Fetch order history failed order_id=%s msg=%s", order_id, exc)
            return {}
        if not isinstance(resp, dict):
            logger.error(
                "Fetch order history returned an invalid response object order_id=%s",
                order_id,
            )
            return {}
        if resp.get("retCode") != 0:
            logger.warning(
                "Fetch order history rejected order_id=%s msg=%s",
                order_id,
                resp.get("retMsg"),
            )
            return {}

        result = resp.get("result")
        if not isinstance(result, dict):
            return {}
        items = result.get("list")
        if not isinstance(items, list):
            return {}
        for item in items:
            if not isinstance(item, dict):
                continue
            item_order_id = str(item.get("orderId", "") or "")
            item_link_id = str(
                item.get("orderLinkId", "") or item.get("order_link_id", "") or ""
            )
            matches_any_identity = bool(
                (order_id and item_order_id == order_id)
                or (link_id and item_link_id == link_id)
            )
            if not matches_any_identity:
                continue
            snapshot = self._validated_terminal_order_snapshot(
                order,
                item,
                source="order history",
            )
            if snapshot:
                return snapshot
            return {}
        return {}

    def _execution_stats_from_order_snapshot(
        self,
        snapshot: dict,
        fallback_price: float,
        fallback_qty: float,
        *,
        liquidity_hint: str,
    ) -> dict | None:
        if not snapshot:
            return None

        qty = self._float_field(snapshot, "executedQty", "cumExecQty", "cumQty", "cum_exec_qty")
        if qty <= 0:
            return None

        volume = self._float_field(snapshot, "cumQuote", "cumExecValue", "cum_exec_value", "volume")
        price = self._float_field(snapshot, "avgPrice", "avg_price", "averagePrice")
        if price <= 0 and volume > 0:
            price = volume / qty
        if price <= 0 and liquidity_hint != "maker":
            # A market or marketable GTC fill needs an exchange execution price;
            # the requested/current price is not authoritative P&L evidence.
            return None
        if price <= 0:
            price = fallback_price
        if volume <= 0:
            volume = price * qty

        return {
            "price": price,
            "qty": qty,
            "volume": volume,
            "fee": self._estimate_fee(volume, liquidity_hint),
            "fee_asset": "USDT estimated",
            "fee_source": "estimated",
            "maker_count": 1 if liquidity_hint == "maker" else 0,
            "taker_count": 1 if liquidity_hint != "maker" else 0,
        }

    @staticmethod
    def _is_filled_status(status: str) -> bool:
        return status == "FILLED"

    @staticmethod
    def _is_partial_status(status: str) -> bool:
        return status in {
            "PARTIALLY_FILLED",
            "PARTIALLYFILLED",
            "FILLED_PARTIALLY",
            "PARTIAL_FILLED",
        }

    @staticmethod
    def _is_cancelled_status(status: str) -> bool:
        return status in {
            "CANCELED",
            "CANCELLED",
            "REJECTED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "DEACTIVATED",
            "PARTIALLY_FILLED_CANCELED",
            "PARTIALLYFILLEDCANCELED",
        }

    def _replace_cancelled_order(self, order: dict) -> bool:
        queued = self._queue_exact_replacement(
            order,
            "exchange-confirmed cancellation",
            retry_immediately=True,
        )
        if not queued:
            return False
        replacement_link_id = str(queued.get("replacement_link_id") or "")
        self._resume_paused_replacements()
        placed = replacement_link_id in self.active_orders
        if placed:
            logger.warning(
                "Replaced cancelled grid order symbol=%s old_order_id=%s new_link_id=%s",
                self.config.get("symbol"),
                order.get("order_id"),
                replacement_link_id,
            )
            return True

        if not self._stopping:
            self.trigger_message = (
                "A cancelled grid order could not be replaced; the exact order is queued "
                "for persistent retry and the gap is not being treated as complete."
            )
            self._mark_fast_poll()
            self._persist_state()
        return False

    def _queue_exact_replacement(
        self,
        order: dict,
        reason: str,
        *,
        retry_immediately: bool = False,
    ) -> dict | None:
        if self._stopping:
            return None
        source_link_id = str(order.get("link_id", "") or "")
        already_queued = next(
            (
                item
                for item in self.paused_replacements
                if item.get("replacement_mode") in {"same_order", "planned_order"}
                and str(item.get("replacement_source_link_id", "") or "")
                == source_link_id
            ),
            None,
        )
        if already_queued:
            if retry_immediately:
                already_queued["replacement_retry_after"] = 0.0
                self._persist_state()
            return already_queued

        side = str(order["side"])
        level_idx = int(order["level_idx"])
        use_level_coverage_plan = bool(
            not order.get("reduce_only")
            and self._position_sizing_mode() == "fixed_grid_qty"
            and self.config.get("direction") in {"long", "short"}
            and side == self._open_side_for_direction()
        )
        queued = dict(order)
        for field in (
            "submission_pending",
            "submission_attempts",
            "submission_not_found_count",
            "submission_last_not_found_at",
            "submission_updated_at",
            "submission_retry_blocked",
        ):
            queued.pop(field, None)
        queued.update(
            {
                "status": "QUEUED_REPLACEMENT",
                "replacement_mode": (
                    "planned_order" if use_level_coverage_plan else "same_order"
                ),
                "replacement_source_link_id": source_link_id,
                "replacement_link_id": (
                    f"g_{level_idx}_{side[0]}_"
                    f"{uuid.uuid4().hex[:ORDER_LINK_RANDOM_HEX_LENGTH]}"
                ),
                "replacement_retry_attempts": 0,
                "replacement_retry_after": (
                    0.0 if retry_immediately else time.time() + NORMAL_POLL_SECONDS
                ),
                "replacement_error": str(reason or "replacement rejected"),
            }
        )
        if use_level_coverage_plan:
            queued["replacement_plan"] = {
                "side": side,
                "price": float(order["price"]),
                "level_idx": level_idx,
                "reduce_only": False,
                "qty": str(order["qty"]),
                "entry_price": order.get("entry_price"),
            }
        self.paused_replacements.append(queued)
        self._mark_fast_poll()
        self._persist_state()
        return queued

    def _record_fill(
        self,
        order: dict,
        stats: dict | None = None,
        *,
        count_completed_pair: bool = True,
        persist_state: bool = True,
    ):
        level_idx = order["level_idx"]
        fallback_qty = float(order["qty"])
        fallback_price = float(order["price"])
        stats = stats or self._get_trade_stats(
            order["order_id"],
            fallback_price,
            fallback_qty,
            liquidity_hint=self._order_liquidity_hint(order),
        )
        qty = stats["qty"]
        price = stats["price"]
        gross_profit = 0.0
        ledger_gross_profit = self._record_reduce_lot_fill(order, qty, price)
        self._apply_grid_position_fill(order, qty)

        if order["reduce_only"]:
            if ledger_gross_profit is not None:
                gross_profit = ledger_gross_profit
            else:
                entry_price = float(order.get("entry_price") or 0)
                if entry_price > 0:
                    if self.config["direction"] == "long" and order["side"] == "Sell":
                        gross_profit = (price - entry_price) * qty
                    elif self.config["direction"] == "short" and order["side"] == "Buy":
                        gross_profit = (entry_price - price) * qty
                elif level_idx + 1 < len(self.grid_levels):
                    gross_profit = (
                        self.grid_levels[level_idx + 1] - self.grid_levels[level_idx]
                    ) * qty
            if count_completed_pair:
                self.completed_pairs += 1
        recorded = self._record_trade_value(
            price,
            qty,
            gross_profit,
            volume=stats["volume"],
            fee=stats["fee"],
            fee_asset=stats["fee_asset"],
            fee_source=stats["fee_source"],
        )

        self.filled_orders.append(
            {
                "side": order["side"],
                "price": price,
                "qty": qty,
                "level_idx": level_idx,
                "volume": round(recorded["volume"], 4),
                "fee": round(recorded["fee"], 4),
                "fee_asset": recorded["fee_asset"],
                "fee_source": recorded["fee_source"],
                "fee_conversion_source": stats.get("fee_conversion_source", ""),
                "maker_count": stats.get("maker_count", 0),
                "taker_count": stats.get("taker_count", 0),
                "liquidity": self._liquidity_label(stats),
                "gross_profit": round(recorded["gross_profit"], 4),
                "profit": round(recorded["net_profit"], 4),
                "time": time.time(),
                "reduce_only": order["reduce_only"],
            }
        )
        self.filled_count += 1
        if persist_state:
            self._persist_state()

    def _fixed_grid_open_counter_deficit(
        self,
        side: str,
        level_idx: int,
    ) -> float:
        lots = self._reduce_lot_decimal_map()
        level_target = Decimal(str(self._target_open_qty_for_level(level_idx)))
        lot_qty = lots.get(level_idx, {}).get("qty", Decimal("0"))
        target_qty = float(max(Decimal("0"), level_target - lot_qty))
        return self._active_order_remaining_qty_deficit(
            side,
            level_idx,
            False,
            target_qty,
        )

    def _fixed_grid_open_replacement_is_covered(self, order: dict) -> bool:
        if order.get("replacement_mode") == "same_order":
            return False
        try:
            plan = self._queued_replacement_order_plan(order)
        except Exception:
            return False
        if (
            not plan
            or bool(plan.get("reduce_only"))
            or self._position_sizing_mode() != "fixed_grid_qty"
            or self.config.get("direction") not in {"long", "short"}
            or str(plan.get("side") or "") != self._open_side_for_direction()
            or not self.reduce_lots_complete
        ):
            return False
        return (
            self._fixed_grid_open_counter_deficit(
                str(plan["side"]),
                int(plan["level_idx"]),
            )
            <= 0
        )

    def _place_counter_leg(
        self,
        side: str,
        price: float,
        level_idx: int,
        *,
        reduce_only: bool,
        qty: float,
        entry_price: float | None = None,
        link_id_override: str | None = None,
    ) -> bool:
        requested_qty = float(qty)
        if not reduce_only:
            if (
                self._position_sizing_mode() == "fixed_grid_qty"
                and self.config.get("direction") in {"long", "short"}
                and side == self._open_side_for_direction()
            ):
                if not self.reduce_lots_complete:
                    logger.warning(
                        "Skipped fixed-grid open counter because lot ledger is incomplete "
                        "symbol=%s level=%s side=%s requested=%s",
                        self.config.get("symbol"),
                        level_idx,
                        side,
                        requested_qty,
                    )
                    return False
                deficit = self._fixed_grid_open_counter_deficit(side, level_idx)
            else:
                deficit = self._active_order_remaining_qty_deficit(
                    side,
                    level_idx,
                    reduce_only,
                    requested_qty,
                )
            if deficit <= 0:
                return True
            qty = deficit
        return (
            self._place(
                side,
                price,
                level_idx,
                reduce_only=reduce_only,
                qty_override=qty,
                entry_price=entry_price,
                allow_duplicate=bool(reduce_only)
                or self._has_active_order(side, level_idx, reduce_only),
                link_id_override=link_id_override,
            )
            is not None
        )

    def _counter_order_reduces_grid_position(self, order: dict) -> bool:
        direction = self.config["direction"]
        side = order.get("side")
        level_idx = int(order.get("level_idx", 0) or 0)
        if direction == "long":
            return side == "Buy" and level_idx + 1 < len(self.grid_levels)
        if direction == "short":
            return side == "Sell"
        return False

    def _counter_order_plan(self, order: dict) -> dict | None:
        if order.get("replacement_mode") == "planned_order":
            return self._queued_replacement_order_plan(order)

        direction = self.config["direction"]
        side = order["side"]
        level_idx = int(order.get("level_idx", 0) or 0)
        qty = self._counter_qty_for_order(order)

        if direction == "long":
            if side == "Buy" and level_idx + 1 < len(self.grid_levels):
                return {
                    "side": "Sell",
                    "price": self.grid_levels[level_idx + 1],
                    "level_idx": level_idx,
                    "reduce_only": True,
                    "qty": qty,
                    "entry_price": float(order.get("fill_price") or order.get("price") or 0),
                }
            if side == "Sell":
                return {
                    "side": "Buy",
                    "price": self.grid_levels[level_idx],
                    "level_idx": level_idx,
                    "reduce_only": False,
                    "qty": qty,
                    "entry_price": None,
                }
        elif direction == "short":
            if side == "Sell":
                return {
                    "side": "Buy",
                    "price": self.grid_levels[level_idx],
                    "level_idx": level_idx,
                    "reduce_only": True,
                    "qty": qty,
                    "entry_price": float(order.get("fill_price") or order.get("price") or 0),
                }
            if level_idx + 1 < len(self.grid_levels):
                return {
                    "side": "Sell",
                    "price": self.grid_levels[level_idx + 1],
                    "level_idx": level_idx,
                    "reduce_only": False,
                    "qty": qty,
                    "entry_price": None,
                }
        else:
            if side == "Buy" and level_idx + 1 < len(self.grid_levels):
                return {
                    "side": "Sell",
                    "price": self.grid_levels[level_idx + 1],
                    "level_idx": level_idx,
                    "reduce_only": False,
                    "qty": qty,
                    "entry_price": None,
                }
            if side == "Sell":
                return {
                    "side": "Buy",
                    "price": self.grid_levels[level_idx],
                    "level_idx": level_idx,
                    "reduce_only": False,
                    "qty": qty,
                    "entry_price": None,
                }
        return None

    def _should_place_counter_order_now(self, order: dict) -> bool:
        # A completed grid leg must always restore its opposite grid order.
        # Price being outside the configured range is not a reason to leave a
        # permanent gap; if the limit is marketable, the taker fill is still
        # part of maintaining the grid state. Risky ledger cases are blocked
        # before this method by reduce-protection checks.
        plan = self._counter_order_plan(order)
        if not plan or bool(plan.get("reduce_only")):
            return True

        qty_text = self._order_qty_text(float(plan.get("qty", 0) or 0), reduce_only=False)
        price_text = self._fp(float(plan.get("price", 0) or 0))
        if qty_text and self._meets_min_notional(price_text, qty_text):
            return True

        source_links = list(order.get("replacement_source_links") or [])
        source_link = str(order.get("link_id") or "")
        if source_link and source_link not in source_links:
            source_links.append(source_link)
        for link_id in source_links:
            source = self.active_orders.get(str(link_id))
            if not source:
                continue
            planned = Decimal(str(source.get("qty", 0) or 0))
            processed = Decimal(str(source.get("processed_fill_qty", 0) or 0))
            if planned - processed > self._qty_tolerance_decimal():
                return False
        return True

    def _place_counter_order(self, order: dict) -> bool:
        plan = self._counter_order_plan(order)
        if not plan:
            return True
        if order.get("replacement_mode") == "planned_order" and not bool(
            plan["reduce_only"]
        ):
            if (
                self._position_sizing_mode() == "fixed_grid_qty"
                and self.config.get("direction") in {"long", "short"}
                and str(plan["side"]) == self._open_side_for_direction()
            ):
                return self._place_counter_leg(
                    str(plan["side"]),
                    float(plan["price"]),
                    int(plan["level_idx"]),
                    reduce_only=False,
                    qty=float(plan["qty"]),
                    entry_price=plan.get("entry_price"),
                    link_id_override=str(order.get("replacement_link_id") or "") or None,
                )
            return (
                self._place(
                    str(plan["side"]),
                    float(plan["price"]),
                    int(plan["level_idx"]),
                    reduce_only=False,
                    qty_override=float(plan["qty"]),
                    entry_price=plan.get("entry_price"),
                    allow_duplicate=True,
                    link_id_override=str(order.get("replacement_link_id") or "") or None,
                )
                is not None
            )
        return self._place_counter_leg(
            str(plan["side"]),
            float(plan["price"]),
            int(plan["level_idx"]),
            reduce_only=bool(plan["reduce_only"]),
            qty=float(plan["qty"]),
            entry_price=plan.get("entry_price"),
            link_id_override=str(order.get("replacement_link_id") or "") or None,
        )
