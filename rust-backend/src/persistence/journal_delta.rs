use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use serde::{Deserialize, Serialize};

use crate::{
    domain::{CancellationIntent, ClientOrderId, OrderIntent},
    engine::{
        InventoryExecutionEvent, LevelLot, NeutralLot, ReplacementObligation, StrategyOrderRecord,
        StrategyState,
    },
};

use super::{LedgerSnapshot, journal::JournalState};

#[derive(Serialize, Deserialize)]
pub(crate) struct MapDelta<K: Ord, V> {
    upserts: BTreeMap<K, V>,
    removals: Vec<K>,
}

impl<K: Ord + Clone, V: PartialEq + Clone> MapDelta<K, V> {
    fn between(previous: &BTreeMap<K, V>, next: &BTreeMap<K, V>) -> Self {
        Self {
            upserts: next
                .iter()
                .filter(|(key, value)| previous.get(*key) != Some(*value))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            removals: previous
                .keys()
                .filter(|key| !next.contains_key(*key))
                .cloned()
                .collect(),
        }
    }

    fn apply(self, map: &mut BTreeMap<K, V>) -> io::Result<()> {
        let mut seen = BTreeSet::new();
        for key in self.removals {
            if self.upserts.contains_key(&key)
                || !seen.insert(key.clone())
                || map.remove(&key).is_none()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid journal map removal",
                ));
            }
        }
        map.extend(self.upserts);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StrategyDelta {
    // Small scalar/configuration header is complete; only changed historical map
    // entries are copied and serialized. Exhaustive destructuring below makes a
    // newly added StrategyState field a compile error until recovery handles it.
    header: StrategyState,
    orders: MapDelta<ClientOrderId, StrategyOrderRecord>,
    lots_by_level: MapDelta<u16, LevelLot>,
    neutral_lots: MapDelta<u64, NeutralLot>,
    inventory_events: MapDelta<u64, InventoryExecutionEvent>,
    replacement_obligations: MapDelta<u64, ReplacementObligation>,
}

fn strategy_header(state: &StrategyState) -> StrategyState {
    macro_rules! copy_fields {
        ($($field:ident),* $(,)?) => {{
            let StrategyState {
                $($field,)* orders: _, lots_by_level: _, neutral_lots: _,
                inventory_events: _, replacement_obligations: _,
            } = state;
            StrategyState { $($field: $field.clone(),)*
                orders: BTreeMap::new(), lots_by_level: BTreeMap::new(),
                neutral_lots: BTreeMap::new(), inventory_events: BTreeMap::new(),
                replacement_obligations: BTreeMap::new(),
            }
        }};
    }
    copy_fields!(
        version,
        revision,
        run_id,
        config,
        instrument_rules,
        exchange,
        symbol,
        direction,
        plan,
        lifecycle,
        baseline,
        grid_position_net_quantity,
        opening_filled_quantity,
        opening_filled_value,
        next_order_sequence,
        next_obligation_sequence,
        next_neutral_lot_sequence,
        next_inventory_event_sequence,
        initial_deployment_complete,
        risk_exit_reason,
        risk_trigger_mark_price,
        trigger_armed_price,
        trigger_observed_price,
        triggered_at_ms,
        completed_pairs,
        gross_realized_profit,
        total_volume,
        total_fee,
        failure,
        created_at_ms,
        updated_at_ms
    )
}

impl JournalState for StrategyState {
    type Delta = StrategyDelta;
    const KIND: &'static str = "strategy";

    fn revision(&self) -> u64 {
        self.revision
    }

    fn validate_successor(&self, next: &Self) -> io::Result<()> {
        if self.revision.checked_add(1) != Some(next.revision)
            || self.run_id != next.run_id
            || self.exchange != next.exchange
            || self.symbol != next.symbol
            || self.created_at_ms != next.created_at_ms
            || self.updated_at_ms > next.updated_at_ms
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal strategy successor identity mismatch",
            ));
        }
        Ok(())
    }

    fn delta(&self, next: &Self) -> Self::Delta {
        StrategyDelta {
            header: strategy_header(next),
            orders: MapDelta::between(&self.orders, &next.orders),
            lots_by_level: MapDelta::between(&self.lots_by_level, &next.lots_by_level),
            neutral_lots: MapDelta::between(&self.neutral_lots, &next.neutral_lots),
            inventory_events: MapDelta::between(&self.inventory_events, &next.inventory_events),
            replacement_obligations: MapDelta::between(
                &self.replacement_obligations,
                &next.replacement_obligations,
            ),
        }
    }

    fn apply(&mut self, delta: Self::Delta) -> io::Result<()> {
        let mut header = delta.header;
        self.validate_successor(&header)?;
        if !header.orders.is_empty()
            || !header.lots_by_level.is_empty()
            || !header.neutral_lots.is_empty()
            || !header.inventory_events.is_empty()
            || !header.replacement_obligations.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal strategy identity or scalar header mismatch",
            ));
        }
        delta.orders.apply(&mut self.orders)?;
        delta.lots_by_level.apply(&mut self.lots_by_level)?;
        delta.neutral_lots.apply(&mut self.neutral_lots)?;
        delta.inventory_events.apply(&mut self.inventory_events)?;
        delta
            .replacement_obligations
            .apply(&mut self.replacement_obligations)?;
        header.orders = std::mem::take(&mut self.orders);
        header.lots_by_level = std::mem::take(&mut self.lots_by_level);
        header.neutral_lots = std::mem::take(&mut self.neutral_lots);
        header.inventory_events = std::mem::take(&mut self.inventory_events);
        header.replacement_obligations = std::mem::take(&mut self.replacement_obligations);
        *self = header;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct IntentDelta {
    revision: u64,
    intents: MapDelta<ClientOrderId, OrderIntent>,
    cancellations: MapDelta<ClientOrderId, CancellationIntent>,
}

impl JournalState for LedgerSnapshot {
    type Delta = IntentDelta;
    const KIND: &'static str = "intents";

    fn revision(&self) -> u64 {
        self.revision
    }

    fn validate_successor(&self, next: &Self) -> io::Result<()> {
        if self.revision.checked_add(1) != Some(next.revision) || self.version != next.version {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal intent successor mismatch",
            ));
        }
        Ok(())
    }

    fn delta(&self, next: &Self) -> Self::Delta {
        IntentDelta {
            revision: next.revision,
            intents: MapDelta::between(&self.intents, &next.intents),
            cancellations: MapDelta::between(&self.cancellations, &next.cancellations),
        }
    }

    fn apply(&mut self, delta: Self::Delta) -> io::Result<()> {
        if self.revision.checked_add(1) != Some(delta.revision) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal intent revision mismatch",
            ));
        }
        delta.intents.apply(&mut self.intents)?;
        delta.cancellations.apply(&mut self.cancellations)?;
        self.revision = delta.revision;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn typed_map_delta_preserves_missing_zero_and_removals_after_serialization() {
        let previous = BTreeMap::from([(1u64, Some(Decimal::ONE)), (2, None)]);
        let next = BTreeMap::from([(1u64, None), (3, Some(Decimal::ZERO))]);
        let bytes = serde_json::to_vec(&MapDelta::between(&previous, &next)).unwrap();
        let delta: MapDelta<u64, Option<Decimal>> = serde_json::from_slice(&bytes).unwrap();
        let mut restored = previous;
        delta.apply(&mut restored).unwrap();
        assert_eq!(restored, next);
    }

    #[test]
    fn conflicting_or_duplicate_removals_are_rejected() {
        for delta in [
            MapDelta {
                upserts: BTreeMap::from([(1, 3)]),
                removals: vec![1],
            },
            MapDelta {
                upserts: BTreeMap::new(),
                removals: vec![1, 1],
            },
            MapDelta {
                upserts: BTreeMap::new(),
                removals: vec![2],
            },
        ] {
            let mut state = BTreeMap::from([(1, 2)]);
            assert!(delta.apply(&mut state).is_err());
        }
    }
}
