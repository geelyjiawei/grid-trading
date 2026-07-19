use std::{cmp::Ordering, collections::BTreeMap};

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    ClientOrderId, Exchange, InstrumentRules, OrderIntent, OrderShape, TerminalOrderStatus,
};

pub(crate) fn strategy_client_order_id(
    value: &str,
) -> Result<Option<ClientOrderId>, crate::domain::OrderIntentError> {
    if ["o_", "g_", "c_", "r_"]
        .iter()
        .any(|prefix| value.starts_with(prefix))
    {
        ClientOrderId::parse(value).map(Some)
    } else {
        Ok(None)
    }
}

pub trait ExchangeIdentityGateway: Send + Sync {
    fn exchange(&self) -> Exchange;
}

pub mod aster;
pub mod binance;
pub mod bybit;
mod bybit_codec;
mod codec;
pub mod configured;
mod execution;
pub mod protocol;
pub mod registry;
pub mod trade_xyz;
mod trade_xyz_codec;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementAcknowledgement {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlacementError {
    #[error("order was not submitted to the exchange: {message}")]
    NotSubmitted { message: String },
    #[error("exchange definitively rejected the order: {message}")]
    Definitive {
        code: Option<String>,
        message: String,
    },
    #[error("exchange write outcome is unknown: {message}")]
    Unknown { message: String },
}

#[async_trait]
pub trait OrderPlacementGateway: Send + Sync {
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeverageAcknowledgement {
    pub exchange: Exchange,
    pub symbol: String,
    pub leverage: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LeverageError {
    #[error("leverage request is invalid: {message}")]
    Invalid { message: String },
    #[error("exchange definitively rejected leverage: {message}")]
    Definitive {
        code: Option<String>,
        message: String,
    },
    #[error("exchange leverage outcome is unknown: {message}")]
    Unknown { message: String },
}

#[async_trait]
pub trait LeverageGateway: Send + Sync {
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancellationAcknowledgement {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CancellationError {
    #[error("order cancellation request is invalid: {message}")]
    Invalid { message: String },
    #[error("order cancellation outcome is unknown: {message}")]
    Unknown { message: String },
}

#[async_trait]
pub trait OrderCancellationGateway: Send + Sync {
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActiveOrderStatus {
    New,
    PartiallyFilled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderLifecycle {
    Active(ActiveOrderStatus),
    Terminal(TerminalOrderStatus),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthoritativeOrder {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
    pub exchange: Exchange,
    pub shape: OrderShape,
    pub lifecycle: OrderLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executed_quantity: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrderExecutionProgress {
    pub order: AuthoritativeOrder,
    pub cumulative_quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderLookup {
    Found(AuthoritativeOrder),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("exchange order lookup is inconclusive: {message}")]
pub struct LookupError {
    pub message: String,
}

#[async_trait]
pub trait OrderLookupGateway: Send + Sync {
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError>;
}

#[async_trait]
pub trait OpenOrderSnapshotGateway: Send + Sync {
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<AuthoritativeOrder>, SnapshotError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalOrder {
    /// Opaque exchange-provided order identity. It must remain a string.
    pub exchange_order_id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub side: crate::domain::OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub status: String,
    pub created_at_ms: u64,
}

impl HistoricalOrder {
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.exchange_order_id.is_empty()
            || self.exchange_order_id.len() > 128
            || !self
                .exchange_order_id
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
            || self.symbol.is_empty()
            || !self.symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || self.price < Decimal::ZERO
            || self.quantity <= Decimal::ZERO
            || self.status.is_empty()
            || self.status.len() > 64
            || !self.status.bytes().all(|byte| byte.is_ascii_graphic())
            || self.created_at_ms == 0
        {
            return Err(SnapshotError::new("historical order is invalid"));
        }
        Ok(())
    }
}

#[async_trait]
pub trait OrderHistorySnapshotGateway: Send + Sync {
    async fn order_history_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<HistoricalOrder>, SnapshotError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeFill {
    /// Opaque exchange-provided execution identity. It must never be parsed as
    /// a number unless an exchange's pagination contract explicitly requires it.
    #[serde(with = "trade_id_serde")]
    pub trade_id: String,
    pub exchange_order_id: String,
    pub symbol: String,
    pub side: crate::domain::OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub quote_quantity: Decimal,
    /// Exact signed value returned by the exchange.
    pub raw_commission: Decimal,
    /// Positive fee cost under the exchange-specific commission convention.
    pub commission_cost: Decimal,
    pub commission_asset: String,
    pub realized_profit: Decimal,
    pub is_maker: bool,
    pub trade_time_ms: u64,
}

pub(crate) fn is_valid_trade_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

pub(crate) fn compare_trade_ids(left: &str, right: &str) -> Ordering {
    match (
        decimal_trade_id_magnitude(left),
        decimal_trade_id_magnitude(right),
    ) {
        (Some(left_magnitude), Some(right_magnitude)) => left_magnitude
            .len()
            .cmp(&right_magnitude.len())
            .then_with(|| left_magnitude.cmp(right_magnitude))
            .then_with(|| left.cmp(right)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

pub(crate) fn compare_trade_chronology(
    left_time_ms: u64,
    left_trade_id: &str,
    right_time_ms: u64,
    right_trade_id: &str,
) -> Ordering {
    left_time_ms
        .cmp(&right_time_ms)
        .then_with(|| compare_trade_ids(left_trade_id, right_trade_id))
}

fn decimal_trade_id_magnitude(value: &str) -> Option<&str> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let magnitude = value.trim_start_matches('0');
    Some(if magnitude.is_empty() { "0" } else { magnitude })
}

pub(crate) fn trades_are_canonically_ordered(trades: &[TradeFill]) -> bool {
    trades.windows(2).all(|pair| {
        compare_trade_chronology(
            pair[0].trade_time_ms,
            &pair[0].trade_id,
            pair[1].trade_time_ms,
            &pair[1].trade_id,
        ) != Ordering::Greater
    })
}

pub(crate) mod trade_id_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum CompatibleTradeId {
        Text(String),
        LegacyNumber(u64),
    }

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match CompatibleTradeId::deserialize(deserializer)? {
            CompatibleTradeId::Text(value) => value,
            CompatibleTradeId::LegacyNumber(value) => value.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderExecutionSnapshot {
    pub order: AuthoritativeOrder,
    pub cumulative_quantity: Decimal,
    pub cumulative_quote: Decimal,
    pub fees_by_asset: BTreeMap<String, Decimal>,
    pub trades: Vec<TradeFill>,
    pub order_time_ms: u64,
    pub update_time_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("exchange execution snapshot is inconclusive: {message}")]
pub struct ExecutionSnapshotError {
    pub message: String,
}

impl ExecutionSnapshotError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait ExecutionSnapshotGateway: Send + Sync {
    async fn open_order_execution_progress_snapshot(
        &self,
        _exchange: Exchange,
        _symbol: &str,
    ) -> Result<Option<Vec<OpenOrderExecutionProgress>>, ExecutionSnapshotError> {
        Ok(None)
    }

    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalMinutePrice {
    pub exchange: Exchange,
    pub symbol: String,
    pub minute_start_ms: u64,
    pub open_price: Decimal,
}

#[async_trait]
pub trait HistoricalPriceGateway: Send + Sync {
    async fn historical_minute_open(
        &self,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<HistoricalMinutePrice, SnapshotError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradingFeeRates {
    pub exchange: Exchange,
    pub symbol: String,
    pub maker_rate: Decimal,
    pub taker_rate: Decimal,
}

impl TradingFeeRates {
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.symbol.is_empty()
            || !self.symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || self.maker_rate <= -Decimal::ONE
            || self.maker_rate >= Decimal::ONE
            || self.taker_rate < Decimal::ZERO
            || self.taker_rate >= Decimal::ONE
        {
            return Err(SnapshotError::new("trading fee rates are invalid"));
        }
        Ok(())
    }
}

#[async_trait]
pub trait TradingFeeRateGateway: Send + Sync {
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AccountBalanceUnit {
    Usdt,
    Usd,
    Usdc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountBalanceSnapshot {
    pub exchange: Exchange,
    pub unit: AccountBalanceUnit,
    pub available_balance: Decimal,
    pub wallet_balance: Decimal,
    pub equity: Decimal,
    pub unrealized_profit: Decimal,
}

impl AccountBalanceSnapshot {
    pub fn validate(&self) -> Result<(), SnapshotError> {
        let expected_unit = match self.exchange {
            Exchange::Binance | Exchange::Aster => AccountBalanceUnit::Usdt,
            Exchange::Bybit => AccountBalanceUnit::Usd,
            Exchange::TradeXyz => AccountBalanceUnit::Usdc,
        };
        if self.unit != expected_unit {
            return Err(SnapshotError::new(
                "account balance unit does not match the exchange contract",
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait AccountBalanceSnapshotGateway: Send + Sync {
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeMarketSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub last_price: Decimal,
    pub mark_price: Decimal,
    pub observed_at_ms: u64,
}

impl ExchangeMarketSnapshot {
    pub fn ensure_fresh(
        &self,
        now_ms: u64,
        maximum_age_ms: u64,
        maximum_future_skew_ms: u64,
    ) -> Result<(), SnapshotError> {
        if maximum_age_ms == 0 {
            return Err(SnapshotError::new(
                "market snapshot maximum age must be positive",
            ));
        }
        if self.observed_at_ms
            > now_ms
                .checked_add(maximum_future_skew_ms)
                .ok_or_else(|| SnapshotError::new("market snapshot clock range overflowed"))?
        {
            return Err(SnapshotError::new(
                "market snapshot timestamp is too far in the future",
            ));
        }
        if now_ms.saturating_sub(self.observed_at_ms) > maximum_age_ms {
            return Err(SnapshotError::new("market snapshot is stale"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionSide {
    Both,
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionLeg {
    pub side: PositionSide,
    pub signed_quantity: Decimal,
    pub entry_price: Option<Decimal>,
    pub mark_price: Decimal,
    pub unrealized_profit: Decimal,
    #[serde(default)]
    pub leverage: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub legs: Vec<PositionLeg>,
}

impl PositionSnapshot {
    pub fn one_way_position(&self) -> Result<(Decimal, Option<Decimal>), SnapshotError> {
        if self.legs.len() != 1 || self.legs[0].side != PositionSide::Both {
            return Err(SnapshotError::new(
                "hedge-mode position cannot be represented by the one-way strategy ledger",
            ));
        }
        Ok((self.legs[0].signed_quantity, self.legs[0].entry_price))
    }

    pub fn one_way_leverage(&self) -> Result<u16, SnapshotError> {
        if self.legs.len() != 1 || self.legs[0].side != PositionSide::Both {
            return Err(SnapshotError::new(
                "hedge-mode position has no single strategy leverage",
            ));
        }
        self.legs[0]
            .leverage
            .filter(|leverage| *leverage > 0)
            .ok_or_else(|| SnapshotError::new("position leverage is unavailable"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("exchange snapshot is inconclusive: {message}")]
pub struct SnapshotError {
    pub message: String,
}

impl SnapshotError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait MarketSnapshotGateway: Send + Sync {
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError>;
}

#[async_trait]
pub trait InstrumentRulesGateway: Send + Sync {
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<InstrumentRules, SnapshotError>;
}

#[async_trait]
pub trait PositionSnapshotGateway: Send + Sync {
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError>;
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::domain::{OrderKind, OrderSide, TimeInForce};

    fn snapshot(observed_at_ms: u64) -> ExchangeMarketSnapshot {
        ExchangeMarketSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            last_price: Decimal::new(1011, 0),
            mark_price: Decimal::new(1010, 0),
            observed_at_ms,
        }
    }

    #[test]
    fn market_freshness_rejects_stale_and_future_snapshots() {
        assert!(snapshot(10_000).ensure_fresh(10_500, 1_000, 100).is_ok());
        assert!(snapshot(9_000).ensure_fresh(10_500, 1_000, 100).is_err());
        assert!(snapshot(10_601).ensure_fresh(10_500, 1_000, 100).is_err());
    }

    #[test]
    fn legacy_position_leg_without_leverage_remains_readable_but_not_verified() {
        let leg: PositionLeg = serde_json::from_str(
            r#"{"side":"Both","signed_quantity":"0","entry_price":null,"mark_price":"1010","unrealized_profit":"0"}"#,
        )
        .unwrap();
        assert_eq!(leg.leverage, None);
        let position = PositionSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            legs: vec![leg],
        };
        assert!(position.one_way_leverage().is_err());
    }

    #[test]
    fn legacy_authoritative_order_without_executed_quantity_remains_readable() {
        let order = AuthoritativeOrder {
            client_order_id: ClientOrderId::parse("g_0_B_legacy").unwrap(),
            exchange_order_id: "42".into(),
            exchange: Exchange::Binance,
            shape: OrderShape {
                symbol: "MUUSDT".into(),
                side: OrderSide::Buy,
                price: Some(Decimal::new(1011, 0)),
                quantity: Decimal::ONE,
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
            executed_quantity: Some(Decimal::ZERO),
        };
        let mut encoded = serde_json::to_value(order).unwrap();
        encoded.as_object_mut().unwrap().remove("executed_quantity");

        let restored: AuthoritativeOrder = serde_json::from_value(encoded).unwrap();
        assert_eq!(restored.executed_quantity, None);
    }

    #[test]
    fn balance_snapshot_rejects_a_unit_from_another_exchange_contract() {
        let snapshot = AccountBalanceSnapshot {
            exchange: Exchange::Bybit,
            unit: AccountBalanceUnit::Usdt,
            available_balance: Decimal::ONE,
            wallet_balance: Decimal::ONE,
            equity: Decimal::ONE,
            unrealized_profit: Decimal::ZERO,
        };

        assert!(snapshot.validate().is_err());
    }

    #[test]
    fn canonical_trade_id_order_handles_numeric_magnitude_without_parsing_identity() {
        assert_eq!(compare_trade_ids("9", "10"), Ordering::Less);
        assert_eq!(
            compare_trade_ids(
                "99999999999999999999999999999999999999",
                "100000000000000000000000000000000000000",
            ),
            Ordering::Less
        );
        assert_eq!(compare_trade_ids("exec-a", "exec-b"), Ordering::Less);
        assert_eq!(compare_trade_ids("10", "exec-a"), Ordering::Less);
        assert_eq!(compare_trade_ids("exec-a", "10"), Ordering::Greater);
        assert_eq!(compare_trade_ids("09", "9"), Ordering::Less);
    }

    #[test]
    fn canonical_trade_id_order_is_transitive_across_id_kinds() {
        let mut ids = ["1a", "10", "2", "exec-b", "exec-a", "09", "9"];
        ids.sort_by(|left, right| compare_trade_ids(left, right));

        assert_eq!(ids, ["2", "09", "9", "10", "1a", "exec-a", "exec-b"]);
        for left in 0..ids.len() {
            for middle in left..ids.len() {
                for right in middle..ids.len() {
                    assert_ne!(compare_trade_ids(ids[left], ids[middle]), Ordering::Greater);
                    assert_ne!(
                        compare_trade_ids(ids[middle], ids[right]),
                        Ordering::Greater
                    );
                    assert_ne!(compare_trade_ids(ids[left], ids[right]), Ordering::Greater);
                }
            }
        }
    }
}
