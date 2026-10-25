use std::{cmp::Ordering, collections::BTreeMap};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    ClientOrderId, Exchange, InstrumentRules, OrderIntent, OrderShape, TerminalOrderStatus,
    is_valid_symbol_for_exchange,
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
pub mod funding;
pub mod protocol;
pub mod realtime;
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

    async fn place_orders(
        &self,
        intents: &[OrderIntent],
        maximum_concurrency: usize,
    ) -> Vec<Result<PlacementAcknowledgement, PlacementError>> {
        stream::iter(intents.iter().cloned())
            .map(|intent| async move { self.place_order(&intent).await })
            .buffered(maximum_concurrency.max(1))
            .collect()
            .await
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancellationTarget {
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

    async fn cancel_orders(
        &self,
        exchange: Exchange,
        symbol: &str,
        targets: &[OrderCancellationTarget],
    ) -> Vec<Result<CancellationAcknowledgement, CancellationError>> {
        stream::iter(targets.iter().cloned().map(|target| async move {
            self.cancel_order(
                exchange,
                symbol,
                &target.client_order_id,
                &target.exchange_order_id,
            )
            .await
        }))
        .buffered(8)
        .collect()
        .await
    }
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
    /// The submission window expired and complete exchange history also excludes this intent.
    ConfirmedNotSubmitted,
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

    async fn lookup_order_intent(
        &self,
        intent: &OrderIntent,
        _now_ms: u64,
    ) -> Result<OrderLookup, LookupError> {
        self.lookup_order_by_client_id(
            intent.exchange,
            &intent.shape.symbol,
            &intent.client_order_id,
        )
        .await
    }
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
            || !is_valid_symbol_for_exchange(self.exchange, &self.symbol)
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
    /// None means the exchange omitted PnL; Some(0) is an explicit reported zero.
    #[serde(default)]
    pub realized_profit: Option<Decimal>,
    pub is_maker: bool,
    pub trade_time_ms: u64,
}

impl TradeFill {
    pub(crate) fn execution_evidence_matches(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }
        if matches!((self.realized_profit, other.realized_profit), (Some(left), Some(right)) if left != right)
        {
            return false;
        }
        // Only omitted optional metadata may differ. Derived equality keeps every
        // economic/identity field (including future additions) exact.
        let mut comparable = other.clone();
        comparable.realized_profit = self.realized_profit;
        self == &comparable
    }

    pub(crate) fn merge_reported_metadata(&mut self, other: &Self) -> bool {
        if !self.execution_evidence_matches(other) {
            return false;
        }
        self.realized_profit = self.realized_profit.or(other.realized_profit);
        true
    }
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
        if !is_valid_symbol_for_exchange(self.exchange, &self.symbol)
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

    async fn funding_history(
        &self,
        _exchange: Exchange,
        _symbol: &str,
        _from_ms: u64,
        _through_ms: u64,
    ) -> Result<funding::FundingHistorySnapshot, SnapshotError> {
        Err(SnapshotError::new("settled funding history is unavailable"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeMarketSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub last_price: Decimal,
    pub mark_price: Decimal,
    #[serde(default)]
    pub price_24h_change_ratio: Option<Decimal>,
    #[serde(default)]
    pub volume_24h: Option<Decimal>,
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
    #[serde(default, serialize_with = "serialize_observed_leverage")]
    pub leverage: Option<Decimal>,
}

pub(crate) fn observed_leverage_json(leverage: Option<Decimal>) -> serde_json::Value {
    match leverage {
        Some(value) if value.fract().is_zero() && value.to_u16().is_some() => {
            serde_json::json!(value.to_u16())
        }
        Some(value) => serde_json::json!(value),
        None => serde_json::Value::Null,
    }
}

fn serialize_observed_leverage<S: serde::Serializer>(
    leverage: &Option<Decimal>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    observed_leverage_json(*leverage).serialize(serializer)
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
        let leverage = self.one_way_leverage_decimal()?;
        if !leverage.fract().is_zero() {
            return Err(SnapshotError::new("position leverage is not an integer"));
        }
        leverage
            .to_u16()
            .ok_or_else(|| SnapshotError::new("position leverage exceeds integer representation"))
    }

    pub fn one_way_leverage_decimal(&self) -> Result<Decimal, SnapshotError> {
        if self.legs.len() != 1 || self.legs[0].side != PositionSide::Both {
            return Err(SnapshotError::new(
                "hedge-mode position has no single strategy leverage",
            ));
        }
        self.legs[0]
            .leverage
            .filter(|leverage| *leverage > Decimal::ZERO)
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
    use std::{
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
        time::Duration,
    };

    use super::*;
    use crate::domain::{OrderKind, OrderSide, TimeInForce};

    fn legacy_pnl_trade() -> serde_json::Value {
        serde_json::json!({
            "trade_id":"fa4268e8-4994-541c-9709-18423acb3515",
            "exchange_order_id":"f89eccf9-f3df-4021-b5bb-d69ce1fb3ab5",
            "symbol":"QUSDT","side":"Buy","price":"0.1162","quantity":"210",
            "quote_quantity":"24.402","raw_commission":"0.0097608",
            "commission_cost":"0.0097608","commission_asset":"USDT",
            "realized_profit":"0","is_maker":true,"trade_time_ms":1792336092619u64
        })
    }

    #[test]
    fn optional_pnl_serde_preserves_legacy_values_and_absence() {
        for text in ["0", "0.54026203", "-0.25"] {
            let mut value = legacy_pnl_trade();
            value["realized_profit"] = serde_json::json!(text);
            let trade: TradeFill = serde_json::from_value(value).unwrap();
            assert_eq!(trade.realized_profit, Some(text.parse().unwrap()));
            let restored: TradeFill =
                serde_json::from_slice(&serde_json::to_vec(&trade).unwrap()).unwrap();
            assert_eq!(restored, trade);
        }
        for omit in [false, true] {
            let mut value = legacy_pnl_trade();
            if omit {
                value.as_object_mut().unwrap().remove("realized_profit");
            } else {
                value["realized_profit"] = serde_json::Value::Null;
            }
            let trade: TradeFill = serde_json::from_value(value).unwrap();
            assert_eq!(trade.realized_profit, None);
            assert!(serde_json::to_value(&trade).unwrap()["realized_profit"].is_null());
        }
    }

    #[test]
    fn optional_pnl_merge_never_changes_economic_evidence_or_explicit_values() {
        let zero: TradeFill = serde_json::from_value(legacy_pnl_trade()).unwrap();
        let mut unknown = zero.clone();
        unknown.realized_profit = None;
        assert!(unknown.merge_reported_metadata(&zero));
        assert_eq!(unknown, zero);
        let mut known = zero.clone();
        let mut missing = zero.clone();
        missing.realized_profit = None;
        assert!(known.merge_reported_metadata(&missing));
        assert_eq!(known, zero);

        for (field, value) in [
            ("realized_profit", serde_json::json!("0.54026203")),
            ("quantity", serde_json::json!("500")),
            ("price", serde_json::json!("0.1163")),
            ("quote_quantity", serde_json::json!("58.1")),
            ("raw_commission", serde_json::json!("0.01")),
            ("commission_cost", serde_json::json!("0.01")),
            ("commission_asset", serde_json::json!("MNT")),
            ("trade_id", serde_json::json!("another-trade")),
            ("exchange_order_id", serde_json::json!("another-order")),
            ("symbol", serde_json::json!("OTHERUSDT")),
            ("side", serde_json::json!("Sell")),
            ("is_maker", serde_json::json!(false)),
            ("trade_time_ms", serde_json::json!(1792336092620u64)),
        ] {
            let mut payload = legacy_pnl_trade();
            payload[field] = value;
            let altered: TradeFill = serde_json::from_value(payload).unwrap();
            let mut baseline = zero.clone();
            assert!(
                !baseline.merge_reported_metadata(&altered),
                "{field} must remain immutable"
            );
            assert_eq!(baseline, zero, "failed merge must be atomic");
        }
    }

    #[derive(Default)]
    struct ConcurrentCancellationGateway {
        active: AtomicUsize,
        maximum_active: AtomicUsize,
    }

    #[async_trait]
    impl OrderCancellationGateway for ConcurrentCancellationGateway {
        async fn cancel_order(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
            exchange_order_id: &str,
        ) -> Result<CancellationAcknowledgement, CancellationError> {
            let active = self.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            self.maximum_active
                .fetch_max(active, AtomicOrdering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.active.fetch_sub(1, AtomicOrdering::SeqCst);
            Ok(CancellationAcknowledgement {
                client_order_id: client_order_id.clone(),
                exchange_order_id: exchange_order_id.into(),
            })
        }
    }

    fn snapshot(observed_at_ms: u64) -> ExchangeMarketSnapshot {
        ExchangeMarketSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            last_price: Decimal::new(1011, 0),
            mark_price: Decimal::new(1010, 0),
            price_24h_change_ratio: None,
            volume_24h: None,
            observed_at_ms,
        }
    }

    #[test]
    fn market_freshness_rejects_stale_and_future_snapshots() {
        assert!(snapshot(10_000).ensure_fresh(10_500, 1_000, 100).is_ok());
        assert!(snapshot(9_000).ensure_fresh(10_500, 1_000, 100).is_err());
        assert!(snapshot(10_601).ensure_fresh(10_500, 1_000, 100).is_err());
    }

    #[tokio::test]
    async fn default_exact_cancellations_are_concurrent_but_bounded() {
        let gateway = ConcurrentCancellationGateway::default();
        let targets = (0..20)
            .map(|index| OrderCancellationTarget {
                client_order_id: ClientOrderId::parse(format!("g_{index}_B_cancel")).unwrap(),
                exchange_order_id: index.to_string(),
            })
            .collect::<Vec<_>>();

        let results = gateway
            .cancel_orders(Exchange::Bybit, "MUUSDT", &targets)
            .await;

        assert!(results.iter().all(Result::is_ok));
        assert_eq!(gateway.maximum_active.load(AtomicOrdering::SeqCst), 8);
        assert_eq!(gateway.active.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn bybit_start_leverage_json_preserves_legacy_integer_and_exact_fraction() {
        for (leverage, expected) in [
            (serde_json::json!(10), serde_json::json!(10)),
            (serde_json::json!("10.0"), serde_json::json!(10)),
            (serde_json::json!("12.5"), serde_json::json!("12.5")),
        ] {
            let leg: PositionLeg = serde_json::from_value(serde_json::json!({
                "side":"Both", "signed_quantity":"0", "entry_price":null,
                "mark_price":"0.1", "unrealized_profit":"0", "leverage":leverage
            }))
            .unwrap();
            assert_eq!(serde_json::to_value(&leg).unwrap()["leverage"], expected);
        }
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
