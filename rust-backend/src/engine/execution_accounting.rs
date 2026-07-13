use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::{Exchange, TerminalOrderStatus},
    engine::ExecutionReport,
    exchange::{HistoricalPriceGateway, OrderExecutionSnapshot, OrderLifecycle},
};

const DEFAULT_PRICE_CACHE_ITEMS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeValuationSource {
    ExchangeZero,
    QuoteAsset,
    HistoricalMinuteOpen,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeValuation {
    pub trade_id: u64,
    pub fee_asset: String,
    pub fee_amount: Decimal,
    pub quote_asset: String,
    pub quote_value: Decimal,
    pub source: FeeValuationSource,
    pub valuation_symbol: Option<String>,
    pub valuation_minute_start_ms: Option<u64>,
    pub valuation_price: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValuedExecutionReport {
    pub report: ExecutionReport,
    pub fee_valuations: Vec<FeeValuation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExecutionAccountingError {
    #[error("quote asset is invalid")]
    InvalidQuoteAsset,
    #[error("execution symbol does not end in the configured quote asset")]
    QuoteAssetMismatch,
    #[error("execution snapshot is internally inconsistent")]
    InvalidExecutionSnapshot,
    #[error("historical fee price is unavailable: {0}")]
    HistoricalPriceUnavailable(String),
    #[error("historical fee price identity does not match the request")]
    HistoricalPriceIdentityMismatch,
    #[error("fee valuation arithmetic overflowed")]
    ArithmeticOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PriceCacheKey {
    exchange_code: u8,
    symbol: String,
    minute_start_ms: u64,
}

pub struct ExecutionAccountingService {
    quote_asset: String,
    price_cache: Mutex<BTreeMap<PriceCacheKey, Decimal>>,
    maximum_cache_items: usize,
}

impl std::fmt::Debug for ExecutionAccountingService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionAccountingService")
            .field("quote_asset", &self.quote_asset)
            .field("maximum_cache_items", &self.maximum_cache_items)
            .finish_non_exhaustive()
    }
}

impl ExecutionAccountingService {
    pub fn new(quote_asset: &str) -> Result<Self, ExecutionAccountingError> {
        Self::with_cache_capacity(quote_asset, DEFAULT_PRICE_CACHE_ITEMS)
    }

    pub fn with_cache_capacity(
        quote_asset: &str,
        maximum_cache_items: usize,
    ) -> Result<Self, ExecutionAccountingError> {
        let quote_asset = quote_asset.trim().to_ascii_uppercase();
        if maximum_cache_items == 0
            || quote_asset.is_empty()
            || !quote_asset.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(ExecutionAccountingError::InvalidQuoteAsset);
        }
        Ok(Self {
            quote_asset,
            price_cache: Mutex::new(BTreeMap::new()),
            maximum_cache_items,
        })
    }

    pub async fn value_snapshot<G>(
        &self,
        gateway: &G,
        snapshot: &OrderExecutionSnapshot,
    ) -> Result<ValuedExecutionReport, ExecutionAccountingError>
    where
        G: HistoricalPriceGateway,
    {
        self.validate_snapshot(snapshot)?;
        let mut fee_valuations = Vec::with_capacity(snapshot.trades.len());
        let mut cumulative_fee = Decimal::ZERO;
        for trade in &snapshot.trades {
            let valuation = if trade.commission_cost.is_zero() {
                FeeValuation {
                    trade_id: trade.trade_id,
                    fee_asset: trade.commission_asset.clone(),
                    fee_amount: trade.commission_cost,
                    quote_asset: self.quote_asset.clone(),
                    quote_value: Decimal::ZERO,
                    source: FeeValuationSource::ExchangeZero,
                    valuation_symbol: None,
                    valuation_minute_start_ms: None,
                    valuation_price: None,
                }
            } else if trade.commission_asset == self.quote_asset {
                FeeValuation {
                    trade_id: trade.trade_id,
                    fee_asset: trade.commission_asset.clone(),
                    fee_amount: trade.commission_cost,
                    quote_asset: self.quote_asset.clone(),
                    quote_value: trade.commission_cost,
                    source: FeeValuationSource::QuoteAsset,
                    valuation_symbol: None,
                    valuation_minute_start_ms: None,
                    valuation_price: Some(Decimal::ONE),
                }
            } else {
                let minute_start_ms = trade.trade_time_ms - (trade.trade_time_ms % 60_000);
                if minute_start_ms == 0 {
                    return Err(ExecutionAccountingError::InvalidExecutionSnapshot);
                }
                let valuation_symbol = format!("{}{}", trade.commission_asset, self.quote_asset);
                let price = self
                    .historical_price(
                        gateway,
                        snapshot.order.exchange,
                        &valuation_symbol,
                        minute_start_ms,
                    )
                    .await?;
                let quote_value = trade
                    .commission_cost
                    .checked_mul(price)
                    .ok_or(ExecutionAccountingError::ArithmeticOverflow)?;
                FeeValuation {
                    trade_id: trade.trade_id,
                    fee_asset: trade.commission_asset.clone(),
                    fee_amount: trade.commission_cost,
                    quote_asset: self.quote_asset.clone(),
                    quote_value,
                    source: FeeValuationSource::HistoricalMinuteOpen,
                    valuation_symbol: Some(valuation_symbol),
                    valuation_minute_start_ms: Some(minute_start_ms),
                    valuation_price: Some(price),
                }
            };
            cumulative_fee = cumulative_fee
                .checked_add(valuation.quote_value)
                .ok_or(ExecutionAccountingError::ArithmeticOverflow)?;
            fee_valuations.push(valuation);
        }

        let terminal_status = match snapshot.order.lifecycle {
            OrderLifecycle::Active(_) => None,
            OrderLifecycle::Terminal(status) => Some(status),
        };
        Ok(ValuedExecutionReport {
            report: ExecutionReport {
                client_order_id: snapshot.order.client_order_id.clone(),
                exchange_order_id: snapshot.order.exchange_order_id.clone(),
                cumulative_quantity: snapshot.cumulative_quantity,
                cumulative_quote: snapshot.cumulative_quote,
                cumulative_fee,
                terminal_status,
            },
            fee_valuations,
        })
    }

    fn validate_snapshot(
        &self,
        snapshot: &OrderExecutionSnapshot,
    ) -> Result<(), ExecutionAccountingError> {
        let symbol = &snapshot.order.shape.symbol;
        if !symbol.ends_with(&self.quote_asset) || symbol.len() == self.quote_asset.len() {
            return Err(ExecutionAccountingError::QuoteAssetMismatch);
        }
        if snapshot.order.shape.validate().is_err()
            || snapshot.order_time_ms == 0
            || snapshot.update_time_ms < snapshot.order_time_ms
            || snapshot.cumulative_quantity < Decimal::ZERO
            || snapshot.cumulative_quote < Decimal::ZERO
            || (snapshot.cumulative_quantity.is_zero() && !snapshot.cumulative_quote.is_zero())
            || (snapshot.cumulative_quantity > Decimal::ZERO
                && snapshot.cumulative_quote <= Decimal::ZERO)
        {
            return Err(ExecutionAccountingError::InvalidExecutionSnapshot);
        }
        let mut trade_ids = BTreeSet::new();
        let mut quantity = Decimal::ZERO;
        let mut quote = Decimal::ZERO;
        let mut fees = BTreeMap::new();
        for trade in &snapshot.trades {
            if !trade_ids.insert(trade.trade_id)
                || trade.exchange_order_id != snapshot.order.exchange_order_id
                || trade.symbol != snapshot.order.shape.symbol
                || trade.side != snapshot.order.shape.side
                || trade.price <= Decimal::ZERO
                || trade.quantity <= Decimal::ZERO
                || trade.quote_quantity <= Decimal::ZERO
                || trade.commission_cost < Decimal::ZERO
                || trade.commission_asset.is_empty()
                || !trade
                    .commission_asset
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
                || trade.trade_time_ms == 0
            {
                return Err(ExecutionAccountingError::InvalidExecutionSnapshot);
            }
            quantity = quantity
                .checked_add(trade.quantity)
                .ok_or(ExecutionAccountingError::ArithmeticOverflow)?;
            quote = quote
                .checked_add(trade.quote_quantity)
                .ok_or(ExecutionAccountingError::ArithmeticOverflow)?;
            let current = fees
                .entry(trade.commission_asset.clone())
                .or_insert(Decimal::ZERO);
            *current = current
                .checked_add(trade.commission_cost)
                .ok_or(ExecutionAccountingError::ArithmeticOverflow)?;
        }
        let lifecycle_totals_are_invalid = match snapshot.order.lifecycle {
            OrderLifecycle::Active(crate::exchange::ActiveOrderStatus::New) => {
                !snapshot.cumulative_quantity.is_zero()
            }
            OrderLifecycle::Active(crate::exchange::ActiveOrderStatus::PartiallyFilled) => {
                snapshot.cumulative_quantity <= Decimal::ZERO
                    || snapshot.cumulative_quantity >= snapshot.order.shape.quantity
            }
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled) => {
                snapshot.cumulative_quantity != snapshot.order.shape.quantity
            }
            OrderLifecycle::Terminal(TerminalOrderStatus::Rejected) => {
                !snapshot.cumulative_quantity.is_zero()
            }
            OrderLifecycle::Terminal(
                TerminalOrderStatus::Cancelled | TerminalOrderStatus::Expired,
            ) => false,
        };
        if quantity != snapshot.cumulative_quantity
            || quote != snapshot.cumulative_quote
            || fees != snapshot.fees_by_asset
            || snapshot.cumulative_quantity > snapshot.order.shape.quantity
            || lifecycle_totals_are_invalid
        {
            return Err(ExecutionAccountingError::InvalidExecutionSnapshot);
        }
        Ok(())
    }

    async fn historical_price<G>(
        &self,
        gateway: &G,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<Decimal, ExecutionAccountingError>
    where
        G: HistoricalPriceGateway,
    {
        let key = PriceCacheKey {
            exchange_code: exchange_code(exchange),
            symbol: symbol.to_owned(),
            minute_start_ms,
        };
        if let Some(price) = self
            .price_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .copied()
            && price > Decimal::ZERO
        {
            return Ok(price);
        }
        let price = gateway
            .historical_minute_open(exchange, symbol, minute_start_ms)
            .await
            .map_err(|error| {
                ExecutionAccountingError::HistoricalPriceUnavailable(error.to_string())
            })?;
        if price.exchange != exchange
            || price.symbol != symbol
            || price.minute_start_ms != minute_start_ms
            || price.open_price <= Decimal::ZERO
        {
            return Err(ExecutionAccountingError::HistoricalPriceIdentityMismatch);
        }
        let mut cache = self
            .price_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.insert(key, price.open_price);
        while cache.len() > self.maximum_cache_items {
            let Some(oldest) = cache.keys().next().cloned() else {
                break;
            };
            cache.remove(&oldest);
        }
        Ok(price.open_price)
    }
}

fn exchange_code(exchange: Exchange) -> u8 {
    match exchange {
        Exchange::Binance => 0,
        Exchange::Aster => 1,
        Exchange::Bybit => 2,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        domain::{ClientOrderId, OrderKind, OrderShape, OrderSide, TimeInForce},
        exchange::{
            ActiveOrderStatus, AuthoritativeOrder, HistoricalMinutePrice, SnapshotError, TradeFill,
        },
    };

    #[derive(Clone)]
    struct MockPriceGateway {
        requests: Arc<Mutex<Vec<(Exchange, String, u64)>>>,
        result: Result<HistoricalMinutePrice, SnapshotError>,
    }

    #[async_trait]
    impl HistoricalPriceGateway for MockPriceGateway {
        async fn historical_minute_open(
            &self,
            exchange: Exchange,
            symbol: &str,
            minute_start_ms: u64,
        ) -> Result<HistoricalMinutePrice, SnapshotError> {
            self.requests
                .lock()
                .unwrap()
                .push((exchange, symbol.into(), minute_start_ms));
            self.result.clone()
        }
    }

    fn order(lifecycle: OrderLifecycle) -> AuthoritativeOrder {
        AuthoritativeOrder {
            client_order_id: ClientOrderId::parse("g_0_S_fixed").unwrap(),
            exchange_order_id: "42".into(),
            exchange: Exchange::Binance,
            shape: OrderShape {
                symbol: "MUUSDT".into(),
                side: OrderSide::Sell,
                price: Some(Decimal::new(1595, 2)),
                quantity: Decimal::new(314, 2),
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            lifecycle,
        }
    }

    fn trade(
        id: u64,
        quantity: Decimal,
        quote: Decimal,
        fee: Decimal,
        asset: &str,
        time: u64,
    ) -> TradeFill {
        TradeFill {
            trade_id: id,
            exchange_order_id: "42".into(),
            symbol: "MUUSDT".into(),
            side: OrderSide::Sell,
            price: Decimal::new(1595, 2),
            quantity,
            quote_quantity: quote,
            raw_commission: fee,
            commission_cost: fee,
            commission_asset: asset.into(),
            realized_profit: Decimal::ZERO,
            is_maker: true,
            trade_time_ms: time,
        }
    }

    fn snapshot(trades: Vec<TradeFill>) -> OrderExecutionSnapshot {
        let mut fees = BTreeMap::new();
        for trade in &trades {
            *fees
                .entry(trade.commission_asset.clone())
                .or_insert(Decimal::ZERO) += trade.commission_cost;
        }
        OrderExecutionSnapshot {
            order: order(OrderLifecycle::Terminal(TerminalOrderStatus::Filled)),
            cumulative_quantity: trades.iter().map(|trade| trade.quantity).sum(),
            cumulative_quote: trades.iter().map(|trade| trade.quote_quantity).sum(),
            fees_by_asset: fees,
            trades,
            order_time_ms: 1_020_000,
            update_time_ms: 1_080_000,
        }
    }

    fn gateway(price: HistoricalMinutePrice) -> MockPriceGateway {
        MockPriceGateway {
            requests: Arc::new(Mutex::new(vec![])),
            result: Ok(price),
        }
    }

    #[tokio::test]
    async fn quote_and_bnb_fees_are_valued_without_current_price_guessing() {
        let gateway = gateway(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::new(600, 0),
        });
        let service = ExecutionAccountingService::new("USDT").unwrap();
        let snapshot = snapshot(vec![
            trade(
                1,
                Decimal::new(1, 0),
                Decimal::new(1595, 2),
                Decimal::new(1, 2),
                "BNB",
                1_020_001,
            ),
            trade(
                2,
                Decimal::new(1, 0),
                Decimal::new(1595, 2),
                Decimal::new(2, 2),
                "BNB",
                1_079_999,
            ),
            trade(
                3,
                Decimal::new(114, 2),
                Decimal::new(18183, 3),
                Decimal::new(1, 1),
                "USDT",
                1_080_001,
            ),
        ]);

        let valued = service.value_snapshot(&gateway, &snapshot).await.unwrap();
        let repeated = service.value_snapshot(&gateway, &snapshot).await.unwrap();

        assert_eq!(valued.report.cumulative_fee, Decimal::new(181, 1));
        assert_eq!(
            valued.report.terminal_status,
            Some(TerminalOrderStatus::Filled)
        );
        assert_eq!(valued.fee_valuations.len(), 3);
        assert_eq!(valued, repeated);
        assert_eq!(gateway.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_or_mismatched_historical_price_never_creates_a_report() {
        let missing = MockPriceGateway {
            requests: Arc::new(Mutex::new(vec![])),
            result: Err(SnapshotError::new("missing candle")),
        };
        let wrong_identity = gateway(HistoricalMinutePrice {
            exchange: Exchange::Aster,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::new(600, 0),
        });
        let service = ExecutionAccountingService::new("USDT").unwrap();
        let snapshot = snapshot(vec![trade(
            1,
            Decimal::new(314, 2),
            Decimal::new(50083, 3),
            Decimal::new(1, 2),
            "BNB",
            1_020_001,
        )]);

        assert!(matches!(
            service.value_snapshot(&missing, &snapshot).await,
            Err(ExecutionAccountingError::HistoricalPriceUnavailable(_))
        ));
        assert_eq!(
            service.value_snapshot(&wrong_identity, &snapshot).await,
            Err(ExecutionAccountingError::HistoricalPriceIdentityMismatch)
        );
    }

    #[tokio::test]
    async fn zero_non_quote_fee_requires_no_price_lookup() {
        let gateway = gateway(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::new(600, 0),
        });
        let service = ExecutionAccountingService::new("USDT").unwrap();
        let snapshot = snapshot(vec![trade(
            1,
            Decimal::new(314, 2),
            Decimal::new(50083, 3),
            Decimal::ZERO,
            "BNB",
            1_020_001,
        )]);

        let valued = service.value_snapshot(&gateway, &snapshot).await.unwrap();
        assert_eq!(valued.report.cumulative_fee, Decimal::ZERO);
        assert_eq!(
            valued.fee_valuations[0].source,
            FeeValuationSource::ExchangeZero
        );
        assert!(gateway.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn externally_corrupted_execution_snapshot_is_rejected_before_price_lookup() {
        let gateway = gateway(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::new(600, 0),
        });
        let service = ExecutionAccountingService::new("USDT").unwrap();
        let mut snapshot = snapshot(vec![trade(
            1,
            Decimal::new(314, 2),
            Decimal::new(50083, 3),
            Decimal::new(1, 2),
            "BNB",
            1_020_001,
        )]);
        snapshot.cumulative_quantity = Decimal::new(313, 2);

        assert_eq!(
            service.value_snapshot(&gateway, &snapshot).await,
            Err(ExecutionAccountingError::InvalidExecutionSnapshot)
        );
        assert!(gateway.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn active_order_maps_to_non_terminal_execution_report() {
        let gateway = gateway(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::new(600, 0),
        });
        let service = ExecutionAccountingService::new("USDT").unwrap();
        let mut snapshot = snapshot(vec![trade(
            1,
            Decimal::new(1, 0),
            Decimal::new(1595, 2),
            Decimal::ZERO,
            "USDT",
            1_020_001,
        )]);
        snapshot.order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
        snapshot.order.shape.quantity = Decimal::new(314, 2);

        let valued = service.value_snapshot(&gateway, &snapshot).await.unwrap();
        assert_eq!(valued.report.terminal_status, None);
    }
}
