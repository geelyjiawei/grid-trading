use std::{
    env,
    process::ExitCode,
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use grid_trading_server::{
    domain::{
        ClientOrderId, Exchange, InstrumentRules, OrderIntent, OrderKind, OrderShape, OrderSide,
        TerminalOrderStatus, TimeInForce,
    },
    exchange::{
        AccountBalanceSnapshotGateway, InstrumentRulesGateway, MarketSnapshotGateway,
        OpenOrderSnapshotGateway, OrderCancellationGateway, OrderLifecycle, OrderLookup,
        OrderLookupGateway, OrderPlacementGateway, PositionSnapshot, PositionSnapshotGateway,
        TradingFeeRateGateway,
        configured::{ExchangeCredentials, ExchangeEnvironment, ExchangeGatewayFactory},
    },
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const ORDER_CONFIRMATION: &str = "TESTNET_ORDER_CYCLE";
const LOOKUP_ATTEMPTS: usize = 30;
const LOOKUP_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    ReadOnly,
    OrderCycle,
}

#[derive(Debug)]
struct Configuration {
    exchange: Exchange,
    symbol: String,
    mode: Mode,
    iterations: usize,
    explicit_quantity: Option<Decimal>,
}

impl Configuration {
    fn from_environment() -> Result<Self> {
        let exchange = parse_exchange(&required("GRID_TESTNET_EXCHANGE")?)?;
        let symbol = required("GRID_TESTNET_SYMBOL")?.to_ascii_uppercase();
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            bail!("GRID_TESTNET_SYMBOL must contain only uppercase letters and digits");
        }
        let mode = match env::var("GRID_TESTNET_MODE")
            .unwrap_or_else(|_| "read_only".into())
            .trim()
        {
            "read_only" => Mode::ReadOnly,
            "order_cycle" => Mode::OrderCycle,
            _ => bail!("GRID_TESTNET_MODE must be read_only or order_cycle"),
        };
        if mode == Mode::OrderCycle
            && env::var("GRID_TESTNET_CONFIRM").as_deref() != Ok(ORDER_CONFIRMATION)
        {
            bail!(
                "order-cycle testing is locked; set GRID_TESTNET_CONFIRM={ORDER_CONFIRMATION} only in the isolated testnet environment"
            );
        }
        let iterations = env::var("GRID_TESTNET_ITERATIONS")
            .unwrap_or_else(|_| "1".into())
            .parse::<usize>()
            .context("GRID_TESTNET_ITERATIONS must be an integer")?;
        if !(1..=20).contains(&iterations) {
            bail!("GRID_TESTNET_ITERATIONS must be between 1 and 20");
        }
        let explicit_quantity = env::var("GRID_TESTNET_ORDER_QTY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Decimal::from_str(value.trim()))
            .transpose()
            .context("GRID_TESTNET_ORDER_QTY must be a decimal")?;
        Ok(Self {
            exchange,
            symbol,
            mode,
            iterations,
            explicit_quantity,
        })
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(report) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("report is serializable")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "ok": false,
                    "environment": "testnet",
                    "error": format!("{error:#}"),
                }))
                .expect("error report is serializable")
            );
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<Value> {
    let configuration = Configuration::from_environment()?;
    let credentials = credentials(configuration.exchange)?;
    let gateway = ExchangeGatewayFactory::standard(ExchangeEnvironment::Testnet)?
        .build(credentials)?
        .shared();

    let snapshot_started = Instant::now();
    let market = gateway
        .market_snapshot(configuration.exchange, &configuration.symbol)
        .await
        .context("testnet market snapshot failed")?;
    let rules = gateway
        .instrument_rules(configuration.exchange, &configuration.symbol)
        .await
        .context("testnet instrument-rules snapshot failed")?;
    rules
        .validate()
        .context("testnet instrument rules are invalid")?;
    let balance = gateway
        .account_balance_snapshot(configuration.exchange)
        .await
        .context("testnet account snapshot failed")?;
    let position = gateway
        .position_snapshot(configuration.exchange, &configuration.symbol)
        .await
        .context("testnet position snapshot failed")?;
    let fees = gateway
        .trading_fee_rates(configuration.exchange, &configuration.symbol)
        .await
        .context("testnet fee snapshot failed")?;
    let open_orders = gateway
        .open_orders_snapshot(configuration.exchange, &configuration.symbol)
        .await
        .context("testnet open-order snapshot failed")?;

    let mut cycles = Vec::new();
    if configuration.mode == Mode::OrderCycle {
        for iteration in 0..configuration.iterations {
            cycles.push(
                order_cycle(
                    &gateway,
                    &configuration,
                    &market.mark_price,
                    &rules,
                    iteration,
                )
                .await?,
            );
        }
    }

    Ok(json!({
        "ok": true,
        "environment": "testnet",
        "exchange": exchange_name(configuration.exchange),
        "symbol": configuration.symbol,
        "mode": match configuration.mode {
            Mode::ReadOnly => "read_only",
            Mode::OrderCycle => "order_cycle",
        },
        "snapshot_elapsed_ms": elapsed_ms(snapshot_started),
        "market": market,
        "instrument_rules": rules,
        "account": balance,
        "position": position,
        "fee_rates": fees,
        "open_order_count_before_test": open_orders.len(),
        "order_cycles": cycles,
    }))
}

async fn order_cycle(
    gateway: &grid_trading_server::exchange::configured::SharedConfiguredExchangeGateway,
    configuration: &Configuration,
    mark_price: &Decimal,
    rules: &InstrumentRules,
    iteration: usize,
) -> Result<Value> {
    let initial_position = gateway
        .position_snapshot(configuration.exchange, &configuration.symbol)
        .await
        .context("pre-order testnet position snapshot failed")?;
    let price = safe_test_price(*mark_price, rules)?;
    let quantity = safe_test_quantity(price, rules, configuration.explicit_quantity)?;
    let now_ms = unix_time_ms()?;
    let client_order_id = test_client_order_id(now_ms, iteration)?;
    let intent = OrderIntent::prepare(
        client_order_id.clone(),
        configuration.exchange,
        OrderShape {
            symbol: configuration.symbol.clone(),
            side: OrderSide::Buy,
            price: Some(price),
            quantity,
            reduce_only: false,
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::PostOnly,
        },
        now_ms,
    )?;

    let placement_started = Instant::now();
    let acknowledgement = gateway
        .place_order(&intent)
        .await
        .context("testnet order placement failed")?;
    let placement_ms = elapsed_ms(placement_started);
    if acknowledgement.client_order_id != client_order_id {
        bail!("testnet placement acknowledged a different client order ID");
    }

    let active_confirmation_started = Instant::now();
    let active_result = confirm_active(
        gateway,
        configuration.exchange,
        &configuration.symbol,
        &client_order_id,
        &acknowledgement.exchange_order_id,
    )
    .await;
    let active_confirmation_ms = elapsed_ms(active_confirmation_started);

    let cancellation_started = Instant::now();
    let cancellation_result = gateway
        .cancel_order(
            configuration.exchange,
            &configuration.symbol,
            &client_order_id,
            &acknowledgement.exchange_order_id,
        )
        .await;
    let cancellation_request_ms = elapsed_ms(cancellation_started);

    let terminal_confirmation_started = Instant::now();
    let terminal_result = confirm_cancelled(
        gateway,
        configuration.exchange,
        &configuration.symbol,
        &client_order_id,
        &acknowledgement.exchange_order_id,
    )
    .await;
    let terminal_confirmation_ms = elapsed_ms(terminal_confirmation_started);

    active_result.context("testnet order was not confirmed active before cleanup")?;
    cancellation_result.context("testnet exact cancellation request failed")?;
    terminal_result.context("testnet cancellation was not confirmed terminal")?;

    let remaining = gateway
        .open_orders_snapshot(configuration.exchange, &configuration.symbol)
        .await
        .context("post-cancel testnet open-order snapshot failed")?;
    if remaining
        .iter()
        .any(|order| order.client_order_id == client_order_id)
    {
        bail!("cancelled testnet order is still present in the open-order snapshot");
    }
    let final_position = gateway
        .position_snapshot(configuration.exchange, &configuration.symbol)
        .await
        .context("post-order testnet position snapshot failed")?;
    if position_exposure(&final_position) != position_exposure(&initial_position) {
        bail!("testnet order cycle changed the account position");
    }

    Ok(json!({
        "iteration": iteration + 1,
        "client_order_id": client_order_id,
        "exchange_order_id": acknowledgement.exchange_order_id,
        "side": "Buy",
        "price": price,
        "quantity": quantity,
        "time_in_force": "PostOnly",
        "placement_ms": placement_ms,
        "active_confirmation_ms": active_confirmation_ms,
        "cancellation_request_ms": cancellation_request_ms,
        "terminal_confirmation_ms": terminal_confirmation_ms,
        "total_ms": placement_ms
            + active_confirmation_ms
            + cancellation_request_ms
            + terminal_confirmation_ms,
    }))
}

async fn confirm_active(
    gateway: &grid_trading_server::exchange::configured::SharedConfiguredExchangeGateway,
    exchange: Exchange,
    symbol: &str,
    client_order_id: &ClientOrderId,
    exchange_order_id: &str,
) -> Result<()> {
    for _ in 0..LOOKUP_ATTEMPTS {
        match gateway
            .lookup_order_by_client_id(exchange, symbol, client_order_id)
            .await
        {
            Ok(OrderLookup::Found(order))
                if order.exchange_order_id == exchange_order_id
                    && matches!(order.lifecycle, OrderLifecycle::Active(_)) =>
            {
                return Ok(());
            }
            Ok(OrderLookup::Found(order))
                if order.exchange_order_id == exchange_order_id
                    && matches!(order.lifecycle, OrderLifecycle::Terminal(_)) =>
            {
                bail!("testnet order became terminal before cancellation");
            }
            Ok(_) | Err(_) => tokio::time::sleep(LOOKUP_DELAY).await,
        }
    }
    bail!("testnet order did not become active within the confirmation window")
}

async fn confirm_cancelled(
    gateway: &grid_trading_server::exchange::configured::SharedConfiguredExchangeGateway,
    exchange: Exchange,
    symbol: &str,
    client_order_id: &ClientOrderId,
    exchange_order_id: &str,
) -> Result<()> {
    for _ in 0..LOOKUP_ATTEMPTS {
        match gateway
            .lookup_order_by_client_id(exchange, symbol, client_order_id)
            .await
        {
            Ok(OrderLookup::Found(order))
                if order.exchange_order_id == exchange_order_id
                    && order.lifecycle
                        == OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled) =>
            {
                return Ok(());
            }
            Ok(OrderLookup::Found(order))
                if order.exchange_order_id == exchange_order_id
                    && matches!(order.lifecycle, OrderLifecycle::Terminal(_)) =>
            {
                bail!("testnet order reached a terminal status other than cancelled");
            }
            Ok(_) | Err(_) => tokio::time::sleep(LOOKUP_DELAY).await,
        }
    }
    bail!("testnet order cancellation was not confirmed within the confirmation window")
}

fn credentials(exchange: Exchange) -> Result<ExchangeCredentials> {
    match exchange {
        Exchange::Binance => ExchangeCredentials::binance(
            required("BINANCE_TESTNET_API_KEY")?,
            required("BINANCE_TESTNET_API_SECRET")?,
        ),
        Exchange::Bybit => ExchangeCredentials::bybit(
            required("BYBIT_TESTNET_API_KEY")?,
            required("BYBIT_TESTNET_API_SECRET")?,
        ),
        Exchange::Aster => {
            ExchangeCredentials::aster(required("ASTER_TESTNET_SIGNER_PRIVATE_KEY")?)
        }
        Exchange::TradeXyz => ExchangeCredentials::trade_xyz(
            required("TRADE_XYZ_TESTNET_ACCOUNT_ADDRESS")?,
            required("TRADE_XYZ_TESTNET_AGENT_PRIVATE_KEY")?,
        ),
    }
    .context("invalid dedicated testnet credentials")
}

fn safe_test_price(mark_price: Decimal, rules: &InstrumentRules) -> Result<Decimal> {
    if mark_price <= Decimal::ZERO {
        bail!("testnet mark price must be positive");
    }
    let unrounded = mark_price
        .checked_mul(Decimal::new(99, 2))
        .ok_or_else(|| anyhow!("testnet limit price overflowed"))?;
    let price = rules
        .floor_price(unrounded)
        .ok_or_else(|| anyhow!("testnet limit price could not be aligned"))?;
    if price <= Decimal::ZERO || price >= mark_price {
        bail!("testnet limit price is not safely below the mark price");
    }
    Ok(price)
}

fn safe_test_quantity(
    price: Decimal,
    rules: &InstrumentRules,
    explicit: Option<Decimal>,
) -> Result<Decimal> {
    let quantity = if let Some(quantity) = explicit {
        quantity
    } else {
        let minimum_for_notional = if rules.min_notional > Decimal::ZERO {
            rules
                .min_notional
                .checked_div(price)
                .ok_or_else(|| anyhow!("testnet minimum quantity overflowed"))?
        } else {
            Decimal::ZERO
        };
        ceil_to_step(
            minimum_for_notional.max(rules.limit_quantity.min),
            rules.limit_quantity.step,
        )?
    };
    if !rules.limit_quantity.accepts(quantity) {
        bail!("testnet order quantity does not satisfy the exchange limit-order rules");
    }
    if quantity
        .checked_mul(price)
        .is_none_or(|notional| notional < rules.min_notional)
    {
        bail!("testnet order quantity is below the exchange minimum notional");
    }
    Ok(quantity)
}

fn ceil_to_step(value: Decimal, step: Decimal) -> Result<Decimal> {
    value
        .checked_div(step)
        .map(|steps| steps.ceil())
        .and_then(|steps| steps.checked_mul(step))
        .ok_or_else(|| anyhow!("testnet quantity alignment overflowed"))
}

fn test_client_order_id(now_ms: u64, iteration: usize) -> Result<ClientOrderId> {
    let run = now_ms & 0xffff_ffff_ffff;
    ClientOrderId::parse(format!("c_{run:012x}_B_{}", iteration + 1))
        .context("testnet client order ID is invalid")
}

fn parse_exchange(value: &str) -> Result<Exchange> {
    match value.trim().to_ascii_lowercase().as_str() {
        "binance" => Ok(Exchange::Binance),
        "bybit" => Ok(Exchange::Bybit),
        "aster" | "asterdex" => Ok(Exchange::Aster),
        "trade_xyz" | "trade.xyz" | "hyperliquid" => Ok(Exchange::TradeXyz),
        _ => bail!("GRID_TESTNET_EXCHANGE is unsupported"),
    }
}

fn exchange_name(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Binance => "binance",
        Exchange::Aster => "aster",
        Exchange::Bybit => "bybit",
        Exchange::TradeXyz => "trade_xyz",
    }
}

fn required(name: &'static str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.trim().is_empty() {
        bail!("{name} is required");
    }
    if value.contains(['\r', '\n', '\0']) {
        bail!("{name} contains forbidden control characters");
    }
    Ok(value)
}

fn unix_time_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis()
        .try_into()
        .context("system timestamp does not fit u64")
}

fn elapsed_ms(started: Instant) -> u128 {
    started.elapsed().as_millis()
}

fn position_exposure(
    snapshot: &PositionSnapshot,
) -> Vec<(
    grid_trading_server::exchange::PositionSide,
    Decimal,
    Option<Decimal>,
)> {
    snapshot
        .legs
        .iter()
        .map(|leg| (leg.side, leg.signed_quantity, leg.entry_price))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use grid_trading_server::domain::QuantityRules;

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(1, 1),
            max_price_significant_digits: None,
            limit_quantity: QuantityRules {
                step: Decimal::new(1, 3),
                min: Decimal::new(1, 3),
                max: Some(Decimal::new(100, 0)),
            },
            market_quantity: QuantityRules {
                step: Decimal::new(1, 3),
                min: Decimal::new(1, 3),
                max: Some(Decimal::new(100, 0)),
            },
            min_notional: Decimal::new(10, 0),
        }
    }

    #[test]
    fn generated_order_is_below_mark_and_aligned() {
        let rules = rules();
        let price = safe_test_price(Decimal::new(1000, 0), &rules).unwrap();
        let quantity = safe_test_quantity(price, &rules, None).unwrap();

        assert_eq!(price, Decimal::new(990, 0));
        assert!(rules.limit_quantity.accepts(quantity));
        assert!(price * quantity >= rules.min_notional);
    }

    #[test]
    fn explicit_invalid_quantity_is_rejected() {
        let error = safe_test_quantity(Decimal::new(990, 0), &rules(), Some(Decimal::new(15, 4)))
            .unwrap_err();

        assert!(error.to_string().contains("limit-order rules"));
    }

    #[test]
    fn generated_identity_is_hyperliquid_compatible() {
        let identity = test_client_order_id(1_700_000_000_123, 0).unwrap();

        assert_eq!(identity.as_str(), "c_018bcfe5687b_B_1");
    }
}
