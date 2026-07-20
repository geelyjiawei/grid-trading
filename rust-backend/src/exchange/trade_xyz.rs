use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    domain::{
        ClientOrderId, Exchange, InstrumentRules, OrderIntent, OrderKind, OrderShape, OrderSide,
        QuantityRules, TerminalOrderStatus, TimeInForce,
    },
    exchange::{
        AccountBalanceSnapshot, AccountBalanceSnapshotGateway, AccountBalanceUnit,
        ActiveOrderStatus, AuthoritativeOrder, CancellationAcknowledgement, CancellationError,
        ExchangeIdentityGateway, ExchangeMarketSnapshot, ExecutionSnapshotError,
        ExecutionSnapshotGateway, HistoricalMinutePrice, HistoricalOrder, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageAcknowledgement, LeverageError, LeverageGateway,
        LookupError, MarketSnapshotGateway, OpenOrderExecutionProgress, OpenOrderSnapshotGateway,
        OrderCancellationGateway, OrderExecutionSnapshot, OrderHistorySnapshotGateway,
        OrderLifecycle, OrderLookup, OrderLookupGateway, OrderPlacementGateway,
        PlacementAcknowledgement, PlacementError, PositionLeg, PositionSide, PositionSnapshot,
        PositionSnapshotGateway, SnapshotError, TradeFill, TradingFeeRateGateway, TradingFeeRates,
        compare_trade_chronology,
        protocol::{
            HttpMethod, HttpResponse, HttpTransport, NonceSource, PreparedHttpRequest,
            TransportError,
        },
        trade_xyz_codec::{
            CancelAction, HyperliquidSignature, HyperliquidSigner, OrderAction, TradeXyzCodecError,
            UpdateLeverageAction, WireOrder, WireOrderType, decode_cloid, effective_price_tick,
            encode_cloid, exchange_coin, local_symbol, maximum_decimal_price_tick,
            normalize_address, quantity_step, valid_price, wire_decimal,
        },
    },
};

const PRODUCTION_BASE_URL: &str = "https://api.hyperliquid.xyz";
const TESTNET_BASE_URL: &str = "https://api.hyperliquid-testnet.xyz";
const DEX_NAME: &str = "xyz";
const MARKET_CACHE_TTL: Duration = Duration::from_millis(750);
const LEVERAGE_CACHE_TTL: Duration = Duration::from_secs(30);
const EXECUTION_FILL_LOOKBACK_MS: u64 = 60_000;

fn minimum_notional() -> Decimal {
    Decimal::new(10, 0)
}

fn market_slippage() -> Decimal {
    Decimal::new(5, 2)
}

#[derive(Debug, Error)]
pub enum TradeXyzAdapterError {
    #[error(transparent)]
    Codec(#[from] TradeXyzCodecError),
}

#[derive(Debug, Clone)]
struct DexMetadata {
    index: u32,
    fee_scale: Decimal,
}

#[derive(Debug, Clone)]
struct MarketInfo {
    coin: String,
    asset_id: u32,
    size_decimals: u32,
    max_leverage: u16,
    delisted: bool,
    growth_mode: bool,
    mark_price: Decimal,
    mid_price: Decimal,
    price_24h_change_ratio: Option<Decimal>,
    volume_24h: Option<Decimal>,
}

#[derive(Debug, Clone, Default)]
struct CachedExecutionFills {
    rows: Vec<Value>,
}

#[derive(Debug, Clone)]
struct CachedOpenExecution {
    order: AuthoritativeOrder,
    order_time_ms: u64,
}

pub struct TradeXyzAdapter<T, N> {
    transport: T,
    signer: HyperliquidSigner,
    nonce: N,
    account_address: String,
    base_url: String,
    mainnet: bool,
    dex_metadata: Arc<tokio::sync::OnceCell<DexMetadata>>,
    market_cache: Arc<tokio::sync::Mutex<HashMap<String, (Instant, MarketInfo)>>>,
    leverage_cache: Arc<tokio::sync::Mutex<HashMap<String, (Instant, u16)>>>,
    open_execution_cache:
        Arc<tokio::sync::Mutex<HashMap<String, BTreeMap<ClientOrderId, CachedOpenExecution>>>>,
    execution_fill_cache: Arc<tokio::sync::Mutex<CachedExecutionFills>>,
    credentials_verified: Arc<tokio::sync::OnceCell<()>>,
}

impl<T, N> std::fmt::Debug for TradeXyzAdapter<T, N> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TradeXyzAdapter")
            .field("account_address", &self.account_address)
            .field("base_url", &self.base_url)
            .field("credential_material", &"[REDACTED]")
            .finish()
    }
}

impl<T, N> TradeXyzAdapter<T, N> {
    pub fn production_wallet(
        transport: T,
        nonce: N,
        account_address: &str,
        agent_private_key: &str,
    ) -> Result<Self, TradeXyzAdapterError> {
        Self::new(
            transport,
            nonce,
            account_address,
            agent_private_key,
            PRODUCTION_BASE_URL,
            true,
        )
    }

    pub fn testnet_wallet(
        transport: T,
        nonce: N,
        account_address: &str,
        agent_private_key: &str,
    ) -> Result<Self, TradeXyzAdapterError> {
        Self::new(
            transport,
            nonce,
            account_address,
            agent_private_key,
            TESTNET_BASE_URL,
            false,
        )
    }

    fn new(
        transport: T,
        nonce: N,
        account_address: &str,
        agent_private_key: &str,
        base_url: &str,
        mainnet: bool,
    ) -> Result<Self, TradeXyzAdapterError> {
        Ok(Self {
            transport,
            signer: HyperliquidSigner::from_private_key(agent_private_key)?,
            nonce,
            account_address: normalize_address(account_address)?,
            base_url: base_url.to_owned(),
            mainnet,
            dex_metadata: Arc::new(tokio::sync::OnceCell::new()),
            market_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            leverage_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            open_execution_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            execution_fill_cache: Arc::new(
                tokio::sync::Mutex::new(CachedExecutionFills::default()),
            ),
            credentials_verified: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    pub fn account_address(&self) -> &str {
        &self.account_address
    }

    pub fn agent_address(&self) -> &str {
        self.signer.address()
    }
}

impl<T, N> ExchangeIdentityGateway for TradeXyzAdapter<T, N>
where
    T: Send + Sync,
    N: Send + Sync,
{
    fn exchange(&self) -> Exchange {
        Exchange::TradeXyz
    }
}

impl<T, N> TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    fn request(&self, path: &str, body: Value) -> Result<PreparedHttpRequest, SnapshotError> {
        let raw_body = serde_json::to_string(&body)
            .map_err(|_| SnapshotError::new("TRADE.XYZ request serialization failed"))?;
        Ok(PreparedHttpRequest {
            method: HttpMethod::Post,
            base_url: self.base_url.clone(),
            path: path.to_owned(),
            query: Vec::new(),
            body: Vec::new(),
            raw_body: Some(raw_body),
            headers: vec![("Content-Type".into(), "application/json".into())],
        })
    }

    async fn post_info(&self, body: Value, context: &str) -> Result<Value, SnapshotError> {
        let request = self.request("/info", body)?;
        let response = self
            .transport
            .execute(request)
            .await
            .map_err(|error| SnapshotError::new(format!("{context}: {error}")))?;
        parse_success_json(response, context)
    }

    async fn post_action<A: Serialize>(&self, action: &A) -> Result<HttpResponse, TransportError> {
        let nonce = self.nonce.next_nonce();
        let signature = self
            .signer
            .sign_action(action, nonce, self.mainnet)
            .map_err(|_| TransportError::Other("Hyperliquid signing failed".into()))?;
        let action = serde_json::to_value(action)
            .map_err(|_| TransportError::Other("Hyperliquid action serialization failed".into()))?;
        let body = action_payload(action, signature, nonce);
        let request = self
            .request("/exchange", body)
            .map_err(|error| TransportError::Other(error.message))?;
        self.transport.execute(request).await
    }

    async fn verify_credentials(&self) -> Result<(), SnapshotError> {
        self.credentials_verified
            .get_or_try_init(|| async {
                if self
                    .signer
                    .address()
                    .eq_ignore_ascii_case(&self.account_address)
                {
                    return Ok(());
                }
                let role = self
                    .post_info(
                        json!({"type": "userRole", "user": self.signer.address()}),
                        "TRADE.XYZ agent role lookup failed",
                    )
                    .await?;
                let role_name = text_field(&role, "role")?;
                let owner = role
                    .get("data")
                    .and_then(|data| data.get("user"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if role_name != "agent" || !owner.eq_ignore_ascii_case(&self.account_address) {
                    return Err(SnapshotError::new(
                        "TRADE.XYZ agent wallet is not authorized for the configured account",
                    ));
                }
                Ok(())
            })
            .await
            .copied()
    }

    async fn dex_metadata(&self) -> Result<DexMetadata, SnapshotError> {
        self.dex_metadata
            .get_or_try_init(|| async {
                let value = self
                    .post_info(
                        json!({"type": "perpDexs"}),
                        "TRADE.XYZ DEX metadata lookup failed",
                    )
                    .await?;
                let entries = value
                    .as_array()
                    .ok_or_else(|| SnapshotError::new("TRADE.XYZ DEX metadata is invalid"))?;
                let includes_default_slot = entries.first().is_some_and(Value::is_null);
                for (position, entry) in entries.iter().enumerate() {
                    if entry.get("name").and_then(Value::as_str) == Some(DEX_NAME) {
                        let index = if includes_default_slot {
                            position
                        } else {
                            position.saturating_add(1)
                        };
                        let index = u32::try_from(index).map_err(|_| {
                            SnapshotError::new("TRADE.XYZ DEX index is out of range")
                        })?;
                        let fee_scale =
                            optional_decimal(entry, "deployerFeeScale")?.unwrap_or(Decimal::ONE);
                        if fee_scale < Decimal::ZERO || fee_scale > Decimal::from(3) {
                            return Err(SnapshotError::new("TRADE.XYZ DEX fee scale is invalid"));
                        }
                        return Ok(DexMetadata { index, fee_scale });
                    }
                }
                Err(SnapshotError::new("TRADE.XYZ DEX is unavailable"))
            })
            .await
            .cloned()
    }

    async fn market_info(&self, symbol: &str) -> Result<MarketInfo, SnapshotError> {
        let symbol = symbol.to_ascii_uppercase();
        let mut cache = self.market_cache.lock().await;
        let now = Instant::now();
        if let Some((cached_at, market)) = cache.get(&symbol)
            && now.saturating_duration_since(*cached_at) < MARKET_CACHE_TTL
        {
            return Ok(market.clone());
        }
        let market = self.fetch_market_info(&symbol).await?;
        cache.insert(symbol, (Instant::now(), market.clone()));
        Ok(market)
    }

    async fn fresh_market_info(&self, symbol: &str) -> Result<MarketInfo, SnapshotError> {
        let symbol = symbol.to_ascii_uppercase();
        let mut cache = self.market_cache.lock().await;
        let market = self.fetch_market_info(&symbol).await?;
        cache.insert(symbol, (Instant::now(), market.clone()));
        Ok(market)
    }

    async fn fetch_market_info(&self, symbol: &str) -> Result<MarketInfo, SnapshotError> {
        let coin = exchange_coin(&symbol.to_ascii_uppercase())
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let value = self
            .post_info(
                json!({"type": "metaAndAssetCtxs", "dex": DEX_NAME}),
                "TRADE.XYZ market metadata lookup failed",
            )
            .await?;
        let pair = value
            .as_array()
            .filter(|items| items.len() == 2)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ market metadata is invalid"))?;
        let universe = pair[0]
            .get("universe")
            .and_then(Value::as_array)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ market universe is invalid"))?;
        let contexts = pair[1]
            .as_array()
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ market contexts are invalid"))?;
        if universe.len() != contexts.len() {
            return Err(SnapshotError::new(
                "TRADE.XYZ market metadata changed during collection",
            ));
        }
        let index = universe
            .iter()
            .position(|item| item.get("name").and_then(Value::as_str) == Some(coin.as_str()))
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ symbol is not listed"))?;
        let metadata = &universe[index];
        let context = &contexts[index];
        let size_decimals = u32_field(metadata, "szDecimals")?;
        if size_decimals > 6 {
            return Err(SnapshotError::new(
                "TRADE.XYZ size precision exceeds the supported contract",
            ));
        }
        let max_leverage = u16::try_from(u32_field(metadata, "maxLeverage")?)
            .map_err(|_| SnapshotError::new("TRADE.XYZ maximum leverage is invalid"))?;
        let mark_price = decimal_field(context, "markPx")?;
        let mid_price = optional_decimal(context, "midPx")?.unwrap_or(mark_price);
        if mark_price <= Decimal::ZERO || mid_price <= Decimal::ZERO || max_leverage == 0 {
            return Err(SnapshotError::new("TRADE.XYZ market values are invalid"));
        }
        let price_24h_change_ratio = match optional_decimal(context, "prevDayPx")? {
            Some(previous_day_price) if previous_day_price > Decimal::ZERO => Some(
                mid_price
                    .checked_sub(previous_day_price)
                    .and_then(|change| change.checked_div(previous_day_price))
                    .ok_or_else(|| SnapshotError::new("TRADE.XYZ 24H price change overflowed"))?,
            ),
            Some(_) => {
                return Err(SnapshotError::new(
                    "TRADE.XYZ previous-day price is invalid",
                ));
            }
            None => None,
        };
        let volume_24h = optional_decimal(context, "dayBaseVlm")?;
        if volume_24h.is_some_and(|volume| volume < Decimal::ZERO) {
            return Err(SnapshotError::new("TRADE.XYZ 24H volume is invalid"));
        }
        let dex = self.dex_metadata().await?;
        let index = u32::try_from(index)
            .map_err(|_| SnapshotError::new("TRADE.XYZ market index is out of range"))?;
        let asset_id = 100_000_u32
            .checked_add(
                dex.index
                    .checked_mul(10_000)
                    .ok_or_else(|| SnapshotError::new("TRADE.XYZ asset identity overflowed"))?,
            )
            .and_then(|base| base.checked_add(index))
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ asset identity overflowed"))?;
        Ok(MarketInfo {
            coin,
            asset_id,
            size_decimals,
            max_leverage,
            delisted: metadata
                .get("isDelisted")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            growth_mode: metadata.get("growthMode").and_then(Value::as_str) == Some("enabled"),
            mark_price,
            mid_price,
            price_24h_change_ratio,
            volume_24h,
        })
    }

    async fn order_status_value(&self, cloid: &str) -> Result<Value, SnapshotError> {
        self.post_info(
            json!({
                "type": "orderStatus",
                "user": self.account_address,
                "oid": cloid,
            }),
            "TRADE.XYZ order status lookup failed",
        )
        .await
    }

    async fn frontend_open_orders(&self) -> Result<Vec<Value>, SnapshotError> {
        let value = self
            .post_info(
                json!({
                    "type": "frontendOpenOrders",
                    "user": self.account_address,
                    "dex": DEX_NAME,
                }),
                "TRADE.XYZ open-order lookup failed",
            )
            .await?;
        value
            .as_array()
            .cloned()
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ open-order response is invalid"))
    }

    async fn active_asset_leverage(&self, coin: &str) -> Result<u16, SnapshotError> {
        let mut cache = self.leverage_cache.lock().await;
        let now = Instant::now();
        if let Some((cached_at, leverage)) = cache.get(coin)
            && now.saturating_duration_since(*cached_at) < LEVERAGE_CACHE_TTL
        {
            return Ok(*leverage);
        }
        let value = self
            .post_info(
                json!({
                    "type": "activeAssetData",
                    "user": self.account_address,
                    "coin": coin,
                }),
                "TRADE.XYZ active-asset lookup failed",
            )
            .await?;
        let leverage = value
            .get("leverage")
            .and_then(|leverage| leverage.get("value"))
            .and_then(value_u64)
            .and_then(|value| u16::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ active leverage is invalid"))?;
        cache.insert(coin.to_owned(), (Instant::now(), leverage));
        Ok(leverage)
    }
}

fn action_payload(action: Value, signature: HyperliquidSignature, nonce: u64) -> Value {
    json!({
        "action": action,
        "nonce": nonce,
        "signature": signature,
        "vaultAddress": null,
    })
}

#[async_trait]
impl<T, N> MarketSnapshotGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
        validate_request(exchange, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let market = self.market_info(&symbol).await?;
        Ok(ExchangeMarketSnapshot {
            exchange: Exchange::TradeXyz,
            symbol,
            last_price: market.mid_price,
            mark_price: market.mark_price,
            price_24h_change_ratio: market.price_24h_change_ratio,
            volume_24h: market.volume_24h,
            observed_at_ms: now_ms()?,
        })
    }
}

#[async_trait]
impl<T, N> InstrumentRulesGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<InstrumentRules, SnapshotError> {
        validate_request(exchange, symbol)?;
        let market = self.market_info(symbol).await?;
        if market.delisted {
            return Err(SnapshotError::new("TRADE.XYZ symbol is delisted"));
        }
        let step = quantity_step(market.size_decimals)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ quantity precision is invalid"))?;
        let tick_size = maximum_decimal_price_tick(market.size_decimals)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ price precision is invalid"))?;
        let rules = InstrumentRules {
            tick_size,
            max_price_significant_digits: Some(5),
            limit_quantity: QuantityRules {
                step,
                min: step,
                max: None,
            },
            market_quantity: QuantityRules {
                step,
                min: step,
                max: None,
            },
            min_notional: minimum_notional(),
        };
        rules
            .validate()
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        Ok(rules)
    }
}

#[async_trait]
impl<T, N> PositionSnapshotGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError> {
        validate_request(exchange, symbol)?;
        self.verify_credentials().await?;
        let symbol = symbol.to_ascii_uppercase();
        let coin = exchange_coin(&symbol).map_err(|error| SnapshotError::new(error.to_string()))?;
        let state = self
            .post_info(
                json!({
                    "type": "clearinghouseState",
                    "user": self.account_address,
                    "dex": DEX_NAME,
                }),
                "TRADE.XYZ position lookup failed",
            )
            .await?;
        let market = self.market_info(&symbol).await?;
        let active_leverage = self.active_asset_leverage(&coin).await?;
        let position = state
            .get("assetPositions")
            .and_then(Value::as_array)
            .and_then(|positions| {
                positions.iter().find_map(|item| {
                    let position = item.get("position")?;
                    (position.get("coin")?.as_str()? == coin).then_some(position)
                })
            });
        let leg = if let Some(position) = position {
            let signed_quantity = decimal_field(position, "szi")?;
            let entry_price =
                optional_decimal(position, "entryPx")?.filter(|price| *price > Decimal::ZERO);
            PositionLeg {
                side: PositionSide::Both,
                signed_quantity,
                entry_price,
                mark_price: market.mark_price,
                unrealized_profit: optional_decimal(position, "unrealizedPnl")?
                    .unwrap_or(Decimal::ZERO),
                leverage: Some(active_leverage),
            }
        } else {
            PositionLeg {
                side: PositionSide::Both,
                signed_quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: market.mark_price,
                unrealized_profit: Decimal::ZERO,
                leverage: Some(active_leverage),
            }
        };
        Ok(PositionSnapshot {
            exchange: Exchange::TradeXyz,
            symbol,
            legs: vec![leg],
        })
    }
}

#[async_trait]
impl<T, N> AccountBalanceSnapshotGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError> {
        if exchange != Exchange::TradeXyz {
            return Err(SnapshotError::new("TRADE.XYZ exchange identity mismatch"));
        }
        self.verify_credentials().await?;
        let abstraction = self
            .post_info(
                json!({
                    "type": "userAbstraction",
                    "user": self.account_address,
                }),
                "TRADE.XYZ account-mode lookup failed",
            )
            .await?;
        let mode = abstraction
            .as_str()
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ account mode is invalid"))?;
        let perp_state = self
            .post_info(
                json!({
                    "type": "clearinghouseState",
                    "user": self.account_address,
                    "dex": DEX_NAME,
                }),
                "TRADE.XYZ account lookup failed",
            )
            .await?;
        let snapshot = match mode {
            "unifiedAccount" => {
                let spot_state = self
                    .post_info(
                        json!({
                            "type": "spotClearinghouseState",
                            "user": self.account_address,
                        }),
                        "TRADE.XYZ unified balance lookup failed",
                    )
                    .await?;
                parse_unified_account_balance(&perp_state, &spot_state)?
            }
            "default" | "disabled" | "dexAbstraction" => {
                parse_standard_account_balance(&perp_state)?
            }
            "portfolioMargin" => {
                return Err(SnapshotError::new(
                    "TRADE.XYZ portfolio-margin accounts are not supported safely; use Standard or Unified account mode",
                ));
            }
            _ => return Err(SnapshotError::new("TRADE.XYZ account mode is unsupported")),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

fn parse_standard_account_balance(state: &Value) -> Result<AccountBalanceSnapshot, SnapshotError> {
    let summary = state
        .get("marginSummary")
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ margin summary is unavailable"))?;
    let equity = decimal_field(summary, "accountValue")?;
    let unrealized_profit = account_unrealized_profit(state)?;
    Ok(AccountBalanceSnapshot {
        exchange: Exchange::TradeXyz,
        unit: AccountBalanceUnit::Usdc,
        available_balance: decimal_field(state, "withdrawable")?,
        wallet_balance: equity
            .checked_sub(unrealized_profit)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ wallet balance overflowed"))?,
        equity,
        unrealized_profit,
    })
}

fn parse_unified_account_balance(
    perp_state: &Value,
    spot_state: &Value,
) -> Result<AccountBalanceSnapshot, SnapshotError> {
    let balances = spot_state
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ unified balances are unavailable"))?;
    let mut usdc = balances
        .iter()
        .filter(|balance| balance.get("coin").and_then(Value::as_str) == Some("USDC"));
    let balance = usdc
        .next()
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ unified USDC balance is unavailable"))?;
    if usdc.next().is_some() {
        return Err(SnapshotError::new(
            "TRADE.XYZ unified USDC balance is ambiguous",
        ));
    }
    let wallet_balance = decimal_field(balance, "total")?;
    let held = decimal_field(balance, "hold")?;
    let available_balance = wallet_balance
        .checked_sub(held)
        .filter(|available| *available >= Decimal::ZERO)
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ unified available balance is invalid"))?;
    let unrealized_profit = account_unrealized_profit(perp_state)?;
    let equity = wallet_balance
        .checked_add(unrealized_profit)
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ unified equity overflowed"))?;
    Ok(AccountBalanceSnapshot {
        exchange: Exchange::TradeXyz,
        unit: AccountBalanceUnit::Usdc,
        available_balance,
        wallet_balance,
        equity,
        unrealized_profit,
    })
}

fn account_unrealized_profit(state: &Value) -> Result<Decimal, SnapshotError> {
    state
        .get("assetPositions")
        .and_then(Value::as_array)
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ positions are unavailable"))?
        .iter()
        .try_fold(Decimal::ZERO, |total, item| {
            let value = item
                .get("position")
                .map(|position| optional_decimal(position, "unrealizedPnl"))
                .transpose()?
                .flatten()
                .unwrap_or(Decimal::ZERO);
            total
                .checked_add(value)
                .ok_or_else(|| SnapshotError::new("TRADE.XYZ account value overflowed"))
        })
}

#[async_trait]
impl<T, N> TradingFeeRateGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError> {
        validate_request(exchange, symbol)?;
        self.verify_credentials().await?;
        let market = self.market_info(symbol).await?;
        let fees = self
            .post_info(
                json!({"type": "userFees", "user": self.account_address}),
                "TRADE.XYZ fee lookup failed",
            )
            .await?;
        let base_maker = decimal_field(&fees, "userAddRate")?;
        let base_taker = decimal_field(&fees, "userCrossRate")?;
        let referral_discount =
            optional_decimal(&fees, "activeReferralDiscount")?.unwrap_or(Decimal::ZERO);
        if !(Decimal::ZERO..Decimal::ONE).contains(&referral_discount) {
            return Err(SnapshotError::new("TRADE.XYZ referral discount is invalid"));
        }
        let dex = self.dex_metadata().await?;
        let hip3_scale = if dex.fee_scale < Decimal::ONE {
            Decimal::ONE + dex.fee_scale
        } else {
            dex.fee_scale * Decimal::from(2)
        };
        let growth_scale = if market.growth_mode {
            Decimal::new(1, 1)
        } else {
            Decimal::ONE
        };
        let discount_scale = Decimal::ONE - referral_discount;
        let maker_rate = if base_maker > Decimal::ZERO {
            base_maker * hip3_scale * growth_scale * discount_scale
        } else {
            base_maker * growth_scale
        };
        let rates = TradingFeeRates {
            exchange: Exchange::TradeXyz,
            symbol: symbol.to_ascii_uppercase(),
            maker_rate,
            taker_rate: base_taker * hip3_scale * growth_scale * discount_scale,
        };
        rates.validate()?;
        Ok(rates)
    }
}

#[async_trait]
impl<T, N> LeverageGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError> {
        validate_request(exchange, symbol).map_err(|error| LeverageError::Invalid {
            message: error.message,
        })?;
        let market = self
            .market_info(symbol)
            .await
            .map_err(|error| LeverageError::Invalid {
                message: error.message,
            })?;
        if leverage == 0 || leverage > market.max_leverage {
            return Err(LeverageError::Invalid {
                message: format!(
                    "TRADE.XYZ leverage must be between 1 and {} for {}",
                    market.max_leverage, market.coin
                ),
            });
        }
        let response = self
            .post_action(&UpdateLeverageAction::isolated(market.asset_id, leverage))
            .await
            .map_err(|error| LeverageError::Unknown {
                message: error.to_string(),
            })?;
        let value = parse_write_response(response, "TRADE.XYZ leverage update")
            .map_err(write_to_leverage_error)?;
        ensure_action_ok(&value, "TRADE.XYZ leverage update").map_err(write_to_leverage_error)?;
        self.leverage_cache
            .lock()
            .await
            .insert(market.coin, (Instant::now(), leverage));
        Ok(LeverageAcknowledgement {
            exchange: Exchange::TradeXyz,
            symbol: symbol.to_ascii_uppercase(),
            leverage,
        })
    }
}

#[async_trait]
impl<T, N> OrderPlacementGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        if intent.exchange != Exchange::TradeXyz {
            return Err(PlacementError::NotSubmitted {
                message: "TRADE.XYZ exchange identity mismatch".into(),
            });
        }
        intent
            .validate()
            .map_err(|error| PlacementError::NotSubmitted {
                message: error.to_string(),
            })?;
        let market = if intent.shape.kind == OrderKind::Market {
            self.fresh_market_info(&intent.shape.symbol).await
        } else {
            self.market_info(&intent.shape.symbol).await
        }
        .map_err(|error| PlacementError::NotSubmitted {
            message: error.message,
        })?;
        if market.delisted {
            return Err(PlacementError::Definitive {
                code: Some("DELISTED".into()),
                message: "TRADE.XYZ symbol is delisted".into(),
            });
        }
        let step =
            quantity_step(market.size_decimals).ok_or_else(|| PlacementError::NotSubmitted {
                message: "TRADE.XYZ quantity precision is invalid".into(),
            })?;
        if intent.shape.quantity < step
            || intent
                .shape
                .quantity
                .checked_div(step)
                .is_none_or(|steps| !steps.fract().is_zero())
        {
            return Err(PlacementError::NotSubmitted {
                message: "TRADE.XYZ order quantity is not aligned to szDecimals".into(),
            });
        }
        let (price, order_type) = match intent.shape.kind {
            OrderKind::Limit => {
                let price = intent
                    .shape
                    .price
                    .ok_or_else(|| PlacementError::NotSubmitted {
                        message: "TRADE.XYZ limit price is missing".into(),
                    })?;
                if !valid_price(price, market.size_decimals) {
                    return Err(PlacementError::NotSubmitted {
                        message: "TRADE.XYZ limit price violates Hyperliquid tick precision".into(),
                    });
                }
                let order_type = match intent.shape.time_in_force {
                    TimeInForce::Gtc => WireOrderType::gtc(),
                    TimeInForce::PostOnly => WireOrderType::post_only(),
                };
                (price, order_type)
            }
            OrderKind::Market => {
                let tick = effective_price_tick(market.mid_price, market.size_decimals)
                    .ok_or_else(|| PlacementError::NotSubmitted {
                        message: "TRADE.XYZ market price precision is invalid".into(),
                    })?;
                let factor = if intent.shape.side == OrderSide::Buy {
                    Decimal::ONE + market_slippage()
                } else {
                    Decimal::ONE - market_slippage()
                };
                let unrounded = market.mid_price * factor;
                let steps =
                    unrounded
                        .checked_div(tick)
                        .ok_or_else(|| PlacementError::NotSubmitted {
                            message: "TRADE.XYZ market price overflowed".into(),
                        })?;
                let price = if intent.shape.side == OrderSide::Buy {
                    steps.ceil() * tick
                } else {
                    steps.floor() * tick
                };
                if !valid_price(price, market.size_decimals) {
                    return Err(PlacementError::NotSubmitted {
                        message: "TRADE.XYZ market protection price is invalid".into(),
                    });
                }
                (price, WireOrderType::immediate_or_cancel())
            }
        };
        if intent.shape.quantity * price < minimum_notional() {
            return Err(PlacementError::Definitive {
                code: Some("MIN_NOTIONAL".into()),
                message: "TRADE.XYZ order notional is below 10 USDC".into(),
            });
        }
        let cloid = encode_cloid(&intent.client_order_id).map_err(|error| {
            PlacementError::NotSubmitted {
                message: error.to_string(),
            }
        })?;
        let action = OrderAction::single(WireOrder {
            asset: market.asset_id,
            is_buy: intent.shape.side == OrderSide::Buy,
            price: wire_decimal(price),
            size: wire_decimal(intent.shape.quantity),
            reduce_only: intent.shape.reduce_only,
            order_type,
            cloid: Some(cloid),
        });
        let response =
            self.post_action(&action)
                .await
                .map_err(|error| PlacementError::Unknown {
                    message: error.to_string(),
                })?;
        let value = parse_write_response(response, "TRADE.XYZ order placement")
            .map_err(write_to_placement_error)?;
        let exchange_order_id = placement_order_id(&value).map_err(write_to_placement_error)?;
        Ok(PlacementAcknowledgement {
            client_order_id: intent.client_order_id.clone(),
            exchange_order_id,
        })
    }
}

#[async_trait]
impl<T, N> OrderCancellationGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError> {
        validate_request(exchange, symbol).map_err(|error| CancellationError::Invalid {
            message: error.message,
        })?;
        let order_id =
            exchange_order_id
                .parse::<u64>()
                .map_err(|_| CancellationError::Invalid {
                    message: "TRADE.XYZ order identity is invalid".into(),
                })?;
        let market =
            self.market_info(symbol)
                .await
                .map_err(|error| CancellationError::Invalid {
                    message: error.message,
                })?;
        let response = self
            .post_action(&CancelAction::single(market.asset_id, order_id))
            .await
            .map_err(|error| CancellationError::Unknown {
                message: error.to_string(),
            })?;
        let value =
            parse_write_response(response, "TRADE.XYZ order cancellation").map_err(|error| {
                CancellationError::Unknown {
                    message: error.message,
                }
            })?;
        ensure_cancel_ok(&value).map_err(|error| CancellationError::Unknown {
            message: error.message,
        })?;
        Ok(CancellationAcknowledgement {
            client_order_id: client_order_id.clone(),
            exchange_order_id: exchange_order_id.to_owned(),
        })
    }
}

#[async_trait]
impl<T, N> OrderLookupGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError> {
        validate_request(exchange, symbol).map_err(|error| LookupError {
            message: error.message,
        })?;
        let cloid = encode_cloid(client_order_id).map_err(|error| LookupError {
            message: error.to_string(),
        })?;
        let value = self
            .order_status_value(&cloid)
            .await
            .map_err(|error| LookupError {
                message: error.message,
            })?;
        if value.get("status").and_then(Value::as_str) == Some("unknownOid") {
            return Ok(OrderLookup::NotFound);
        }
        let (row, status, _) = order_status_parts(&value).map_err(|error| LookupError {
            message: error.message,
        })?;
        let order = parse_authoritative_order(
            row,
            status,
            &exchange_coin(symbol).map_err(|error| LookupError {
                message: error.to_string(),
            })?,
            Some(client_order_id),
        )
        .map_err(|error| LookupError {
            message: error.message,
        })?;
        Ok(OrderLookup::Found(order))
    }
}

#[async_trait]
impl<T, N> OpenOrderSnapshotGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<AuthoritativeOrder>, SnapshotError> {
        validate_request(exchange, symbol)?;
        let coin = exchange_coin(symbol).map_err(|error| SnapshotError::new(error.to_string()))?;
        let rows = self.frontend_open_orders().await?;
        let mut cached = BTreeMap::new();
        for row in &rows {
            if row.get("coin").and_then(Value::as_str) != Some(coin.as_str()) {
                continue;
            }
            let Some(cloid) = row.get("cloid").and_then(Value::as_str) else {
                continue;
            };
            let Some(client_order_id) = decode_cloid(cloid) else {
                continue;
            };
            let order = parse_authoritative_order(row, "open", &coin, Some(&client_order_id))?;
            cached.insert(
                client_order_id,
                CachedOpenExecution {
                    order: order.clone(),
                    order_time_ms: u64_field(row, "timestamp")?,
                },
            );
        }
        let symbol = symbol.to_ascii_uppercase();
        let mut cache = self.open_execution_cache.lock().await;
        if let Some(previous) = cache.get(&symbol) {
            for (client_order_id, current) in &mut cached {
                let Some(prior) = previous.get(client_order_id) else {
                    continue;
                };
                let prior_quantity = prior.order.executed_quantity.unwrap_or(Decimal::ZERO);
                let current_quantity = current.order.executed_quantity.unwrap_or(Decimal::ZERO);
                if prior.order.exchange_order_id == current.order.exchange_order_id
                    && prior.order.shape == current.order.shape
                    && prior_quantity > current_quantity
                {
                    *current = prior.clone();
                }
            }
        }
        let orders = cached.values().map(|cached| cached.order.clone()).collect();
        cache.insert(symbol, cached);
        Ok(orders)
    }
}

#[async_trait]
impl<T, N> OrderHistorySnapshotGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn order_history_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<HistoricalOrder>, SnapshotError> {
        validate_request(exchange, symbol)?;
        if limit == 0 || limit > 2_000 {
            return Err(SnapshotError::new("TRADE.XYZ history limit is invalid"));
        }
        let coin = exchange_coin(symbol).map_err(|error| SnapshotError::new(error.to_string()))?;
        let value = self
            .post_info(
                json!({"type": "historicalOrders", "user": self.account_address}),
                "TRADE.XYZ order history lookup failed",
            )
            .await?;
        let rows = value
            .as_array()
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ order history is invalid"))?;
        parse_historical_orders(rows, &symbol.to_ascii_uppercase(), &coin, limit)
    }
}

#[async_trait]
impl<T, N> ExecutionSnapshotGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn open_order_execution_progress_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Option<Vec<OpenOrderExecutionProgress>>, ExecutionSnapshotError> {
        let orders = self
            .open_orders_snapshot(exchange, symbol)
            .await
            .map_err(|error| ExecutionSnapshotError::new(error.message))?;
        Ok(Some(
            orders
                .into_iter()
                .map(|order| OpenOrderExecutionProgress {
                    cumulative_quantity: order.executed_quantity.unwrap_or(Decimal::ZERO),
                    order,
                })
                .collect(),
        ))
    }

    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        validate_request(exchange, symbol)
            .map_err(|error| ExecutionSnapshotError::new(error.message))?;
        let cached_open = self
            .open_execution_cache
            .lock()
            .await
            .get(&symbol.to_ascii_uppercase())
            .and_then(|orders| orders.get(client_order_id))
            .cloned()
            .filter(|cached| cached.order.exchange_order_id == exchange_order_id);
        let (mut order, order_time, status_time) = if let Some(cached) = cached_open {
            (cached.order, cached.order_time_ms, cached.order_time_ms)
        } else {
            let cloid = encode_cloid(client_order_id)
                .map_err(|error| ExecutionSnapshotError::new(error.to_string()))?;
            let value = self
                .order_status_value(&cloid)
                .await
                .map_err(|error| ExecutionSnapshotError::new(error.message))?;
            let (row, status, status_time) = order_status_parts(&value)
                .map_err(|error| ExecutionSnapshotError::new(error.message))?;
            let coin = exchange_coin(symbol)
                .map_err(|error| ExecutionSnapshotError::new(error.to_string()))?;
            let order = parse_authoritative_order(row, status, &coin, Some(client_order_id))
                .map_err(|error| ExecutionSnapshotError::new(error.message))?;
            let order_time = u64_field(row, "timestamp")
                .map_err(|error| ExecutionSnapshotError::new(error.message))?;
            (order, order_time, status_time)
        };
        if order.exchange_order_id != exchange_order_id {
            return Err(ExecutionSnapshotError::new(
                "TRADE.XYZ order identity changed during execution collection",
            ));
        }
        let collection_time =
            now_ms().map_err(|error| ExecutionSnapshotError::new(error.message))?;
        let fill_end_time = collection_time.max(status_time);
        let expected_quantity = order.executed_quantity.ok_or_else(|| {
            ExecutionSnapshotError::new("TRADE.XYZ authoritative execution quantity is missing")
        })?;
        let trades = if expected_quantity.is_zero() {
            Vec::new()
        } else {
            let cache = Arc::clone(&self.execution_fill_cache);
            let mut cache = cache.lock().await;
            let cached = trades_for_order(&cache.rows, symbol, exchange_order_id)?;
            if trade_quantity(&cached)? == expected_quantity {
                cached
            } else {
                let fills_value = self
                    .post_info(
                        json!({
                            "type": "userFillsByTime",
                            "user": self.account_address,
                            "startTime": order_time.saturating_sub(EXECUTION_FILL_LOOKBACK_MS),
                            "endTime": fill_end_time.saturating_add(1),
                            "aggregateByTime": false,
                        }),
                        "TRADE.XYZ execution lookup failed",
                    )
                    .await
                    .map_err(|error| ExecutionSnapshotError::new(error.message))?;
                let rows = fills_value.as_array().ok_or_else(|| {
                    ExecutionSnapshotError::new("TRADE.XYZ fill history is invalid")
                })?;
                if rows.len() >= 2_000 {
                    return Err(ExecutionSnapshotError::new(
                        "TRADE.XYZ fill history reached the API page limit",
                    ));
                }
                cache.rows.clone_from(rows);
                trades_for_order(&cache.rows, symbol, exchange_order_id)?
            }
        };
        let (cumulative_quantity, cumulative_quote, fees_by_asset) = summarize_trades(&trades)?;
        if matches!(order.lifecycle, OrderLifecycle::Active(_))
            && cumulative_quantity > expected_quantity
            && cumulative_quantity <= order.shape.quantity
        {
            order.executed_quantity = Some(cumulative_quantity);
            order.lifecycle = if cumulative_quantity == order.shape.quantity {
                OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
            } else {
                OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
            };
        }
        if order.executed_quantity != Some(cumulative_quantity) {
            return Err(ExecutionSnapshotError::new(
                "TRADE.XYZ fills do not reconcile to the authoritative order quantity",
            ));
        }
        Ok(OrderExecutionSnapshot {
            order,
            cumulative_quantity,
            cumulative_quote,
            fees_by_asset,
            trades,
            order_time_ms: order_time,
            update_time_ms: fill_end_time,
        })
    }
}

#[async_trait]
impl<T, N> HistoricalPriceGateway for TradeXyzAdapter<T, N>
where
    T: HttpTransport,
    N: NonceSource,
{
    async fn historical_minute_open(
        &self,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<HistoricalMinutePrice, SnapshotError> {
        validate_request(exchange, symbol)?;
        if minute_start_ms == 0 || !minute_start_ms.is_multiple_of(60_000) {
            return Err(SnapshotError::new("TRADE.XYZ candle time is invalid"));
        }
        let coin = exchange_coin(symbol).map_err(|error| SnapshotError::new(error.to_string()))?;
        let value = self
            .post_info(
                json!({
                    "type": "candleSnapshot",
                    "req": {
                        "coin": coin,
                        "interval": "1m",
                        "startTime": minute_start_ms,
                        "endTime": minute_start_ms.saturating_add(60_000),
                    }
                }),
                "TRADE.XYZ candle lookup failed",
            )
            .await?;
        let row = value
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row.get("t").and_then(value_u64) == Some(minute_start_ms))
            })
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ minute candle is unavailable"))?;
        Ok(HistoricalMinutePrice {
            exchange: Exchange::TradeXyz,
            symbol: symbol.to_ascii_uppercase(),
            minute_start_ms,
            open_price: decimal_field(row, "o")?,
        })
    }
}

fn parse_success_json(response: HttpResponse, context: &str) -> Result<Value, SnapshotError> {
    if !(200..300).contains(&response.status) {
        return Err(SnapshotError::new(format!(
            "{context}: HTTP {}",
            response.status
        )));
    }
    serde_json::from_str(&response.body)
        .map_err(|_| SnapshotError::new(format!("{context}: invalid JSON response")))
}

#[derive(Debug)]
struct WriteResponseError {
    code: Option<String>,
    message: String,
    definitive: bool,
}

fn parse_write_response(
    response: HttpResponse,
    context: &str,
) -> Result<Value, WriteResponseError> {
    let definitive =
        (400..500).contains(&response.status) && !matches!(response.status, 408 | 425 | 429);
    if !(200..300).contains(&response.status) {
        return Err(WriteResponseError {
            code: Some(response.status.to_string()),
            message: format!("{context}: HTTP {}", response.status),
            definitive,
        });
    }
    serde_json::from_str(&response.body).map_err(|_| WriteResponseError {
        code: None,
        message: format!("{context}: invalid JSON response"),
        definitive: false,
    })
}

fn ensure_action_ok(value: &Value, context: &str) -> Result<(), WriteResponseError> {
    if value.get("status").and_then(Value::as_str) == Some("ok") {
        return Ok(());
    }
    Err(WriteResponseError {
        code: None,
        message: value
            .get("response")
            .and_then(Value::as_str)
            .map_or_else(|| format!("{context} was rejected"), str::to_owned),
        definitive: true,
    })
}

fn statuses(value: &Value) -> Result<&Vec<Value>, WriteResponseError> {
    ensure_action_ok(value, "TRADE.XYZ exchange action")?;
    value
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .ok_or_else(|| WriteResponseError {
            code: None,
            message: "TRADE.XYZ action acknowledgement is invalid".into(),
            definitive: false,
        })
}

fn placement_order_id(value: &Value) -> Result<String, WriteResponseError> {
    let status = statuses(value)?.first().ok_or_else(|| WriteResponseError {
        code: None,
        message: "TRADE.XYZ placement acknowledgement is empty".into(),
        definitive: false,
    })?;
    if let Some(message) = status.get("error").and_then(Value::as_str) {
        return Err(WriteResponseError {
            code: None,
            message: message.to_owned(),
            definitive: true,
        });
    }
    status
        .get("resting")
        .or_else(|| status.get("filled"))
        .and_then(|value| value.get("oid"))
        .and_then(value_u64)
        .map(|value| value.to_string())
        .ok_or_else(|| WriteResponseError {
            code: None,
            message: "TRADE.XYZ placement acknowledgement has no order ID".into(),
            definitive: false,
        })
}

fn ensure_cancel_ok(value: &Value) -> Result<(), WriteResponseError> {
    let status = statuses(value)?.first().ok_or_else(|| WriteResponseError {
        code: None,
        message: "TRADE.XYZ cancellation acknowledgement is empty".into(),
        definitive: false,
    })?;
    if status.as_str() == Some("success") {
        return Ok(());
    }
    Err(WriteResponseError {
        code: None,
        message: status
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("TRADE.XYZ cancellation was not confirmed")
            .to_owned(),
        definitive: false,
    })
}

fn write_to_placement_error(error: WriteResponseError) -> PlacementError {
    if error.definitive {
        PlacementError::Definitive {
            code: error.code,
            message: error.message,
        }
    } else {
        PlacementError::Unknown {
            message: error.message,
        }
    }
}

fn write_to_leverage_error(error: WriteResponseError) -> LeverageError {
    if error.definitive {
        LeverageError::Definitive {
            code: error.code,
            message: error.message,
        }
    } else {
        LeverageError::Unknown {
            message: error.message,
        }
    }
}

fn order_status_parts(value: &Value) -> Result<(&Value, &str, u64), SnapshotError> {
    if value.get("status").and_then(Value::as_str) != Some("order") {
        return Err(SnapshotError::new("TRADE.XYZ order status is unavailable"));
    }
    let envelope = value
        .get("order")
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ order status is invalid"))?;
    let row = envelope
        .get("order")
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ authoritative order is invalid"))?;
    Ok((
        row,
        text_field(envelope, "status")?,
        u64_field(envelope, "statusTimestamp")?,
    ))
}

fn parse_authoritative_order(
    row: &Value,
    status: &str,
    expected_coin: &str,
    expected_client_order_id: Option<&ClientOrderId>,
) -> Result<AuthoritativeOrder, SnapshotError> {
    if text_field(row, "coin")? != expected_coin {
        return Err(SnapshotError::new("TRADE.XYZ order symbol mismatch"));
    }
    let cloid = text_field(row, "cloid")?;
    let decoded = decode_cloid(cloid)
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ strategy cloid is invalid"))?;
    if expected_client_order_id.is_some_and(|expected| expected != &decoded) {
        return Err(SnapshotError::new(
            "TRADE.XYZ client order identity mismatch",
        ));
    }
    let original = decimal_field(row, "origSz")?;
    let remaining = decimal_field(row, "sz")?;
    let executed = original
        .checked_sub(remaining)
        .filter(|quantity| *quantity >= Decimal::ZERO)
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ executed quantity is invalid"))?;
    let kind = if row.get("orderType").and_then(Value::as_str) == Some("Market")
        || matches!(
            row.get("tif").and_then(Value::as_str),
            Some("FrontendMarket" | "Ioc")
        ) {
        OrderKind::Market
    } else {
        OrderKind::Limit
    };
    let time_in_force = if row.get("tif").and_then(Value::as_str) == Some("Alo") {
        TimeInForce::PostOnly
    } else {
        TimeInForce::Gtc
    };
    let lifecycle = lifecycle(status, executed)?;
    let shape = OrderShape {
        symbol: local_symbol(expected_coin)
            .map_err(|error| SnapshotError::new(error.to_string()))?,
        side: parse_side(row)?,
        price: (kind == OrderKind::Limit)
            .then(|| decimal_field(row, "limitPx"))
            .transpose()?,
        quantity: original,
        reduce_only: row
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ reduce-only state is unavailable"))?,
        kind,
        time_in_force,
    };
    shape
        .validate()
        .map_err(|error| SnapshotError::new(error.to_string()))?;
    Ok(AuthoritativeOrder {
        client_order_id: decoded,
        exchange_order_id: id_string(row.get("oid"))?,
        exchange: Exchange::TradeXyz,
        shape,
        lifecycle,
        executed_quantity: Some(executed),
    })
}

fn parse_historical_orders(
    rows: &[Value],
    symbol: &str,
    expected_coin: &str,
    limit: usize,
) -> Result<Vec<HistoricalOrder>, SnapshotError> {
    let mut history = Vec::new();
    for item in rows {
        let row = item
            .get("order")
            .ok_or_else(|| SnapshotError::new("TRADE.XYZ historical order is invalid"))?;
        if row.get("coin").and_then(Value::as_str) != Some(expected_coin) {
            continue;
        }
        let order = HistoricalOrder {
            exchange_order_id: id_string(row.get("oid"))?,
            exchange: Exchange::TradeXyz,
            symbol: symbol.to_owned(),
            side: parse_side(row)?,
            price: decimal_field(row, "limitPx")?,
            quantity: decimal_field(row, "origSz")?,
            status: text_field(item, "status")?.to_owned(),
            created_at_ms: u64_field(row, "timestamp")?,
        };
        order.validate()?;
        history.push(order);
    }
    history.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.exchange_order_id.cmp(&right.exchange_order_id))
    });
    if history.len() > limit {
        history = history.split_off(history.len() - limit);
    }
    Ok(history)
}

fn lifecycle(status: &str, executed: Decimal) -> Result<OrderLifecycle, SnapshotError> {
    Ok(match status {
        "open" => OrderLifecycle::Active(if executed.is_zero() {
            ActiveOrderStatus::New
        } else {
            ActiveOrderStatus::PartiallyFilled
        }),
        "filled" => OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
        "canceled"
        | "marginCanceled"
        | "vaultWithdrawalCanceled"
        | "openInterestCapCanceled"
        | "selfTradeCanceled"
        | "reduceOnlyCanceled"
        | "siblingFilledCanceled"
        | "delistedCanceled"
        | "liquidatedCanceled"
        | "scheduledCancel" => OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
        "rejected"
        | "tickRejected"
        | "minTradeNtlRejected"
        | "perpMarginRejected"
        | "reduceOnlyRejected"
        | "badAloPxRejected"
        | "iocCancelRejected"
        | "badTriggerPxRejected"
        | "marketOrderNoLiquidityRejected"
        | "positionIncreaseAtOpenInterestCapRejected"
        | "positionFlipAtOpenInterestCapRejected"
        | "tooAggressiveAtOpenInterestCapRejected"
        | "openInterestIncreaseRejected"
        | "insufficientSpotBalanceRejected"
        | "oracleRejected"
        | "perpMaxPositionRejected" => OrderLifecycle::Terminal(TerminalOrderStatus::Rejected),
        "triggered" => OrderLifecycle::Terminal(TerminalOrderStatus::Expired),
        _ => return Err(SnapshotError::new("TRADE.XYZ order status is unsupported")),
    })
}

fn parse_trade_fill(
    row: &Value,
    symbol: &str,
    exchange_order_id: &str,
) -> Result<TradeFill, ExecutionSnapshotError> {
    let price = decimal_field(row, "px").map_err(snapshot_to_execution)?;
    let quantity = decimal_field(row, "sz").map_err(snapshot_to_execution)?;
    let raw_fee = decimal_field(row, "fee").map_err(snapshot_to_execution)?;
    let side = parse_side(row).map_err(snapshot_to_execution)?;
    Ok(TradeFill {
        trade_id: id_string(row.get("tid")).map_err(snapshot_to_execution)?,
        exchange_order_id: exchange_order_id.to_owned(),
        symbol: symbol.to_ascii_uppercase(),
        side,
        price,
        quantity,
        quote_quantity: price
            .checked_mul(quantity)
            .ok_or_else(|| ExecutionSnapshotError::new("TRADE.XYZ fill quote overflowed"))?,
        raw_commission: raw_fee,
        // Existing strategy accounting models costs as non-negative. Preserve
        // the exact signed rebate in raw_commission and never misstate it as a cost.
        commission_cost: raw_fee.max(Decimal::ZERO),
        commission_asset: text_field(row, "feeToken")
            .map_err(snapshot_to_execution)?
            .to_ascii_uppercase(),
        realized_profit: optional_decimal(row, "closedPnl")
            .map_err(snapshot_to_execution)?
            .unwrap_or(Decimal::ZERO),
        is_maker: !row
            .get("crossed")
            .and_then(Value::as_bool)
            .ok_or_else(|| ExecutionSnapshotError::new("TRADE.XYZ fill liquidity is invalid"))?,
        trade_time_ms: u64_field(row, "time").map_err(snapshot_to_execution)?,
    })
}

fn trades_for_order(
    rows: &[Value],
    symbol: &str,
    exchange_order_id: &str,
) -> Result<Vec<TradeFill>, ExecutionSnapshotError> {
    let mut trades = rows
        .iter()
        .filter(|fill| id_string(fill.get("oid")).ok().as_deref() == Some(exchange_order_id))
        .map(|fill| parse_trade_fill(fill, symbol, exchange_order_id))
        .collect::<Result<Vec<_>, _>>()?;
    trades.sort_by(|left, right| {
        compare_trade_chronology(
            left.trade_time_ms,
            &left.trade_id,
            right.trade_time_ms,
            &right.trade_id,
        )
    });
    Ok(trades)
}

fn trade_quantity(trades: &[TradeFill]) -> Result<Decimal, ExecutionSnapshotError> {
    trades.iter().try_fold(Decimal::ZERO, |total, trade| {
        total
            .checked_add(trade.quantity)
            .ok_or_else(|| ExecutionSnapshotError::new("TRADE.XYZ fill quantity overflowed"))
    })
}

fn summarize_trades(
    trades: &[TradeFill],
) -> Result<(Decimal, Decimal, BTreeMap<String, Decimal>), ExecutionSnapshotError> {
    let cumulative_quantity = trade_quantity(trades)?;
    let mut cumulative_quote = Decimal::ZERO;
    let mut fees_by_asset = BTreeMap::new();
    for trade in trades {
        cumulative_quote = cumulative_quote
            .checked_add(trade.quote_quantity)
            .ok_or_else(|| ExecutionSnapshotError::new("TRADE.XYZ fill quote overflowed"))?;
        let fee = fees_by_asset
            .entry(trade.commission_asset.clone())
            .or_insert(Decimal::ZERO);
        *fee = fee
            .checked_add(trade.commission_cost)
            .ok_or_else(|| ExecutionSnapshotError::new("TRADE.XYZ fill fee overflowed"))?;
    }
    Ok((cumulative_quantity, cumulative_quote, fees_by_asset))
}

fn snapshot_to_execution(error: SnapshotError) -> ExecutionSnapshotError {
    ExecutionSnapshotError::new(error.message)
}

fn validate_request(exchange: Exchange, symbol: &str) -> Result<(), SnapshotError> {
    if exchange != Exchange::TradeXyz {
        return Err(SnapshotError::new("TRADE.XYZ exchange identity mismatch"));
    }
    exchange_coin(&symbol.to_ascii_uppercase())
        .map_err(|error| SnapshotError::new(error.to_string()))?;
    Ok(())
}

fn parse_side(row: &Value) -> Result<OrderSide, SnapshotError> {
    match text_field(row, "side")? {
        "B" => Ok(OrderSide::Buy),
        "A" => Ok(OrderSide::Sell),
        _ => Err(SnapshotError::new("TRADE.XYZ order side is invalid")),
    }
}

fn decimal_field(value: &Value, field: &str) -> Result<Decimal, SnapshotError> {
    optional_decimal(value, field)?
        .ok_or_else(|| SnapshotError::new(format!("TRADE.XYZ field {field} is unavailable")))
}

fn optional_decimal(value: &Value, field: &str) -> Result<Option<Decimal>, SnapshotError> {
    let Some(value) = value.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .or_else(|| value.as_u64().map(|value| value.to_string()))
        .ok_or_else(|| SnapshotError::new(format!("TRADE.XYZ field {field} is invalid")))?;
    Decimal::from_str(&text)
        .map(Some)
        .map_err(|_| SnapshotError::new(format!("TRADE.XYZ field {field} is invalid")))
}

fn text_field<'a>(value: &'a Value, field: &str) -> Result<&'a str, SnapshotError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| SnapshotError::new(format!("TRADE.XYZ field {field} is invalid")))
}

fn u64_field(value: &Value, field: &str) -> Result<u64, SnapshotError> {
    value
        .get(field)
        .and_then(value_u64)
        .ok_or_else(|| SnapshotError::new(format!("TRADE.XYZ field {field} is invalid")))
}

fn u32_field(value: &Value, field: &str) -> Result<u32, SnapshotError> {
    u64_field(value, field).and_then(|value| {
        u32::try_from(value)
            .map_err(|_| SnapshotError::new(format!("TRADE.XYZ field {field} is invalid")))
    })
}

fn value_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn id_string(value: Option<&Value>) -> Result<String, SnapshotError> {
    let value = value.ok_or_else(|| SnapshotError::new("TRADE.XYZ identity is unavailable"))?;
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SnapshotError::new("TRADE.XYZ identity is invalid"))
}

fn now_ms() -> Result<u64, SnapshotError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or_else(|| SnapshotError::new("system clock is unavailable"))
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use super::*;

    fn official_public_test_vector_key() -> String {
        hex::encode([
            0xe9, 0x08, 0xf8, 0x6d, 0xbb, 0x4d, 0x55, 0xac, 0x87, 0x63, 0x78, 0x56, 0x5a, 0xaf,
            0xea, 0xbc, 0x18, 0x7f, 0x66, 0x90, 0xf0, 0x46, 0x45, 0x93, 0x97, 0xb1, 0x7d, 0x9b,
            0x9a, 0x19, 0x68, 0x8e,
        ])
    }

    #[derive(Clone)]
    struct ScriptedTransport {
        responses: Arc<Mutex<VecDeque<HttpResponse>>>,
        requests: Arc<Mutex<Vec<PreparedHttpRequest>>>,
    }

    impl ScriptedTransport {
        fn new(responses: impl IntoIterator<Item = HttpResponse>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<PreparedHttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HttpTransport for ScriptedTransport {
        async fn execute(
            &self,
            request: PreparedHttpRequest,
        ) -> Result<HttpResponse, TransportError> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| TransportError::Other("script is exhausted".into()))
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedNonce;

    impl NonceSource for FixedNonce {
        fn next_nonce(&self) -> u64 {
            1_583_838
        }
    }

    fn response(body: Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            body: body.to_string(),
        }
    }

    fn market_response() -> HttpResponse {
        let mut universe = (0..15)
            .map(|index| {
                json!({
                    "name": format!("xyz:D{index}"),
                    "szDecimals": 2,
                    "maxLeverage": 5
                })
            })
            .collect::<Vec<_>>();
        let mut contexts = (0..15)
            .map(|_| json!({"markPx": "1", "midPx": "1"}))
            .collect::<Vec<_>>();
        universe.push(json!({
            "name": "xyz:MU",
            "szDecimals": 3,
            "maxLeverage": 10,
            "growthMode": "enabled"
        }));
        contexts.push(json!({
            "markPx": "849.84",
            "midPx": "849.955",
            "prevDayPx": "800",
            "dayBaseVlm": "81704.125"
        }));
        response(json!([{"universe": universe}, contexts]))
    }

    fn dex_response() -> HttpResponse {
        response(json!([
            null,
            {"name": "xyz", "deployerFeeScale": "1.0"}
        ]))
    }

    fn test_adapter(
        transport: ScriptedTransport,
    ) -> TradeXyzAdapter<ScriptedTransport, FixedNonce> {
        let private_key = official_public_test_vector_key();
        let account = HyperliquidSigner::from_private_key(&private_key)
            .unwrap()
            .address()
            .to_owned();
        TradeXyzAdapter::production_wallet(transport, FixedNonce, &account, &private_key).unwrap()
    }

    fn limit_intent(client_order_id: &str) -> OrderIntent {
        OrderIntent::prepare(
            ClientOrderId::parse(client_order_id).unwrap(),
            Exchange::TradeXyz,
            OrderShape {
                symbol: "MUUSDC".into(),
                side: OrderSide::Sell,
                price: Some(Decimal::new(850, 0)),
                quantity: Decimal::new(2, 1),
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            100,
        )
        .unwrap()
    }

    #[test]
    fn terminal_statuses_never_treat_cancellation_as_fill() {
        assert_eq!(
            lifecycle("marginCanceled", Decimal::ZERO).unwrap(),
            OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled)
        );
        assert_eq!(
            lifecycle("minTradeNtlRejected", Decimal::ZERO).unwrap(),
            OrderLifecycle::Terminal(TerminalOrderStatus::Rejected)
        );
        assert_eq!(
            lifecycle("filled", Decimal::ONE).unwrap(),
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
        );
    }

    #[tokio::test]
    async fn market_snapshot_includes_authoritative_24h_statistics() {
        let transport = ScriptedTransport::new([market_response(), dex_response()]);
        let adapter = test_adapter(transport);

        let snapshot = adapter
            .market_snapshot(Exchange::TradeXyz, "MUUSDC")
            .await
            .unwrap();

        assert_eq!(
            snapshot.price_24h_change_ratio,
            Some(Decimal::from_str_exact("0.06244375").unwrap())
        );
        assert_eq!(
            snapshot.volume_24h,
            Some(Decimal::from_str_exact("81704.125").unwrap())
        );
    }

    #[test]
    fn placement_response_accepts_resting_and_immediate_fill_ids() {
        let resting = json!({
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 42}}]}}
        });
        let filled = json!({
            "status": "ok",
            "response": {"data": {"statuses": [{"filled": {"oid": 43}}]}}
        });
        assert_eq!(placement_order_id(&resting).unwrap(), "42");
        assert_eq!(placement_order_id(&filled).unwrap(), "43");
    }

    #[test]
    fn placement_response_preserves_definitive_exchange_rejection() {
        let rejected = json!({
            "status": "ok",
            "response": {"data": {"statuses": [{"error": "Insufficient margin"}]}}
        });
        let error = placement_order_id(&rejected).unwrap_err();
        assert!(error.definitive);
        assert_eq!(error.message, "Insufficient margin");
    }

    #[test]
    fn api_ioc_order_is_reconciled_as_the_market_intent_it_represents() {
        let client_order_id = ClientOrderId::parse("o_012345abcdef_S_4").unwrap();
        let row = json!({
            "coin": "xyz:MU",
            "side": "A",
            "limitPx": "807.45",
            "sz": "0",
            "origSz": "0.2",
            "oid": 44,
            "timestamp": 5,
            "reduceOnly": false,
            "orderType": "Limit",
            "tif": "Ioc",
            "cloid": encode_cloid(&client_order_id).unwrap()
        });

        let order =
            parse_authoritative_order(&row, "filled", "xyz:MU", Some(&client_order_id)).unwrap();

        assert_eq!(order.shape.kind, OrderKind::Market);
        assert_eq!(order.shape.price, None);
        assert_eq!(order.shape.time_in_force, TimeInForce::Gtc);
    }

    #[test]
    fn history_selects_the_latest_symbol_orders_and_returns_chronological_rows() {
        let row = |coin: &str, oid: u64, timestamp: u64| {
            json!({
                "order": {
                    "coin": coin,
                    "side": "A",
                    "limitPx": "850.25",
                    "origSz": "0.200",
                    "oid": oid,
                    "timestamp": timestamp
                },
                "status": "filled"
            })
        };
        let rows = vec![
            row("xyz:MU", 13, 300),
            row("xyz:NVDA", 99, 400),
            row("xyz:MU", 11, 100),
            row("xyz:MU", 12, 200),
        ];

        let history = parse_historical_orders(&rows, "MUUSDC", "xyz:MU", 2).unwrap();

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].exchange_order_id, "12");
        assert_eq!(history[1].exchange_order_id, "13");
        assert!(history.iter().all(|order| order.symbol == "MUUSDC"));
    }

    #[tokio::test]
    async fn standard_account_balance_uses_the_dex_margin_state() {
        let transport = ScriptedTransport::new([
            response(json!("default")),
            response(json!({
                "marginSummary": {"accountValue": "125.5"},
                "withdrawable": "100.25",
                "assetPositions": [
                    {"position": {"unrealizedPnl": "5.5"}},
                    {"position": {"unrealizedPnl": "-1.25"}}
                ]
            })),
        ]);
        let adapter = test_adapter(transport.clone());

        let snapshot = adapter
            .account_balance_snapshot(Exchange::TradeXyz)
            .await
            .unwrap();

        assert_eq!(snapshot.unit, AccountBalanceUnit::Usdc);
        assert_eq!(snapshot.available_balance, Decimal::new(10025, 2));
        assert_eq!(snapshot.unrealized_profit, Decimal::new(425, 2));
        assert_eq!(snapshot.wallet_balance, Decimal::new(12125, 2));
        assert_eq!(snapshot.equity, Decimal::new(1255, 1));
        assert_eq!(transport.requests().len(), 2);
    }

    #[tokio::test]
    async fn unified_account_balance_uses_spot_usdc_without_double_counting_pnl() {
        let transport = ScriptedTransport::new([
            response(json!("unifiedAccount")),
            response(json!({
                "assetPositions": [
                    {"position": {"unrealizedPnl": "3.75"}},
                    {"position": {"unrealizedPnl": "-0.25"}}
                ]
            })),
            response(json!({
                "balances": [
                    {"coin": "USDC", "total": "250", "hold": "12.5"},
                    {"coin": "HYPE", "total": "1", "hold": "0"}
                ]
            })),
        ]);
        let adapter = test_adapter(transport.clone());

        let snapshot = adapter
            .account_balance_snapshot(Exchange::TradeXyz)
            .await
            .unwrap();

        assert_eq!(snapshot.available_balance, Decimal::new(2375, 1));
        assert_eq!(snapshot.wallet_balance, Decimal::new(250, 0));
        assert_eq!(snapshot.unrealized_profit, Decimal::new(35, 1));
        assert_eq!(snapshot.equity, Decimal::new(2535, 1));
        let requests = transport.requests();
        assert_eq!(requests.len(), 3);
        let spot_query: Value =
            serde_json::from_str(requests[2].raw_body.as_deref().unwrap()).unwrap();
        assert_eq!(spot_query["type"], "spotClearinghouseState");
    }

    #[tokio::test]
    async fn portfolio_margin_is_rejected_before_balance_can_drive_an_order() {
        let transport = ScriptedTransport::new([
            response(json!("portfolioMargin")),
            response(json!({"assetPositions": []})),
        ]);
        let adapter = test_adapter(transport.clone());

        let error = adapter
            .account_balance_snapshot(Exchange::TradeXyz)
            .await
            .unwrap_err();

        assert!(error.message.contains("portfolio-margin"));
        assert_eq!(transport.requests().len(), 2);
    }

    #[tokio::test]
    async fn limit_placements_use_the_exact_hip3_asset_and_share_fresh_metadata() {
        let transport = ScriptedTransport::new([
            market_response(),
            dex_response(),
            response(json!({
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 41}}]}}
            })),
            response(json!({
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 42}}]}}
            })),
        ]);
        let adapter = test_adapter(transport.clone());

        adapter
            .place_order(&limit_intent("g_012345abcdef_15_S_1"))
            .await
            .unwrap();
        adapter
            .place_order(&limit_intent("g_012345abcdef_15_S_2"))
            .await
            .unwrap();

        let requests = transport.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].path, "/info");
        assert_eq!(requests[1].path, "/info");
        for request in &requests[2..] {
            assert_eq!(request.path, "/exchange");
            let body: Value = serde_json::from_str(request.raw_body.as_deref().unwrap()).unwrap();
            let order = &body["action"]["orders"][0];
            assert_eq!(order["a"], 110_015);
            assert_eq!(order["p"], "850");
            assert_eq!(order["s"], "0.2");
            assert_eq!(order["r"], false);
            assert!(decode_cloid(order["c"].as_str().unwrap()).is_some());
        }
    }

    #[tokio::test]
    async fn limit_minimum_notional_uses_the_order_price_not_the_mark_price() {
        let transport = ScriptedTransport::new([market_response(), dex_response()]);
        let adapter = test_adapter(transport.clone());
        let mut intent = limit_intent("g_012345abcdef_15_S_3");
        intent.shape.price = Some(Decimal::ONE);
        intent.shape.quantity = Decimal::new(2, 0);

        let error = adapter.place_order(&intent).await.unwrap_err();

        assert!(matches!(
            error,
            PlacementError::Definitive {
                code: Some(ref code),
                ..
            } if code == "MIN_NOTIONAL"
        ));
        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| request.path == "/info"));
    }

    #[tokio::test]
    async fn unchanged_leverage_is_not_reloaded_on_every_runtime_snapshot() {
        let transport = ScriptedTransport::new([response(json!({
            "leverage": {"type": "isolated", "value": 5}
        }))]);
        let adapter = test_adapter(transport.clone());

        assert_eq!(adapter.active_asset_leverage("xyz:MU").await.unwrap(), 5);
        assert_eq!(adapter.active_asset_leverage("xyz:MU").await.unwrap(), 5);

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_str(requests[0].raw_body.as_deref().unwrap()).unwrap();
        assert_eq!(body["type"], "activeAssetData");
        assert_eq!(body["coin"], "xyz:MU");
    }

    #[tokio::test]
    async fn partial_fill_collection_uses_collection_time_not_open_status_time() {
        let client_order_id = ClientOrderId::parse("g_012345abcdef_15_S_9").unwrap();
        let cloid = encode_cloid(&client_order_id).unwrap();
        let transport = ScriptedTransport::new([
            response(json!({
                "status": "order",
                "order": {
                    "order": {
                        "coin": "xyz:MU",
                        "side": "A",
                        "limitPx": "850",
                        "sz": "0.1",
                        "origSz": "0.2",
                        "oid": 42,
                        "timestamp": 1,
                        "reduceOnly": false,
                        "orderType": "Limit",
                        "tif": "Gtc",
                        "cloid": cloid
                    },
                    "status": "open",
                    "statusTimestamp": 2
                }
            })),
            response(json!([{
                "closedPnl": "0",
                "coin": "xyz:MU",
                "crossed": false,
                "oid": 42,
                "px": "850",
                "side": "A",
                "sz": "0.1",
                "time": 3,
                "fee": "0.01",
                "tid": 7,
                "feeToken": "USDC"
            }])),
        ]);
        let adapter = test_adapter(transport.clone());

        let snapshot = adapter
            .execution_snapshot(Exchange::TradeXyz, "MUUSDC", &client_order_id, "42")
            .await
            .unwrap();

        assert_eq!(snapshot.cumulative_quantity, Decimal::new(1, 1));
        assert_eq!(snapshot.order.executed_quantity, Some(Decimal::new(1, 1)));
        assert!(snapshot.update_time_ms >= snapshot.trades[0].trade_time_ms);
        let requests = transport.requests();
        let fill_query: Value =
            serde_json::from_str(requests[1].raw_body.as_deref().unwrap()).unwrap();
        assert!(fill_query["endTime"].as_u64().unwrap() > 3);
    }

    #[tokio::test]
    async fn open_progress_cannot_be_rolled_back_by_stale_order_status() {
        let client_order_id = ClientOrderId::parse("g_012345abcdef_15_S_9").unwrap();
        let cloid = encode_cloid(&client_order_id).unwrap();
        let transport = ScriptedTransport::new([
            response(json!([{
                "coin": "xyz:MU",
                "side": "A",
                "limitPx": "850",
                "sz": "13.1",
                "origSz": "15.0",
                "oid": 42,
                "timestamp": 1,
                "reduceOnly": false,
                "orderType": "Limit",
                "tif": "Gtc",
                "cloid": cloid
            }])),
            response(json!([{
                "closedPnl": "0",
                "coin": "xyz:MU",
                "crossed": false,
                "oid": 42,
                "px": "850",
                "side": "A",
                "sz": "1.9",
                "time": 3,
                "fee": "0.01",
                "tid": 7,
                "feeToken": "USDC"
            }])),
        ]);
        let adapter = test_adapter(transport.clone());

        let progress = adapter
            .open_order_execution_progress_snapshot(Exchange::TradeXyz, "MUUSDC")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(progress[0].cumulative_quantity, Decimal::new(19, 1));

        let snapshot = adapter
            .execution_snapshot(Exchange::TradeXyz, "MUUSDC", &client_order_id, "42")
            .await
            .unwrap();

        assert_eq!(snapshot.cumulative_quantity, Decimal::new(19, 1));
        assert_eq!(snapshot.order.executed_quantity, Some(Decimal::new(19, 1)));
        assert_eq!(
            snapshot.order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
        );
        let request_types = transport
            .requests()
            .iter()
            .map(|request| {
                serde_json::from_str::<Value>(request.raw_body.as_deref().unwrap()).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(request_types, vec!["frontendOpenOrders", "userFillsByTime"]);
    }

    #[tokio::test]
    async fn open_progress_is_monotonic_across_concurrent_exchange_reads() {
        let client_order_id = ClientOrderId::parse("g_012345abcdef_15_S_9").unwrap();
        let cloid = encode_cloid(&client_order_id).unwrap();
        let row = |remaining: &str| {
            json!([{
                "coin": "xyz:MU",
                "side": "A",
                "limitPx": "850",
                "sz": remaining,
                "origSz": "15.0",
                "oid": 42,
                "timestamp": 1,
                "reduceOnly": false,
                "orderType": "Limit",
                "tif": "Gtc",
                "cloid": cloid
            }])
        };
        let transport = ScriptedTransport::new([response(row("13.1")), response(row("15.0"))]);
        let adapter = test_adapter(transport);

        let first = adapter
            .open_order_execution_progress_snapshot(Exchange::TradeXyz, "MUUSDC")
            .await
            .unwrap()
            .unwrap();
        let stale = adapter
            .open_order_execution_progress_snapshot(Exchange::TradeXyz, "MUUSDC")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(first[0].cumulative_quantity, Decimal::new(19, 1));
        assert_eq!(stale[0].cumulative_quantity, Decimal::new(19, 1));
    }

    #[tokio::test]
    async fn exact_fills_can_advance_a_just_observed_partial_order() {
        let client_order_id = ClientOrderId::parse("g_012345abcdef_15_S_9").unwrap();
        let cloid = encode_cloid(&client_order_id).unwrap();
        let fill = |quantity: &str, trade_id: u64| {
            json!({
                "closedPnl": "0",
                "coin": "xyz:MU",
                "crossed": false,
                "oid": 42,
                "px": "850",
                "side": "A",
                "sz": quantity,
                "time": 3,
                "fee": "0.01",
                "tid": trade_id,
                "feeToken": "USDC"
            })
        };
        let transport = ScriptedTransport::new([
            response(json!([{
                "coin": "xyz:MU",
                "side": "A",
                "limitPx": "850",
                "sz": "0.1",
                "origSz": "0.2",
                "oid": 42,
                "timestamp": 1,
                "reduceOnly": false,
                "orderType": "Limit",
                "tif": "Gtc",
                "cloid": cloid
            }])),
            response(json!([fill("0.1", 7), fill("0.1", 8)])),
        ]);
        let adapter = test_adapter(transport);
        adapter
            .open_order_execution_progress_snapshot(Exchange::TradeXyz, "MUUSDC")
            .await
            .unwrap();

        let snapshot = adapter
            .execution_snapshot(Exchange::TradeXyz, "MUUSDC", &client_order_id, "42")
            .await
            .unwrap();

        assert_eq!(snapshot.cumulative_quantity, Decimal::new(2, 1));
        assert_eq!(snapshot.order.executed_quantity, Some(Decimal::new(2, 1)));
        assert_eq!(
            snapshot.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
        );
    }

    #[tokio::test]
    async fn execution_snapshots_share_one_exact_fill_batch() {
        let first_id = ClientOrderId::parse("g_012345abcdef_15_S_9").unwrap();
        let second_id = ClientOrderId::parse("g_012345abcdef_16_S_10").unwrap();
        let status = |client_order_id: &ClientOrderId, order_id: u64| {
            response(json!({
                "status": "order",
                "order": {
                    "order": {
                        "coin": "xyz:MU",
                        "side": "A",
                        "limitPx": "850",
                        "sz": "0.1",
                        "origSz": "0.2",
                        "oid": order_id,
                        "timestamp": 100,
                        "reduceOnly": false,
                        "orderType": "Limit",
                        "tif": "Gtc",
                        "cloid": encode_cloid(client_order_id).unwrap()
                    },
                    "status": "open",
                    "statusTimestamp": 200
                }
            }))
        };
        let fill = |order_id: u64, trade_id: u64| {
            json!({
                "closedPnl": "0",
                "coin": "xyz:MU",
                "crossed": false,
                "oid": order_id,
                "px": "850",
                "side": "A",
                "sz": "0.1",
                "time": 201,
                "fee": "0.01",
                "tid": trade_id,
                "feeToken": "USDC"
            })
        };
        let transport = ScriptedTransport::new([
            status(&first_id, 42),
            response(json!([fill(42, 7), fill(43, 8)])),
            status(&second_id, 43),
        ]);
        let adapter = test_adapter(transport.clone());

        let first = adapter
            .execution_snapshot(Exchange::TradeXyz, "MUUSDC", &first_id, "42")
            .await
            .unwrap();
        let second = adapter
            .execution_snapshot(Exchange::TradeXyz, "MUUSDC", &second_id, "43")
            .await
            .unwrap();

        assert_eq!(first.cumulative_quantity, Decimal::new(1, 1));
        assert_eq!(second.cumulative_quantity, Decimal::new(1, 1));
        let requests = transport.requests();
        assert_eq!(requests.len(), 3);
        let request_types = requests
            .iter()
            .map(|request| {
                serde_json::from_str::<Value>(request.raw_body.as_deref().unwrap()).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            request_types,
            vec!["orderStatus", "userFillsByTime", "orderStatus"]
        );
    }
}
