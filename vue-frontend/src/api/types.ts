export type Exchange = "binance" | "aster" | "bybit";
export type Direction = "long" | "short" | "neutral";
export type GridMode = "arithmetic" | "geometric";
export type PositionSizingMode = "investment" | "fixed_grid_qty";
export type InitialOrderType = "market" | "limit" | "post_only";
export type DecimalValue = string | number;

export interface GridConfigRequest {
  exchange?: Exchange;
  symbol: string;
  direction: Direction;
  upper_price: DecimalValue;
  lower_price: DecimalValue;
  grid_count: number;
  total_investment?: DecimalValue;
  leverage?: number;
  position_sizing_mode?: PositionSizingMode;
  grid_order_qty?: DecimalValue | null;
  initial_order_type?: InitialOrderType;
  initial_order_price?: DecimalValue | null;
  grid_order_post_only?: boolean;
  grid_mode?: GridMode;
  trigger_price?: DecimalValue | null;
  stop_loss_price?: DecimalValue | null;
  take_profit_price?: DecimalValue | null;
  maker_fee_rate?: DecimalValue;
  taker_fee_rate?: DecimalValue;
  fee_rate?: DecimalValue;
}

export interface AuthStatus {
  required: boolean;
  configured: boolean;
  authenticated: boolean;
  username?: string;
  totp_secret?: string;
}

export interface GridStatus {
  run_id?: string;
  exchange: Exchange;
  symbol: string;
  running: boolean;
  engine_running?: boolean;
  runtime_advancing?: boolean;
  lifecycle?: string;
  direction?: Direction;
  grid_mode?: GridMode;
  total_profit?: DecimalValue;
  completed_pairs?: number;
  trigger_message?: string;
  waiting_initial_order?: boolean;
  waiting_trigger?: boolean;
  total_equity_profit?: DecimalValue;
  gross_profit?: DecimalValue;
  total_fee?: DecimalValue;
  realized_net_profit?: DecimalValue;
  unrealised_pnl?: DecimalValue;
  total_volume?: DecimalValue;
  current_price?: DecimalValue;
  active_orders?: GridOrder[];
  filled_orders?: GridTrade[];
  baseline_position?: PositionBaseline;
  grid_position_net_qty?: DecimalValue;
  expected_position_net_qty?: DecimalValue;
  opening_filled_qty?: DecimalValue;
  opening_planned_qty?: DecimalValue;
  grid_profit_pct?: DecimalValue;
  initial_side?: "Buy" | "Sell";
  initial_qty?: number;
  [key: string]: unknown;
}

export interface GridStatusList {
  running: boolean;
  count: number;
  running_count?: number;
  trading_enabled?: boolean;
  grids: GridStatus[];
}

export interface LoginRequest {
  username: string;
  password: string;
  code: string;
}

export interface ExchangeConfigSummary {
  exchange: Exchange;
  configured: boolean;
  api_key?: string;
  testnet?: boolean;
  source?: "env" | "file" | string;
}

export interface ApiConfigResponse {
  configured: boolean;
  exchange?: Exchange;
  active_exchange?: Exchange;
  testnet?: boolean;
  storage_error?: string | boolean;
  configs: Partial<Record<Exchange, ExchangeConfigSummary>>;
}

export interface ExchangeConfigRequest {
  exchange: Exchange;
  api_key?: string;
  api_secret?: string;
  private_key?: string;
  testnet: boolean;
}

export interface ExchangeConfigSaveResponse {
  ok: boolean;
  message: string;
  exchange: Exchange;
  configured: boolean;
  testnet: boolean;
}

export interface FeeRates {
  exchange?: Exchange;
  symbol?: string;
  maker_fee_rate: DecimalValue;
  taker_fee_rate: DecimalValue;
  source?: string;
}

export interface PriceSnapshot {
  last_price: string;
  mark_price: string;
  price_24h_pcnt?: string;
  volume_24h?: string;
}

export interface BalanceSnapshot {
  exchange?: Exchange;
  unit?: "USDT" | "USD";
  source?: string;
  available?: string | number;
  available_balance?: string | number;
  wallet_balance?: string | number;
  equity?: string | number;
  unrealised_pnl?: string | number;
}

export interface GridOrder {
  order_id?: string;
  orderId?: string;
  order_link_id?: string;
  orderLinkId?: string;
  side: "Buy" | "Sell";
  price: string | number;
  qty: string | number;
  status?: string;
  reduce_only?: boolean;
  reduceOnly?: boolean;
  created_time?: string | number;
}

export interface GridTrade {
  order_id?: string;
  trade_id?: string;
  side: "Buy" | "Sell";
  price: string | number;
  qty: string | number;
  volume?: string | number;
  fee?: string | number;
  fee_usdt?: string | number;
  fee_asset?: string;
  fee_quote_asset?: string;
  liquidity?: string;
  is_maker?: boolean;
  realized_pnl?: string | number;
  profit?: string | number;
  time?: string | number;
}

export interface PositionBaseline {
  side?: "Buy" | "Sell";
  qty?: string | number;
}

export interface PositionSnapshot {
  side: "Buy" | "Sell";
  size: string | number;
  entry_price?: string | number;
  mark_price?: string | number;
  unrealised_pnl?: string | number;
  leverage?: string | number;
  liq_price?: string | number;
}

export interface PositionsResponse {
  positions: PositionSnapshot[];
}

export interface OpenOrdersResponse {
  orders?: GridOrder[];
  result?: { list?: GridOrder[] };
}

export interface TradesResponse {
  trades?: GridTrade[];
  result?: { list?: GridTrade[] };
}

export interface RiskSnapshot {
  run_id?: string | null;
  exchange?: Exchange;
  symbol?: string;
  strategy_present?: boolean;
  engine_running?: boolean;
  runtime_advancing?: boolean;
  runtime_configured?: boolean;
  runtime_market_entry_count?: number;
  runtime_run_id?: string | null;
  runtime_state_error?: string | null;
  has_risk?: boolean;
  unmanaged_position?: boolean;
  unmanaged_delta_qty?: string | number | null;
  expected_position_net_qty?: string | number | null;
  actual_position_net_qty?: string | number | null;
  orphan_order_count?: number;
  queued_replacement_count?: number;
  gross_profit?: DecimalValue;
  realized_net_profit?: DecimalValue | null;
  unrealised_pnl?: DecimalValue | null;
  grid_unrealised_pnl?: DecimalValue | null;
  total_equity_profit?: DecimalValue | null;
  total_profit?: DecimalValue | null;
  total_fee?: DecimalValue;
  total_volume?: DecimalValue;
  completed_pairs?: number;
  profit_scope?: "strategy_owned_inventory" | string;
  profit_calculation_error?: string | null;
  [key: string]: unknown;
}

export interface GridHistoryResponse {
  runs: GridHistoryRun[];
}

export interface GridHistoryRun {
  started_at?: string | number;
  symbol?: string;
  exchange?: Exchange;
  direction?: Direction;
  grid_mode?: GridMode;
  initial_order_type?: InitialOrderType;
  initial_order_price?: string | number;
  position_sizing_mode?: PositionSizingMode;
  grid_order_qty?: string | number;
  total_investment?: string | number;
  status?: string;
  total_equity_profit?: string | number;
  net_profit?: string | number;
  total_fee?: string | number;
  total_volume?: string | number;
  completed_pairs?: number;
}

export interface GridPreview {
  exchange?: Exchange;
  symbol?: string;
  reference_price?: DecimalValue;
  grid_step: DecimalValue;
  grid_step_min?: DecimalValue;
  grid_step_max?: DecimalValue;
  grid_profit_pct: DecimalValue;
  grid_profit_pct_min?: DecimalValue;
  grid_profit_pct_max?: DecimalValue;
  per_grid_gross_profit: DecimalValue;
  per_grid_fee: DecimalValue;
  per_grid_open_fee?: DecimalValue;
  per_grid_close_fee?: DecimalValue;
  per_grid_net_profit: DecimalValue;
  per_grid_net_profit_min?: DecimalValue;
  per_grid_net_profit_max?: DecimalValue;
  active_grid_count: number;
  participating_level_count?: number;
  grid_count: number;
  qty_per_grid_min?: DecimalValue;
  qty_per_grid_max?: DecimalValue;
  qty_per_grid_avg: DecimalValue;
  min_notional?: DecimalValue;
  total_qty: DecimalValue;
  maker_fee_rate: DecimalValue;
  taker_fee_rate: DecimalValue;
  fee_rate_source?: string;
  fee_estimate_liquidity?: "maker" | "taker_conservative" | string;
  initial_open_fee_rate?: DecimalValue | null;
  initial_open_fee?: DecimalValue | null;
  opening_order?: GridPlanOrderPreview | null;
  grid_orders?: GridPlanOrderPreview[];
  cycles?: GridCyclePreview[];
}

export interface GridPlanOrderPreview {
  level_index?: number;
  side: "Buy" | "Sell";
  price?: DecimalValue | null;
  qty: DecimalValue;
  reduce_only?: boolean;
  kind?: string;
  time_in_force?: string;
  role?: string;
}

export interface GridCyclePreview {
  level_index: number;
  qty: DecimalValue;
  entry_price: DecimalValue;
  exit_price: DecimalValue;
  gross_profit: DecimalValue;
  open_fee: DecimalValue;
  close_fee: DecimalValue;
  net_profit: DecimalValue;
  gross_profit_pct: DecimalValue;
  fee_rate: DecimalValue;
  liquidity_estimate: string;
}

export interface StrategyCommandResponse {
  ok: boolean;
  message: string;
  run_id: string;
  exchange: Exchange;
  symbol: string;
  lifecycle: string;
}

export interface ApiErrorBody {
  detail?: string | Array<{ msg?: string }>;
  message?: string;
  error?: {
    code?: string;
    message?: string;
  };
}
