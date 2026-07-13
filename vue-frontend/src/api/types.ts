export type Exchange = "binance" | "aster" | "bybit";
export type Direction = "long" | "short" | "neutral";
export type GridMode = "arithmetic" | "geometric";
export type PositionSizingMode = "investment" | "fixed_grid_qty";
export type InitialOrderType = "market" | "limit" | "post_only";

export interface GridConfigRequest {
  exchange?: Exchange;
  symbol: string;
  direction: Direction;
  upper_price: number;
  lower_price: number;
  grid_count: number;
  total_investment?: number;
  leverage?: number;
  position_sizing_mode?: PositionSizingMode;
  grid_order_qty?: number | null;
  initial_order_type?: InitialOrderType;
  initial_order_price?: number | null;
  grid_order_post_only?: boolean;
  grid_mode?: GridMode;
  trigger_price?: number | null;
  stop_loss_price?: number | null;
  take_profit_price?: number | null;
  maker_fee_rate?: number;
  taker_fee_rate?: number;
  fee_rate?: number;
}

export interface AuthStatus {
  required: boolean;
  configured: boolean;
  authenticated: boolean;
  username?: string;
  totp_secret?: string;
}

export interface GridStatus {
  exchange: Exchange;
  symbol: string;
  running: boolean;
  direction?: Direction;
  grid_mode?: GridMode;
  total_profit?: number;
  completed_pairs?: number;
  trigger_message?: string;
  waiting_initial_order?: boolean;
  waiting_trigger?: boolean;
  total_equity_profit?: number;
  gross_profit?: number;
  total_fee?: number;
  realized_net_profit?: number;
  unrealised_pnl?: number;
  total_volume?: number;
  current_price?: number;
  active_orders?: GridOrder[];
  filled_orders?: GridTrade[];
  baseline_position?: PositionBaseline;
  grid_position_net_qty?: number;
  grid_profit_pct?: number;
  initial_side?: "Buy" | "Sell";
  initial_qty?: number;
  [key: string]: unknown;
}

export interface GridStatusList {
  running: boolean;
  count: number;
  running_count?: number;
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

export interface SaveApiConfigRequest {
  exchange: Exchange;
  api_key: string;
  api_secret: string;
  testnet: boolean;
}

export interface FeeRates {
  exchange?: Exchange;
  symbol?: string;
  maker_fee_rate: number;
  taker_fee_rate: number;
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
  has_risk?: boolean;
  unmanaged_position?: boolean;
  unmanaged_delta_qty?: number;
  orphan_order_count?: number;
  queued_replacement_count?: number;
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
  grid_step: number;
  grid_profit_pct: number;
  per_grid_gross_profit: number;
  per_grid_fee: number;
  per_grid_open_fee?: number;
  per_grid_close_fee?: number;
  per_grid_net_profit: number;
  active_grid_count: number;
  grid_count: number;
  qty_per_grid_min?: number;
  qty_per_grid_max?: number;
  qty_per_grid_avg: number;
  min_notional?: number;
  total_qty: number;
  maker_fee_rate: number;
  taker_fee_rate: number;
  fee_rate_source?: string;
}

export interface ApiErrorBody {
  detail?: string | Array<{ msg?: string }>;
  message?: string;
  error?: {
    code?: string;
    message?: string;
  };
}
