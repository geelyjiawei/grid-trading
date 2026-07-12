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
  [key: string]: unknown;
}

export interface GridStatusList {
  running: boolean;
  count: number;
  grids: GridStatus[];
}

export interface ApiErrorBody {
  detail?: string | Array<{ msg?: string }>;
  message?: string;
}
