import type {
  ApiErrorBody,
  ApiConfigResponse,
  BalanceSnapshot,
  AuthStatus,
  Exchange,
  FeeRates,
  GridConfigRequest,
  GridHistoryResponse,
  GridPreview,
  GridStatusList,
  LoginRequest,
  OpenOrdersResponse,
  PositionsResponse,
  PriceSnapshot,
  RiskSnapshot,
  SaveApiConfigRequest,
  TradesResponse,
} from "./types";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

function errorMessage(body: ApiErrorBody, fallback: string): string {
  if (typeof body.detail === "string") return body.detail;
  if (Array.isArray(body.detail)) {
    const joined = body.detail.map((item) => item.msg).filter(Boolean).join("; ");
    if (joined) return joined;
  }
  return body.error?.message || body.message || fallback;
}

export async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, {
    ...init,
    headers,
    credentials: "same-origin",
  });
  const body = (await response.json().catch(() => ({}))) as T | ApiErrorBody;
  if (!response.ok) {
    throw new ApiError(
      errorMessage(body as ApiErrorBody, `Request failed (${response.status})`),
      response.status,
    );
  }
  return body as T;
}

export function withExchange(path: string, exchange: Exchange): string {
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}exchange=${encodeURIComponent(exchange)}`;
}

export const api = {
  authStatus: () => request<AuthStatus>("/api/auth/status"),
  login: (credentials: LoginRequest) =>
    request<{ ok?: boolean; message?: string }>("/api/auth/login", {
      method: "POST",
      body: JSON.stringify(credentials),
    }),
  logout: () =>
    request<{ ok?: boolean; message?: string }>("/api/auth/logout", {
      method: "POST",
    }),
  config: () => request<ApiConfigResponse>("/api/config"),
  saveConfig: (config: SaveApiConfigRequest) =>
    request<{ ok?: boolean; message: string }>("/api/config", {
      method: "POST",
      body: JSON.stringify(config),
    }),
  feeRates: (exchange: Exchange, symbol: string) =>
    request<FeeRates>(withExchange(`/api/fees/${encodeURIComponent(symbol)}`, exchange)),
  price: (exchange: Exchange, symbol: string) =>
    request<PriceSnapshot>(withExchange(`/api/price/${encodeURIComponent(symbol)}`, exchange)),
  balance: (exchange: Exchange) =>
    request<BalanceSnapshot>(withExchange("/api/balance", exchange)),
  gridStatus: () => request<GridStatusList>("/api/grid/status"),
  preview: (config: GridConfigRequest) =>
    request<GridPreview>("/api/grid/preview", {
      method: "POST",
      body: JSON.stringify(config),
    }),
  start: (config: GridConfigRequest) =>
    request<{ ok: boolean; message: string }>("/api/grid/start", {
      method: "POST",
      body: JSON.stringify(config),
    }),
  stop: (exchange: Exchange, symbol: string) =>
    request<{ ok: boolean; message: string }>(
      `/api/grid/stop/${encodeURIComponent(symbol)}?exchange=${encodeURIComponent(exchange)}`,
      { method: "POST" },
    ),
  openOrders: (exchange: Exchange, symbol: string) =>
    request<OpenOrdersResponse>(
      withExchange(`/api/orders/open/${encodeURIComponent(symbol)}`, exchange),
    ),
  trades: (exchange: Exchange, symbol: string) =>
    request<TradesResponse>(
      withExchange(`/api/trades/${encodeURIComponent(symbol)}?limit=100`, exchange),
    ),
  positions: (exchange: Exchange, symbol: string) =>
    request<PositionsResponse>(
      withExchange(`/api/positions/${encodeURIComponent(symbol)}`, exchange),
    ),
  risk: (exchange: Exchange, symbol: string) =>
    request<RiskSnapshot>(
      withExchange(`/api/risk/${encodeURIComponent(symbol)}`, exchange),
    ),
  history: () => request<GridHistoryResponse>("/api/grid/history?limit=100"),
};
