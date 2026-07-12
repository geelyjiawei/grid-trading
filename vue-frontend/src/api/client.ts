import type {
  ApiErrorBody,
  AuthStatus,
  Exchange,
  GridConfigRequest,
  GridStatusList,
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
  return body.message || fallback;
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

export const api = {
  authStatus: () => request<AuthStatus>("/api/auth/status"),
  gridStatus: () => request<GridStatusList>("/api/grid/status"),
  preview: (config: GridConfigRequest) =>
    request<Record<string, unknown>>("/api/grid/preview", {
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
};
