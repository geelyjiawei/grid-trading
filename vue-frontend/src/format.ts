import type { Direction, Exchange, GridStatus } from "./api/types";

export function exchangeName(exchange: Exchange): string {
  if (exchange === "binance") return "Binance";
  if (exchange === "aster") return "AsterDEX";
  if (exchange === "bybit") return "Bybit";
  return "TRADE.XYZ";
}

export function quoteAsset(exchange: Exchange): "USDT" | "USDC" {
  return exchange === "trade_xyz" ? "USDC" : "USDT";
}

export function directionName(direction?: Direction): string {
  if (direction === "long") return "做多";
  if (direction === "short") return "做空";
  if (direction === "neutral") return "中性";
  return "--";
}

export function strategyStatusLabel(status?: GridStatus | null): string {
  if (!status) return "未运行";
  if (status.waiting_initial_order || status.lifecycle === "awaiting_opening") return "等待开仓";
  if (status.waiting_trigger || status.lifecycle === "waiting_trigger") return "等待触发";
  if (status.lifecycle === "deploying_grid") return "部署网格中";
  if (status.lifecycle === "stop_requested") return "停止确认中";
  if (status.lifecycle === "risk_exit_requested") return "风险退出中";
  if (status.lifecycle === "failed") return "异常待处理";
  if (["stopped", "closed", "cancelled"].includes(status.lifecycle ?? "")) return "已停止";
  return status.running ? "运行中" : "未运行";
}

export function strategyStatusTone(status?: GridStatus | null): "running" | "pending" | "stopped" {
  if (["stop_requested", "risk_exit_requested", "failed"].includes(status?.lifecycle ?? "")) {
    return "pending";
  }
  return status?.running ? "running" : "stopped";
}

export function strategyCanStop(status?: GridStatus | null): boolean {
  return status?.running === true
    && !["stop_requested", "risk_exit_requested", "failed", "stopped", "closed", "cancelled"]
      .includes(status.lifecycle ?? "");
}

export function finiteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function formatNumber(value: unknown, maximumFractionDigits = 4): string {
  const number = finiteNumber(value);
  if (number === null) return "--";
  return number.toLocaleString("zh-CN", {
    minimumFractionDigits: 0,
    maximumFractionDigits,
  });
}

export function formatExactDecimal(value: unknown): string {
  const text = typeof value === "string"
    ? value.trim()
    : typeof value === "number" && Number.isFinite(value)
      ? String(value)
      : "";
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(text);
  if (!match) return "--";

  const integer = match[2].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  const fraction = match[3] ?? "";
  return `${match[1]}${integer}${fraction ? `.${fraction}` : ""}`;
}

export function formatPercent(value: unknown, fractionDigits = 2): string {
  const number = finiteNumber(value);
  if (number === null) return "--";
  return `${number >= 0 ? "+" : ""}${number.toFixed(fractionDigits)}%`;
}

export function formatTimestamp(value: unknown): string {
  const number = finiteNumber(value);
  if (number === null) return "--";
  const milliseconds = number < 1_000_000_000_000 ? number * 1000 : number;
  const date = new Date(milliseconds);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleString("zh-CN", { hour12: false });
}
