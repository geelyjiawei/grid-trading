import type { Direction, Exchange } from "./api/types";

export function exchangeName(exchange: Exchange): string {
  if (exchange === "binance") return "Binance";
  if (exchange === "aster") return "AsterDEX";
  return "Bybit";
}

export function directionName(direction?: Direction): string {
  if (direction === "long") return "做多";
  if (direction === "short") return "做空";
  if (direction === "neutral") return "中性";
  return "--";
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
  const fraction = (match[3] ?? "").replace(/0+$/, "");
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
