import type { DecimalValue, GridStatus, RiskSnapshot } from "./api/types";
import { finiteNumber } from "./format";

export const PROFIT_SNAPSHOT_MAX_AGE_MS = 60_000;
export const PRIOR_PROFIT_DISPLAY_MAX_AGE_MS = 120_000;

export function matchingStrategyRisk(
  status: GridStatus | null,
  risk: RiskSnapshot | null | undefined,
): RiskSnapshot | null {
  return status?.run_id && risk?.run_id === status.run_id
    && risk.exchange === status.exchange && risk.symbol === status.symbol
    ? risk : null;
}

export function strategyNetProfit(
  status: GridStatus | null,
  snapshot: RiskSnapshot | null | undefined,
): DecimalValue | null {
  const risk = currentStrategyRisk(status, snapshot);
  if (!risk || profitValidationReason(status, risk)) return null;
  // Rust already includes settled funding and deducts fees for this run's inventory.
  return validDecimal(risk.total_equity_profit);
}

export interface StrategyProfitDisplay {
  value: DecimalValue | null;
  state: "verified" | "stale" | "unavailable";
  observedAtMs: number | null;
  reason: string;
}

function observationTime(risk: RiskSnapshot, now: number): number | null {
  const value = risk.profit_observed_at_ms;
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 && value <= now
    ? value : null;
}

export function currentStrategyRisk(
  status: GridStatus | null,
  snapshot: RiskSnapshot | null | undefined,
  now = Date.now(),
): RiskSnapshot | null {
  const risk = matchingStrategyRisk(status, snapshot);
  if (!risk || risk.profit_data_status === undefined) return risk;
  const observedAt = observationTime(risk, now);
  return risk.profit_data_status === "verified" && observedAt !== null && now - observedAt <= PROFIT_SNAPSHOT_MAX_AGE_MS
    ? risk : null;
}

export function currentStrategyObservation(
  status: GridStatus | null,
  snapshot: RiskSnapshot | null | undefined,
  now = Date.now(),
): RiskSnapshot | null {
  const risk = matchingStrategyRisk(status, snapshot);
  if (!risk) return null;
  if (risk.profit_data_status === undefined && risk.risk_observed_at_ms === undefined) return risk;
  // Missing funding invalidates profit, not independently verified position evidence.
  if (risk.profit_data_status === "stale" || risk.profit_data_status === "refreshing"
    || risk.observation_complete !== true) return null;
  const observedAt = risk.risk_observed_at_ms;
  return typeof observedAt === "number" && Number.isSafeInteger(observedAt) && observedAt > 0
    && observedAt <= now && now - observedAt <= PROFIT_SNAPSHOT_MAX_AGE_MS ? risk : null;
}

function refreshReason(error: string | null | undefined): string {
  if (/429|busy|繁忙/i.test(error ?? "")) return "查询暂时繁忙，等待重试";
  if (/funding|资金费/i.test(error ?? "")) return "已结算资金费核对暂不可用，等待重试";
  if (/timeout|timed out|超时/i.test(error ?? "")) return "收益查询超时，等待重试";
  return "收益核对暂不可用，等待重试";
}

function profitValidationReason(status: GridStatus | null, risk: RiskSnapshot): string {
  if (strategyFundingIncome(status, risk) === null) return "已结算资金费尚未核实，不按零计算";
  if (risk.profit_calculation_error) return refreshReason(risk.profit_calculation_error);
  if (risk.observation_complete === false) return "收益快照不完整，等待权威核对";
  const metadata = risk.profit_data_status !== undefined;
  if ((metadata || risk.profit_scope != null) && risk.profit_scope !== "strategy_owned_inventory") {
    return "收益归属范围尚未核实";
  }
  if (((metadata || risk.fee_valuation_pending_count != null) && risk.fee_valuation_pending_count !== 0)
    || (metadata && validDecimal(risk.total_fee) === null)) return "成交手续费尚未结算核实";
  return validDecimal(risk.total_equity_profit) === null ? "净收益金额缺失或无效，不按零计算" : "";
}

// Display-only: historical values must never enter canonical net/protection calculations.
export function strategyProfitDisplay(
  status: GridStatus | null,
  snapshot: RiskSnapshot | null | undefined,
  now = Date.now(),
): StrategyProfitDisplay {
  const unavailable = (reason: string): StrategyProfitDisplay => ({
    value: null, state: "unavailable", observedAtMs: null, reason,
  });
  const risk = matchingStrategyRisk(status, snapshot);
  if (!risk) return unavailable("等待本轮策略的权威收益快照");
  const invalid = profitValidationReason(status, risk);
  if (invalid) return unavailable(invalid);
  const state = risk.profit_data_status;
  if (state === undefined) {
    return { value: validDecimal(risk.total_equity_profit), state: "verified", observedAtMs: null, reason: "" };
  }
  if (state !== "verified" && state !== "stale" && state !== "refreshing") {
    return unavailable(refreshReason(risk.profit_refresh_error));
  }
  const observedAt = observationTime(risk, now);
  if (observedAt === null) return unavailable("缺少有效的原始核对时间，等待重新核实");
  if (now - observedAt > PRIOR_PROFIT_DISPLAY_MAX_AGE_MS) return unavailable("上次核对已超过120秒，等待重新核实");
  const stale = state !== "verified" || now - observedAt > PROFIT_SNAPSHOT_MAX_AGE_MS;
  return {
    value: validDecimal(risk.total_equity_profit), state: stale ? "stale" : "verified",
    observedAtMs: observedAt,
    reason: stale ? refreshReason(risk.profit_refresh_error) : "",
  };
}

export function strategyFundingIncome(
  status: GridStatus | null,
  snapshot: RiskSnapshot | null | undefined,
): DecimalValue | null {
  const risk = matchingStrategyRisk(status, snapshot);
  return risk?.funding_status === "verified" ? validDecimal(risk.funding_net_income) : null;
}

function validDecimal(value: unknown): DecimalValue | null {
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && !/^[+-]?\d+(?:\.\d+)?$/.test(value)) return null;
  return finiteNumber(value) === null ? null : value;
}

export function profitTone(value: DecimalValue | null): "positive" | "negative" | undefined {
  const number = finiteNumber(value);
  return number === null ? undefined : number < 0 ? "negative" : "positive";
}
