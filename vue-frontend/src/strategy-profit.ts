import type { DecimalValue, GridStatus, RiskSnapshot } from "./api/types";
import { finiteNumber } from "./format";

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
  const risk = matchingStrategyRisk(status, snapshot);
  if (!risk || risk.profit_calculation_error || risk.observation_complete === false
    || strategyFundingIncome(status, risk) === null
    || (risk.profit_scope != null && risk.profit_scope !== "strategy_owned_inventory")
    || (risk.fee_valuation_pending_count != null && risk.fee_valuation_pending_count !== 0)) {
    return null;
  }
  // Rust already includes settled funding and deducts fees for this run's inventory.
  return validDecimal(risk.total_equity_profit);
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
