import { describe, expect, it } from "vitest";
import type { GridStatus, RiskSnapshot } from "./api/types";
import { strategyNetProfit } from "./strategy-profit";

const status: GridStatus = {
  run_id: "funding-run", exchange: "bybit", symbol: "BTCUSDT", running: true,
  realized_net_profit: "95",
};

function risk(overrides: Record<string, unknown> = {}): RiskSnapshot {
  return {
    run_id: "funding-run", exchange: "bybit", symbol: "BTCUSDT",
    gross_profit: "100", total_fee: "5", realized_net_profit: "95",
    grid_unrealised_pnl: "-120", trading_net_equity_profit: "-25",
    total_equity_profit: "-22", profit_scope: "strategy_owned_inventory",
    funding_status: "verified", funding_net_income: "3", funding_payment_count: 2,
    funding_observed_through_ms: 1_700_000_000_000,
    ...overrides,
  } as RiskSnapshot;
}

describe("funding-inclusive strategy net profit", () => {
  it.each([
    ["3", "-22"], ["-3", "-28"], ["0", "-25"], [0, "-25"],
  ])("uses the authoritative total with funding %s without charging fees or funding again", (funding, total) => {
    expect(strategyNetProfit(status, risk({ funding_net_income: funding, total_equity_profit: total }))).toBe(total);
  });

  it.each([undefined, null, "unavailable", "pending", "VERIFIED"])(
    "withholds net profit when funding status is %s", (fundingStatus) => {
      expect(strategyNetProfit(status, risk({ funding_status: fundingStatus }))).toBeNull();
    },
  );

  it.each([undefined, null, "", " ", "NaN", "Infinity", "0x10", "1_000", "1e3", true, [], {}, Number.NaN, Infinity])(
    "withholds net profit for malformed or missing funding income %s", (income) => {
      expect(strategyNetProfit(status, risk({ funding_net_income: income }))).toBeNull();
    },
  );

  it.each([undefined, null, "", " ", "NaN", "Infinity", "0x10", true, [], {}])(
    "does not reconstruct an invalid backend total %s from other metrics", (total) => {
      expect(strategyNetProfit(status, risk({ total_equity_profit: total }))).toBeNull();
    },
  );

  it.each([
    { run_id: "previous-run" }, { exchange: "aster" }, { symbol: "ETHUSDT" },
    { observation_complete: false }, { fee_valuation_pending_count: 1 },
    { profit_calculation_error: "funding unavailable" }, { profit_scope: "account" },
  ])("retains identity and valuation safety for %o", (override) => {
    expect(strategyNetProfit(status, risk(override))).toBeNull();
  });

  it("accepts a verified zero total without a truthiness check", () => {
    expect(strategyNetProfit(status, risk({ total_equity_profit: "0", funding_net_income: "25" }))).toBe("0");
    expect(strategyNetProfit(null, risk())).toBeNull();
    expect(strategyNetProfit(status, null)).toBeNull();
  });

  it("does not confuse an unavailable payment count with an invalid verified funding amount", () => {
    expect(strategyNetProfit(status, risk({
      funding_net_income: "0", funding_payment_count: null, total_equity_profit: "-25",
    }))).toBe("-25");
  });
});
