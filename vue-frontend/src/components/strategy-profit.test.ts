import { mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { GridStatus, RiskSnapshot } from "../api/types";
import StrategyList from "./StrategyList.vue";
import StrategyOverview from "./StrategyOverview.vue";

const grid: GridStatus = {
  run_id: "run-net-profit",
  exchange: "binance",
  symbol: "QUSDT",
  running: true,
  gross_profit: "100",
  total_fee: "5",
  realized_net_profit: "95",
  total_profit: "95",
};
const risk: RiskSnapshot = {
  run_id: grid.run_id,
  exchange: grid.exchange,
  symbol: grid.symbol,
  gross_profit: "100",
  total_fee: "5",
  realized_net_profit: "95",
  grid_unrealised_pnl: "-120",
  funding_status: "verified",
  funding_net_income: "0",
  total_equity_profit: "-25",
  profit_scope: "strategy_owned_inventory",
  fee_valuation_pending_count: 0,
};

function list(snapshot: RiskSnapshot | null, selected = true) {
  return mount(StrategyList, {
    props: {
      grids: [grid],
      activeExchange: selected ? "binance" : "aster",
      activeSymbol: selected ? "QUSDT" : "OTHERUSDT",
      loading: false,
      currentRisk: snapshot,
      risks: snapshot ? [snapshot] : [],
    },
  });
}

afterEach(() => vi.useRealTimers());

describe("explicitly historical profit display", () => {
  const now = 1_800_000_000_000;
  function historical(overrides: Record<string, unknown> = {}): RiskSnapshot {
    return { ...risk, funding_net_income: "3", total_equity_profit: "-22",
      profit_data_status: "stale", profit_observed_at_ms: now - 30_000,
      profit_refresh_error: "risk collection busy (429)", ...overrides };
  }

  it("keeps current risk warnings and actual settled fees independent of missing funding", () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const snapshot: RiskSnapshot = { ...risk, funding_net_income: null, total_fee: "7",
      funding_status: "unavailable", profit_data_status: "unavailable" as const,
      profit_observed_at_ms: null, risk_observed_at_ms: now,
      observation_complete: true, has_risk: true, unmanaged_position: true,
      actual_position_net_qty: "5", unmanaged_delta_qty: "5" };
    const overview = mount(StrategyOverview, { props: { status: grid, risk: snapshot } });
    expect(overview.get(".metric-primary strong").text()).toBe("待核对");
    expect(overview.text()).toContain("风险核对未通过");
    expect(overview.text()).toContain("手续费7 USDT");
    expect(overview.text()).toContain("未归属差额 多 5 Q");
    expect(overview.get('[data-testid="settled-funding"] strong').text()).toBe("待核对");
  });

  it.each(["stale", "refreshing"])("labels %s profit, funding and fees with the original timestamp in both views", state => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const snapshot = historical({ profit_data_status: state });
    const card = list(snapshot);
    const overview = mount(StrategyOverview, { props: { status: grid, risk: snapshot } });
    for (const view of [card, overview]) {
      expect(view.get(view === card ? ".strategy-profit strong" : ".metric-primary strong").text()).toBe("-22 USDT");
      expect(view.get('[data-testid="profit-freshness"]').text()).toContain("上次核对");
      expect(view.get('[data-testid="profit-freshness"]').text()).toContain("非当前收益");
      expect(view.get("time").attributes("datetime")).toBe("2027-01-15T07:59:30.000Z");
    }
    expect(overview.get('[data-testid="settled-funding"] strong').text()).toBe("3 USDT");
    expect(overview.text()).toContain("手续费5 USDT");
  });

  it.each([
    { run_id: "wrong-run" }, { exchange: "aster" }, { symbol: "OTHERUSDT" },
    { profit_observed_at_ms: now - 120_001 }, { profit_observed_at_ms: null },
    { profit_observed_at_ms: now + 1 }, { profit_observed_at_ms: 0 },
    { funding_status: "unavailable" }, { funding_net_income: undefined },
    { funding_net_income: "" }, { fee_valuation_pending_count: 1 },
    { fee_valuation_pending_count: undefined }, { total_fee: undefined },
    { total_fee: "NaN" }, { profit_scope: undefined },
    { profit_data_status: "unavailable" }, { profit_data_status: "unknown" },
  ])("withholds historical numbers for unsafe data %o", override => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const snapshot = historical(override);
    const card = list(snapshot);
    const overview = mount(StrategyOverview, { props: { status: grid, risk: snapshot } });
    expect(card.get(".strategy-profit strong").text()).toBe("待核对");
    expect(overview.get(".metric-primary strong").text()).toBe("待核对");
    expect(card.get('[data-testid="profit-freshness"]').text()).toMatch(/[\u4e00-\u9fff]/);
    expect(overview.get('[data-testid="profit-freshness"]').text()).toBe(card.get('[data-testid="profit-freshness"]').text());
  });

  it("treats a verified but old backend observation as historical, never newly received current data", () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const card = list(historical({ profit_data_status: "verified", profit_observed_at_ms: now - 90_000 }));
    expect(card.get(".strategy-profit strong").text()).toBe("-22 USDT");
    expect(card.get('[data-testid="profit-freshness"]').text()).toContain("上次核对");
  });
});

describe("strategy-owned net equity profit", () => {
  it("shows the same net loss in the strategy card and overview without charging fees twice", () => {
    const card = list(risk);
    const overview = mount(StrategyOverview, { props: { status: grid, risk } });
    expect(card.get(".strategy-profit strong").text()).toBe("-25 USDT");
    expect(card.get(".strategy-profit strong").classes()).toContain("negative");
    expect(card.text()).toContain("净收益（含浮盈亏及资金费）");
    expect(overview.get(".metric-primary strong").text()).toBe("-25 USDT");
    expect(overview.text()).toContain("－成交手续费＋已结算资金费");
    expect(overview.text()).toContain("未来平仓手续费尚未发生");
  });

  it("shows independently matched profits for an unselected strategy", () => {
    expect(list(risk, false).get(".strategy-profit strong").text()).toBe("-25 USDT");
  });

  it("keeps zero net equity instead of falling back to positive realized profit", () => {
    const card = list({ ...risk, total_equity_profit: "0" });
    expect(card.get(".strategy-profit strong").text()).toBe("0 USDT");
  });

  it.each([
    ["missing", null],
    ["previous run", { ...risk, run_id: "previous-run" }],
    ["other exchange", { ...risk, exchange: "bybit" as const }],
    ["other symbol", { ...risk, symbol: "OTHERUSDT" }],
    ["pending fees", { ...risk, fee_valuation_pending_count: 1 }],
    ["profit error", { ...risk, profit_calculation_error: "strategy_profit_unavailable" }],
    ["account scope", { ...risk, profit_scope: "whole_account" }],
    ["unknown net", { ...risk, total_equity_profit: null }],
    ["invalid net", { ...risk, total_equity_profit: "NaN" }],
    ["blank net", { ...risk, total_equity_profit: " " }],
    ["incomplete snapshot", { ...risk, observation_complete: false }],
  ])("does not substitute realized profit for %s equity", (_label, snapshot) => {
    const card = list(snapshot);
    expect(card.get(".strategy-profit strong").text()).toBe("待核对");
    expect(card.get(".strategy-profit strong").classes()).not.toContain("positive");
    expect(card.text()).not.toContain("95");
    const overview = mount(StrategyOverview, { props: { status: grid, risk: snapshot } });
    expect(overview.get(".metric-primary strong").text()).toBe("待核对");
    expect(overview.get(".metric-primary strong").classes()).not.toContain("positive");
  });

  it("does not revive a cached selected-card profit when the current request failed", async () => {
    const card = list(null);
    await card.setProps({ risks: [risk] });
    expect(card.get(".strategy-profit strong").text()).toBe("待核对");
  });
});
