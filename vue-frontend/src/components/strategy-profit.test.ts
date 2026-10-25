import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
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
