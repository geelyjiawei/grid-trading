import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App.vue";
import { ApiError, api } from "./api/client";
import type { GridStatus, PriceSnapshot, RiskSnapshot } from "./api/types";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolver) => {
    resolve = resolver;
  });
  return { promise, resolve };
}

function installWorkspaceMocks(grids: GridStatus[]): void {
  vi.spyOn(api, "authStatus").mockResolvedValue({
    required: false,
    configured: true,
    authenticated: true,
  });
  vi.spyOn(api, "config").mockResolvedValue({
    configured: true,
    active_exchange: grids[0]?.exchange ?? "aster",
    configs: {
      aster: { exchange: "aster", configured: true },
      binance: { exchange: "binance", configured: true },
      bybit: { exchange: "bybit", configured: true },
    },
  });
  vi.spyOn(api, "gridStatus").mockResolvedValue({
    running: grids.length > 0,
    count: grids.length,
    trading_enabled: false,
    grids,
  });
  vi.spyOn(api, "balance").mockImplementation(async (exchange) => ({
    exchange,
    available: exchange === "binance" ? "200" : "100",
    equity: exchange === "binance" ? "220" : "110",
    unit: "USDT",
  }));
  vi.spyOn(api, "feeRates").mockImplementation(async (exchange, symbol) => ({
    exchange,
    symbol,
    maker_fee_rate: "0.0002",
    taker_fee_rate: "0.0005",
  }));
  vi.spyOn(api, "positions").mockResolvedValue({ positions: [] });
  vi.spyOn(api, "openOrders").mockResolvedValue({ orders: [] });
  vi.spyOn(api, "trades").mockResolvedValue({ trades: [] });
  vi.spyOn(api, "history").mockResolvedValue({ runs: [] });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("workspace request isolation", () => {
  it("keeps a stop-pending strategy visible until runtime accounting finishes", async () => {
    installWorkspaceMocks([{
      run_id: "run-esports-stop",
      exchange: "binance",
      symbol: "ESPORTSUSDT",
      running: false,
      lifecycle: "stop_requested",
      manual_stop_pending: true,
      realized_net_profit: "1",
    }]);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "0.42", mark_price: "0.42" });
    vi.spyOn(api, "risk").mockRejectedValue(new Error("strategy accounting pending"));

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.findAll("button.strategy-row")).toHaveLength(1);
    });

    expect(wrapper.find("button.strategy-row").text()).toContain("ESPORTSUSDT");
    expect(wrapper.find("button.strategy-row").text()).toContain("停止确认中");
    await wrapper.find("button.strategy-row").trigger("click");
    await flushPromises();
    expect(wrapper.find(".strategy-overview").text()).toContain("停止请求已保存");
    expect(wrapper.find("button.stop-button").exists()).toBe(false);
    wrapper.unmount();
  });

  it("ignores a previous exchange response that arrives after strategy switching", async () => {
    const grids: GridStatus[] = [
      {
        run_id: "run-aster-old",
        exchange: "aster",
        symbol: "BTCUSDT",
        running: true,
        realized_net_profit: "1",
      },
      {
        run_id: "run-binance-current",
        exchange: "binance",
        symbol: "MUUSDT",
        running: true,
        realized_net_profit: "1",
      },
    ];
    installWorkspaceMocks(grids);
    const oldPrice = deferred<PriceSnapshot>();
    const oldRisk = deferred<RiskSnapshot>();
    vi.spyOn(api, "price").mockImplementation((exchange) => (
      exchange === "aster"
        ? oldPrice.promise
        : Promise.resolve({ last_price: "123.45", mark_price: "123.40" })
    ));
    vi.spyOn(api, "risk").mockImplementation((exchange, symbol) => (
      exchange === "aster"
        ? oldRisk.promise
        : Promise.resolve({
          run_id: "run-binance-current",
          exchange,
          symbol,
          realized_net_profit: "1",
          grid_unrealised_pnl: "2",
          total_equity_profit: "3",
          has_risk: false,
        })
    ));

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.findAll("button.strategy-row")).toHaveLength(2);
    });
    const binanceStrategy = wrapper
      .findAll("button.strategy-row")
      .find((button) => button.text().includes("MUUSDT"));
    expect(binanceStrategy).toBeDefined();
    await binanceStrategy!.trigger("click");
    await flushPromises();

    expect(wrapper.find(".market-card").text()).toContain("Binance · MUUSDT");
    expect(wrapper.find(".market-card").text()).toContain("123.45");
    expect(wrapper.find(".strategy-overview").text()).toContain("总权益利润3 USDT");

    oldPrice.resolve({ last_price: "999", mark_price: "999" });
    oldRisk.resolve({
      run_id: "run-aster-old",
      exchange: "aster",
      symbol: "BTCUSDT",
      total_equity_profit: "999",
      grid_unrealised_pnl: "998",
      has_risk: true,
    });
    await flushPromises();

    expect(wrapper.find(".market-card").text()).toContain("123.45");
    expect(wrapper.find(".strategy-overview").text()).toContain("总权益利润3 USDT");
    expect(wrapper.find(".strategy-overview").text()).not.toContain("999");
    wrapper.unmount();
  });

  it("clears a previous risk snapshot when the current refresh fails", async () => {
    const grid: GridStatus = {
      run_id: "run-risk-current",
      exchange: "binance",
      symbol: "BTCUSDT",
      running: true,
      realized_net_profit: "1",
    };
    installWorkspaceMocks([grid]);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    vi.spyOn(api, "risk")
      .mockResolvedValueOnce({
        run_id: "run-risk-current",
        exchange: "binance",
        symbol: "BTCUSDT",
        realized_net_profit: "1",
        grid_unrealised_pnl: "986",
        total_equity_profit: "987",
        has_risk: false,
      })
      .mockRejectedValueOnce(new Error("risk snapshot unavailable"));

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.find(".strategy-overview").text()).toContain("总权益利润987 USDT");
    });
    const refreshButton = wrapper
      .findAll("button")
      .find((button) => button.text() === "立即刷新");
    expect(refreshButton).toBeDefined();
    await refreshButton!.trigger("click");
    await flushPromises();

    await vi.waitFor(() => {
      expect(wrapper.find(".strategy-overview").text()).toContain("总权益利润等待当前策略");
    });
    expect(wrapper.find(".strategy-overview").text()).not.toContain("987");
    wrapper.unmount();
  });

  it("keeps the last successful detail snapshot and never renders a failed read as zero", async () => {
    const grid: GridStatus = {
      run_id: "run-detail-current",
      exchange: "binance",
      symbol: "ESPORTSUSDT",
      running: true,
      realized_net_profit: "1",
    };
    installWorkspaceMocks([grid]);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "0.42", mark_price: "0.42" });
    vi.spyOn(api, "risk").mockResolvedValue({
      run_id: grid.run_id,
      exchange: grid.exchange,
      symbol: grid.symbol,
      has_risk: false,
    });
    vi.mocked(api.positions)
      .mockResolvedValueOnce({
        positions: [{ side: "Sell", size: "125", entry_price: "0.43" }],
      })
      .mockRejectedValueOnce(new ApiError(
        "Binance 请求频率受限，服务器正在冷却；实时快照暂不可用，不能将持仓或挂单视为 0",
        503,
      ));
    vi.mocked(api.openOrders)
      .mockResolvedValueOnce({
        orders: [{
          order_id: "42",
          side: "Buy",
          price: "0.41",
          qty: "125",
          status: "NEW",
          reduce_only: true,
        }],
      })
      .mockRejectedValueOnce(new ApiError(
        "Binance 请求频率受限，服务器正在冷却；实时快照暂不可用，不能将持仓或挂单视为 0",
        503,
      ));

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.find(".detail-tabs").text()).toContain("持仓 1");
      expect(wrapper.find(".detail-tabs").text()).toContain("挂单 1");
    });
    await wrapper.find(".detail-header button").trigger("click");
    await flushPromises();

    expect(wrapper.find(".detail-tabs").text()).toContain("持仓 --");
    expect(wrapper.find(".detail-tabs").text()).toContain("挂单 --");
    expect(wrapper.find(".detail-panel").text()).toContain("不能将持仓或挂单视为 0");
    expect(wrapper.find(".detail-panel").text()).toContain("保留上次成功快照");
    expect(wrapper.find(".position-item").text()).toContain("125");
    wrapper.unmount();
  });
});
