import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App.vue";
import "../test-support/dialog-mock";
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
      trade_xyz: { exchange: "trade_xyz", configured: true },
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
    unit: exchange === "trade_xyz" ? "USDC" : "USDT",
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
  vi.useRealTimers();
  document.documentElement.dataset.theme = "dark";
  window.localStorage.removeItem("grid-console-theme");
});

describe("theme control", () => {
  it("switches between dark and light themes", async () => {
    document.documentElement.dataset.theme = "dark";
    installWorkspaceMocks([]);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    vi.spyOn(api, "risk").mockResolvedValue({
      exchange: "aster",
      symbol: "BTCUSDT",
      has_risk: false,
    });

    const wrapper = mount(App);
    const themeButton = wrapper.get('button[aria-label="切换到浅色模式"]');
    await themeButton.trigger("click");

    expect(document.documentElement.dataset.theme).toBe("light");
    expect(themeButton.attributes("aria-label")).toBe("切换到深色模式");
    expect(themeButton.attributes("aria-pressed")).toBe("true");
    wrapper.unmount();
  });
});

describe("workspace request isolation", () => {
  it.each(["run", "exchange", "symbol"])("clears cached unselected profit after a mismatched %s response", async (field) => {
    vi.useFakeTimers();
    const grids: GridStatus[] = [
      { run_id: "identity-current", exchange: "binance", symbol: "BTCUSDT", running: true },
      { run_id: "identity-other", exchange: "aster", symbol: "ANSEMUSDT", running: true },
    ];
    installWorkspaceMocks(grids);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    let otherReads = 0;
    vi.spyOn(api, "risk").mockImplementation(async (exchange, symbol) => {
      const response: RiskSnapshot = {
        run_id: grids.find(grid => grid.exchange === exchange && grid.symbol === symbol)?.run_id,
        exchange, symbol, total_equity_profit: "-3",
        funding_status: "verified", funding_net_income: "0",
      };
      if (exchange === "aster" && ++otherReads > 1) {
        if (field === "run") response.run_id = "previous-run";
        if (field === "exchange") response.exchange = "bybit";
        if (field === "symbol") response.symbol = "OTHERUSDT";
      }
      return response;
    });
    const wrapper = mount(App);
    await flushPromises();
    await flushPromises();
    expect(wrapper.findAll(".strategy-profit strong")[1]!.text()).toBe("-3 USDT");
    await vi.advanceTimersByTimeAsync(33_000);
    expect(otherReads).toBe(2);
    expect(wrapper.findAll(".strategy-profit strong")[1]!.text()).toBe("待核对");
    wrapper.unmount();
  });

  it("serializes background profit reads and reuses one when that strategy is selected", async () => {
    const grids: GridStatus[] = [
      { run_id: "serial-current", exchange: "binance", symbol: "BTCUSDT", running: true },
      { run_id: "serial-aster", exchange: "aster", symbol: "ANSEMUSDT", running: true },
      { run_id: "serial-bybit", exchange: "bybit", symbol: "QUSDT", running: true },
    ];
    installWorkspaceMocks(grids);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    const waiting = deferred<RiskSnapshot>();
    const riskSpy = vi.spyOn(api, "risk").mockImplementation((exchange, symbol) => exchange === "aster"
      ? waiting.promise : Promise.resolve({
        run_id: grids.find(grid => grid.exchange === exchange && grid.symbol === symbol)?.run_id,
        exchange, symbol, total_equity_profit: "-1",
        funding_status: "verified", funding_net_income: "0",
      }));
    const wrapper = mount(App);
    await vi.waitFor(() => expect(riskSpy).toHaveBeenCalledWith("aster", "ANSEMUSDT"));
    expect(riskSpy).not.toHaveBeenCalledWith("bybit", "QUSDT");
    await wrapper.findAll("button.strategy-row")[1]!.trigger("click");
    expect(riskSpy.mock.calls.filter(([exchange]) => exchange === "aster")).toHaveLength(1);
    waiting.resolve({ run_id: "serial-aster", exchange: "aster", symbol: "ANSEMUSDT", total_equity_profit: "-2", funding_status: "verified", funding_net_income: "0" });
    await vi.waitFor(() => expect(riskSpy).toHaveBeenCalledWith("bybit", "QUSDT"));
    expect(wrapper.get(".metric-primary strong").text()).toBe("-2 USDT");
    expect(wrapper.findAll(".strategy-profit strong")[1]!.text()).toBe("-2 USDT");
    wrapper.unmount();
  });

  it("expires both selected and unselected profits if subsequent risk reads never complete", async () => {
    vi.useFakeTimers();
    const grids: GridStatus[] = [
      { run_id: "expiry-current", exchange: "binance", symbol: "BTCUSDT", running: true },
      { run_id: "expiry-other", exchange: "aster", symbol: "ANSEMUSDT", running: true },
    ];
    installWorkspaceMocks(grids);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    const riskSpy = vi.spyOn(api, "risk").mockImplementation(async (exchange, symbol) => ({
      run_id: grids.find(grid => grid.exchange === exchange && grid.symbol === symbol)?.run_id,
      exchange, symbol, total_equity_profit: "-10",
      funding_status: "verified", funding_net_income: "0",
    }));
    const wrapper = mount(App);
    await flushPromises();
    await flushPromises();
    expect(wrapper.findAll(".strategy-profit strong").map(node => node.text())).toEqual(["-10 USDT", "-10 USDT"]);
    riskSpy.mockImplementation(() => new Promise(() => {}));
    await vi.advanceTimersByTimeAsync(63_000);
    expect(wrapper.findAll(".strategy-profit strong").map(node => node.text())).toEqual(["待核对", "待核对"]);
    expect(wrapper.get(".metric-primary strong").text()).toBe("待核对");
    wrapper.unmount();
  });

  it("uses matching net-equity snapshots for every strategy and shares the selected overview value", async () => {
    const grids: GridStatus[] = [
      { run_id: "net-binance", exchange: "binance", symbol: "BTCUSDT", running: true, realized_net_profit: "95" },
      { run_id: "net-xyz", exchange: "trade_xyz", symbol: "MUUSDC", running: true, realized_net_profit: "50" },
    ];
    installWorkspaceMocks(grids);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    vi.spyOn(api, "risk").mockImplementation(async (exchange, symbol) => ({
      run_id: grids.find(grid => grid.exchange === exchange && grid.symbol === symbol)?.run_id,
      exchange, symbol, total_equity_profit: exchange === "binance" ? "-25" : "-7",
      funding_status: "verified", funding_net_income: "0",
      total_fee: "5", realized_net_profit: "95", grid_unrealised_pnl: "-120",
      profit_scope: "strategy_owned_inventory",
    }));
    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.findAll(".strategy-profit strong").map(node => node.text())).toEqual(["-25 USDT", "-7 USDC"]);
    });
    expect(wrapper.get(".metric-primary strong").text()).toBe("-25 USDT");
    expect(api.risk).toHaveBeenCalledTimes(2);
    wrapper.unmount();
  });

  it.each(["unavailable", "missing"] as const)("withdraws selected and background profits when funding becomes %s", async (fundingStatus) => {
    vi.useFakeTimers();
    const grids: GridStatus[] = [
      { run_id: "funding-current", exchange: "binance", symbol: "BTCUSDT", running: true },
      { run_id: "funding-background", exchange: "trade_xyz", symbol: "MUUSDC", running: true },
    ];
    installWorkspaceMocks(grids);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    let fundingFailed = false;
    vi.spyOn(api, "risk").mockImplementation(async (exchange, symbol) => ({
      run_id: grids.find(grid => grid.exchange === exchange && grid.symbol === symbol)?.run_id,
      exchange, symbol, total_equity_profit: "-22", total_fee: "5",
      realized_net_profit: "95", grid_unrealised_pnl: "-120",
      funding_status: fundingFailed ? fundingStatus === "missing" ? undefined : "unavailable" : "verified",
      funding_net_income: fundingFailed ? null : "3",
    }));
    const wrapper = mount(App);
    await flushPromises();
    await flushPromises();
    expect(wrapper.findAll(".strategy-profit strong").map(node => node.text())).toEqual(["-22 USDT", "-22 USDC"]);
    expect(wrapper.get(".metric-primary strong").text()).toBe("-22 USDT");
    fundingFailed = true;
    await vi.advanceTimersByTimeAsync(33_000);
    expect(wrapper.findAll(".strategy-profit strong").map(node => node.text())).toEqual(["待核对", "待核对"]);
    expect(wrapper.get(".metric-primary strong").text()).toBe("待核对");
    expect(wrapper.get('[data-testid="settled-funding"] strong').text()).toBe("待核对");
    wrapper.unmount();
  });

  it("refreshes unselected profits at low frequency rather than with every status or price poll", async () => {
    vi.useFakeTimers();
    const grids: GridStatus[] = [
      { run_id: "net-current", exchange: "binance", symbol: "BTCUSDT", running: true },
      { run_id: "net-other", exchange: "aster", symbol: "ANSEMUSDT", running: true },
    ];
    installWorkspaceMocks(grids);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    const riskSpy = vi.spyOn(api, "risk").mockImplementation(async (exchange, symbol) => ({
      run_id: grids.find(grid => grid.exchange === exchange && grid.symbol === symbol)?.run_id,
      exchange, symbol, total_equity_profit: "-1", profit_scope: "strategy_owned_inventory",
      funding_status: "verified", funding_net_income: "0",
    }));
    const wrapper = mount(App);
    await flushPromises();
    await flushPromises();
    expect(riskSpy.mock.calls.filter(([exchange]) => exchange === "aster")).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(29_000);
    expect(riskSpy.mock.calls.filter(([exchange]) => exchange === "aster")).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(4_000);
    expect(riskSpy.mock.calls.filter(([exchange]) => exchange === "aster")).toHaveLength(2);
    expect(riskSpy.mock.calls.filter(([exchange]) => exchange === "binance")).toHaveLength(2);
    wrapper.unmount();
  });

  it("finishes exchange details before scanning global strategy history", async () => {
    installWorkspaceMocks([]);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "100", mark_price: "100" });
    vi.spyOn(api, "risk").mockResolvedValue({
      exchange: "aster",
      symbol: "BTCUSDT",
      has_risk: false,
    });
    const pendingTrades = deferred<{ trades: [] }>();
    vi.mocked(api.trades).mockReturnValueOnce(pendingTrades.promise);

    const wrapper = mount(App);
    await vi.waitFor(() => expect(api.trades).toHaveBeenCalledTimes(1));
    expect(api.history).not.toHaveBeenCalled();

    pendingTrades.resolve({ trades: [] });
    await vi.waitFor(() => expect(api.history).toHaveBeenCalledTimes(1));
    wrapper.unmount();
  });

  it("keeps exchange-heavy snapshots out of the five-second price poll", async () => {
    vi.useFakeTimers();
    installWorkspaceMocks([{
      run_id: "run-trade-xyz-poll",
      exchange: "trade_xyz",
      symbol: "MUUSDC",
      running: true,
      realized_net_profit: "1",
    }]);
    const priceSpy = vi.spyOn(api, "price").mockResolvedValue({
      last_price: "100",
      mark_price: "100",
    });
    const riskSpy = vi.spyOn(api, "risk").mockResolvedValue({
      exchange: "trade_xyz",
      symbol: "MUUSDC",
      has_risk: false,
    });

    const wrapper = mount(App);
    await flushPromises();
    await flushPromises();
    priceSpy.mockClear();
    riskSpy.mockClear();
    vi.mocked(api.balance).mockClear();
    vi.mocked(api.feeRates).mockClear();
    vi.mocked(api.positions).mockClear();
    vi.mocked(api.openOrders).mockClear();
    vi.mocked(api.trades).mockClear();

    await vi.advanceTimersByTimeAsync(5_000);
    await flushPromises();

    expect(priceSpy).toHaveBeenCalledTimes(1);
    expect(api.balance).not.toHaveBeenCalled();
    expect(api.feeRates).not.toHaveBeenCalled();
    expect(riskSpy).not.toHaveBeenCalled();
    expect(api.positions).not.toHaveBeenCalled();
    expect(api.openOrders).not.toHaveBeenCalled();
    expect(api.trades).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(25_000);
    await flushPromises();

    expect(priceSpy).toHaveBeenCalledTimes(6);
    expect(api.balance).toHaveBeenCalledTimes(1);
    expect(api.feeRates).not.toHaveBeenCalled();
    expect(riskSpy).toHaveBeenCalledTimes(1);
    expect(api.positions).not.toHaveBeenCalled();
    expect(api.openOrders).not.toHaveBeenCalled();
    expect(api.trades).not.toHaveBeenCalled();
    wrapper.unmount();
  });

  it("does not poll an unconfigured preferred exchange", async () => {
    vi.spyOn(api, "authStatus").mockResolvedValue({
      required: false,
      configured: true,
      authenticated: true,
    });
    vi.spyOn(api, "config").mockResolvedValue({
      configured: false,
      active_exchange: "bybit",
      configs: {
        binance: { exchange: "binance", configured: true },
        aster: { exchange: "aster", configured: false },
        bybit: { exchange: "bybit", configured: false },
        trade_xyz: { exchange: "trade_xyz", configured: false },
      },
    });
    vi.spyOn(api, "gridStatus").mockResolvedValue({
      running: false,
      count: 0,
      trading_enabled: true,
      grids: [],
    });
    const priceSpy = vi.spyOn(api, "price").mockResolvedValue({
      last_price: "100",
      mark_price: "100",
    });
    vi.spyOn(api, "balance").mockResolvedValue({
      exchange: "binance",
      available: "100",
      equity: "100",
    });
    vi.spyOn(api, "feeRates").mockResolvedValue({
      exchange: "binance",
      symbol: "BTCUSDT",
      maker_fee_rate: "0.0002",
      taker_fee_rate: "0.0005",
    });
    vi.spyOn(api, "risk").mockResolvedValue({
      exchange: "binance",
      symbol: "BTCUSDT",
      has_risk: false,
    });
    vi.spyOn(api, "positions").mockResolvedValue({ positions: [] });
    vi.spyOn(api, "openOrders").mockResolvedValue({ orders: [] });
    vi.spyOn(api, "trades").mockResolvedValue({ trades: [] });
    vi.spyOn(api, "history").mockResolvedValue({ runs: [] });

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(priceSpy).toHaveBeenCalledWith("binance", "BTCUSDT");
    });
    expect(priceSpy).not.toHaveBeenCalledWith("bybit", expect.any(String));

    const bybitTab = wrapper
      .findAll(".exchange-tabs button")
      .find((button) => button.text().includes("Bybit"));
    expect(bybitTab).toBeDefined();
    await bybitTab!.trigger("click");
    await flushPromises();

    expect(priceSpy).not.toHaveBeenCalledWith("bybit", expect.any(String));
    expect(wrapper.find(".global-error").exists()).toBe(false);
    wrapper.unmount();
  });

  it("renders TRADE.XYZ market data without waiting for a slower risk snapshot", async () => {
    const grid: GridStatus = {
      run_id: "run-trade-xyz",
      exchange: "trade_xyz",
      symbol: "MUUSDC",
      running: true,
      realized_net_profit: "1",
    };
    installWorkspaceMocks([grid]);
    const priceSpy = vi.spyOn(api, "price").mockResolvedValue({
      last_price: "859.155",
      mark_price: "858.77",
      price_24h_pcnt: "0.06244375",
      volume_24h: "81704.125",
    });
    const slowRisk = deferred<RiskSnapshot>();
    const riskSpy = vi.spyOn(api, "risk").mockReturnValue(slowRisk.promise);

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.find(".market-card").text()).toContain("859.155");
      expect(wrapper.find(".market-card").text()).toContain("81,704.13");
    });

    await wrapper.find("button.strategy-row").trigger("click");
    await vi.waitFor(() => expect(priceSpy).toHaveBeenCalledTimes(2));
    expect(riskSpy).toHaveBeenCalledTimes(1);

    slowRisk.resolve({
      run_id: grid.run_id,
      exchange: grid.exchange,
      symbol: grid.symbol,
      has_risk: false,
    });
    await flushPromises();
    wrapper.unmount();
  });

  it("starts a fresh market read when returning to a context with a stale request", async () => {
    const grids: GridStatus[] = [
      {
        run_id: "run-aster-stale",
        exchange: "aster",
        symbol: "BTCUSDT",
        running: true,
        realized_net_profit: "1",
      },
      {
        run_id: "run-binance-switch",
        exchange: "binance",
        symbol: "MUUSDT",
        running: true,
        realized_net_profit: "1",
      },
    ];
    installWorkspaceMocks(grids);
    const stalePrice = deferred<PriceSnapshot>();
    const staleRisk = deferred<RiskSnapshot>();
    let asterPriceCalls = 0;
    let asterRiskCalls = 0;
    vi.spyOn(api, "price").mockImplementation((exchange) => {
      if (exchange !== "aster") {
        return Promise.resolve({ last_price: "123", mark_price: "123" });
      }
      asterPriceCalls += 1;
      return asterPriceCalls === 1
        ? stalePrice.promise
        : Promise.resolve({ last_price: "777", mark_price: "777" });
    });
    vi.spyOn(api, "risk").mockImplementation((exchange, symbol) => {
      if (exchange === "aster") {
        asterRiskCalls += 1;
        if (asterRiskCalls === 1) return staleRisk.promise;
      }
      return Promise.resolve({ exchange, symbol, has_risk: false });
    });

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.findAll("button.strategy-row")).toHaveLength(2);
    });
    const strategyButtons = wrapper.findAll("button.strategy-row");
    await strategyButtons.find((button) => button.text().includes("MUUSDT"))!.trigger("click");
    await flushPromises();
    await strategyButtons.find((button) => button.text().includes("BTCUSDT"))!.trigger("click");

    await vi.waitFor(() => {
      expect(wrapper.find(".market-card").text()).toContain("777");
    });
    expect(asterPriceCalls).toBe(2);

    stalePrice.resolve({ last_price: "999", mark_price: "999" });
    staleRisk.resolve({ exchange: "aster", symbol: "BTCUSDT", has_risk: true });
    await flushPromises();
    expect(wrapper.find(".market-card").text()).toContain("777");
    expect(wrapper.find(".market-card").text()).not.toContain("999");
    wrapper.unmount();
  });

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

  it("refreshes a typed symbol without requiring blur or Enter", async () => {
    const grid: GridStatus = {
      run_id: "run-trade-xyz-input",
      exchange: "trade_xyz",
      symbol: "MUUSDC",
      running: true,
      realized_net_profit: "1",
    };
    installWorkspaceMocks([grid]);
    const priceSpy = vi.spyOn(api, "price").mockImplementation(async (_exchange, requestedSymbol) => ({
      last_price: requestedSymbol === "SKHXUSDC" ? "1163.85" : "859.155",
      mark_price: requestedSymbol === "SKHXUSDC" ? "1163.3" : "858.77",
    }));
    vi.spyOn(api, "risk").mockImplementation(async (exchange, requestedSymbol) => ({
      exchange,
      symbol: requestedSymbol,
      has_risk: false,
    }));

    const wrapper = mount(App);
    await vi.waitFor(() => expect(priceSpy).toHaveBeenCalledWith("trade_xyz", "MUUSDC"));

    const symbolInput = wrapper.get<HTMLInputElement>('input[spellcheck="false"]');
    symbolInput.element.value = "SKHXUSDC";
    await symbolInput.trigger("input");

    await vi.waitFor(() => {
      expect(priceSpy).toHaveBeenCalledWith("trade_xyz", "SKHXUSDC");
      expect(wrapper.find(".market-card").text()).toContain("1,163.85");
    });
    wrapper.unmount();
  });

  it("submits cleanup for a selected failed strategy", async () => {
    const failed: GridStatus = {
      run_id: "run-skhynix-failed",
      exchange: "binance",
      symbol: "SKHYNIXUSDT",
      running: false,
      lifecycle: "failed",
      realized_net_profit: "15.7876",
    };
    installWorkspaceMocks([failed]);
    vi.spyOn(api, "price").mockResolvedValue({ last_price: "1004", mark_price: "1004" });
    vi.spyOn(api, "risk").mockResolvedValue({
      run_id: failed.run_id,
      exchange: failed.exchange,
      symbol: failed.symbol,
      has_risk: false,
    });
    const stop = vi.spyOn(api, "stop").mockResolvedValue({
      ok: true,
      message: "cleanup already in progress",
      run_id: "run-skhynix-failed",
      exchange: failed.exchange,
      symbol: failed.symbol,
      lifecycle: "unchanged",
    });

    const wrapper = mount(App);
    await vi.waitFor(() => {
      expect(wrapper.findAll("button.strategy-row")).toHaveLength(1);
    });
    await wrapper.find("button.strategy-row").trigger("click");
    await flushPromises();

    const stopButton = wrapper.get("button.stop-button");
    expect(stopButton.text()).toContain("清理并停止");
    await stopButton.trigger("click");
    await stopButton.trigger("click");
    await vi.waitFor(() => {
      expect(stop).toHaveBeenCalledWith("binance", "SKHYNIXUSDT");
    });
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
          funding_status: "verified", funding_net_income: "0",
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
      funding_status: "verified", funding_net_income: "0",
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
        funding_status: "verified", funding_net_income: "0",
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
    expect(wrapper.get(".strategy-profit strong").text()).toBe("待核对");
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
      expect(api.history).toHaveBeenCalled();
    });
    await wrapper.get('[aria-label="查看持仓"]').trigger('click');
    await vi.waitFor(() => {
      expect(wrapper.find(".detail-tabs").text()).toContain("持仓 1");
      expect(wrapper.find(".detail-tabs").text()).toContain("挂单 1");
    });
    await wrapper.find(".records-toolbar > button").trigger("click");
    await flushPromises();

    expect(wrapper.find(".detail-tabs").text()).toContain("持仓 --");
    expect(wrapper.find(".detail-tabs").text()).toContain("挂单 --");
    expect(wrapper.find(".bot-records").text()).toContain("不能将持仓或挂单视为 0");
    expect(wrapper.find(".bot-records").text()).toContain("保留上次成功快照");
    await wrapper.findAll('.detail-tabs button').find(button => button.text().startsWith('持仓'))!.trigger('click');
    expect(wrapper.find(".position-item").text()).toContain("125");
    wrapper.unmount();
  });
});
