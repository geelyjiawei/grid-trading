import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api } from "../api/client";
import type { GridStatus, ProfitProtectionStatus } from "../api/types";
import ProfitProtectionPanel from "./ProfitProtectionPanel.vue";

const settings = { revision: 2, take_profit_amount: "20", stop_loss_amount: "10", updated_at_ms: 10 };
const grid: GridStatus = {
  exchange: "binance", symbol: "BTCUSDT", run_id: "run-a", running: true,
  lifecycle: "running", profit_protection: settings,
};
function status(overrides: Partial<ProfitProtectionStatus> = {}): ProfitProtectionStatus {
  return {
    exchange: "binance", symbol: "BTCUSDT", run_id: "run-a", lifecycle: "running",
    profit_protection: { ...settings },
    observation: { status: "monitoring", observed_at_ms: 12, net_profit: "-1.25", error: null },
    trigger: null, ...overrides,
  };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
function setup(selected = grid) {
  vi.spyOn(api, "profitProtection").mockResolvedValue(status());
  vi.spyOn(api, "updateProfitProtection").mockResolvedValue(status({
    profit_protection: { ...settings, revision: 3 },
  }));
  return mount(ProfitProtectionPanel, { props: { status: selected, refreshKey: 0 } });
}
afterEach(() => vi.restoreAllMocks());

describe("runtime net-profit protection", () => {
  it("does not apply a late save or its success message to a replacement run", async () => {
    const wrapper = setup();
    await flushPromises();
    const waiting = deferred<ProfitProtectionStatus>();
    vi.mocked(api.updateProfitProtection).mockReturnValue(waiting.promise);
    await wrapper.get("form").trigger("submit");
    await wrapper.setProps({ status: { ...grid, run_id: "replacement", profit_protection: undefined } });
    waiting.resolve(status({ profit_protection: { ...settings, revision: 3 } }));
    await flushPromises();
    expect(wrapper.text()).not.toContain("已保存");
    expect(wrapper.text()).toContain("版本 0");
    expect(wrapper.get('[data-testid="take-profit-amount"]').element).toHaveProperty("value", "");
    wrapper.unmount();
  });

  it.each(["run_id", "exchange", "symbol"] as const)("rejects a mismatched %s response for the current request", async field => {
    const wrapper = setup();
    await flushPromises();
    vi.mocked(api.profitProtection).mockResolvedValue(status({ [field]: field === "exchange" ? "aster" : "other" }));
    await wrapper.setProps({ refreshKey: 1 });
    await flushPromises();
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain("暂不可用");
    expect(wrapper.text()).not.toContain("-1.25 USDT");
    wrapper.unmount();
  });

  it.each(["stop_requested", "risk_exit_requested", "stopped", "closed", "failed"])("does not update limits during %s", async lifecycle => {
    const wrapper = setup({ ...grid, lifecycle });
    await flushPromises();
    await wrapper.get("form").trigger("submit");
    await wrapper.get('[data-testid="protection-disable"]').trigger("click");
    expect(api.updateProfitProtection).not.toHaveBeenCalled();
    wrapper.unmount();
  });

  it.each(["awaiting_opening", "deploying_grid", "running"])("can configure the current run during %s", async lifecycle => {
    const wrapper = setup({ ...grid, lifecycle });
    vi.mocked(api.profitProtection).mockResolvedValue(status({ lifecycle }));
    await flushPromises();
    await wrapper.setProps({ refreshKey: 1 });
    await flushPromises();
    await wrapper.get("form").trigger("submit");
    expect(api.updateProfitProtection).toHaveBeenCalledWith("binance", "BTCUSDT", {
      run_id: "run-a", expected_revision: 2, take_profit_amount: "20", stop_loss_amount: "10",
    });
    wrapper.unmount();
  });

  it("labels verified observations as monitoring", async () => {
    const wrapper = setup();
    await flushPromises();
    vi.mocked(api.profitProtection).mockResolvedValue(status({
      observation: { status: "verified", observed_at_ms: 12, net_profit: "-1.25", error: null },
    }));
    await wrapper.setProps({ refreshKey: 1 });
    await flushPromises();
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain("监控中");
    wrapper.unmount();
  });
  it("monitors only this run without submitting on render and explains owned net profit", async () => {
    const wrapper = setup();
    await flushPromises();
    expect(api.profitProtection).toHaveBeenCalledWith("binance", "BTCUSDT", "run-a");
    expect(api.updateProfitProtection).not.toHaveBeenCalled();
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain("监控中");
    expect(wrapper.text()).toContain("-1.25 USDT");
    expect(wrapper.text()).toContain("已实现毛收益－实际成交手续费＋策略浮盈亏＋已结算资金费");
    expect(wrapper.text()).toContain("实际退出净收益可能因平仓手续费和滑点变化");
    expect(wrapper.text()).toContain("保留启动时已核对的手动基准仓位");
    expect(wrapper.text()).toContain("仓位与台账不一致或归属不明时阻止平仓");
    const serverWarning = wrapper.get('[data-testid="protection-server-warning"]');
    expect(serverWarning.isVisible()).toBe(true);
    expect(serverWarning.text()).toBe("这是服务端监控，不是交易所托管止损；服务器停机、断网或数据无法核实时不能保证及时退出。");
    expect(wrapper.text()).toContain("下一次有效核对");
    wrapper.unmount();
  });

  it("saves exact positive magnitudes and blocks duplicate saves without touching trade endpoints", async () => {
    const wrapper = setup();
    await flushPromises();
    const waiting = deferred<ProfitProtectionStatus>();
    vi.mocked(api.updateProfitProtection).mockReturnValue(waiting.promise);
    const start = vi.spyOn(api, "start");
    const stop = vi.spyOn(api, "stop");
    await wrapper.get('[data-testid="take-profit-amount"]').setValue("100.000000000000000001");
    await wrapper.get('[data-testid="stop-loss-amount"]').setValue("0.000000000000000001");
    await wrapper.get("form").trigger("submit");
    await wrapper.get("form").trigger("submit");
    expect(api.updateProfitProtection).toHaveBeenCalledTimes(1);
    expect(api.updateProfitProtection).toHaveBeenCalledWith("binance", "BTCUSDT", {
      run_id: "run-a", expected_revision: 2,
      take_profit_amount: "100.000000000000000001", stop_loss_amount: "0.000000000000000001",
    });
    expect(wrapper.get('[data-testid="protection-save"]').attributes("disabled")).toBeDefined();
    expect(start).not.toHaveBeenCalled();
    expect(stop).not.toHaveBeenCalled();
    waiting.resolve(status({ profit_protection: { ...settings, revision: 3 } }));
    await flushPromises();
    expect(wrapper.text()).toContain("已保存");
    wrapper.unmount();
  });

  it.each(["0", "-1", "NaN", "1e2", "1,2", "79228162514264337593543950336", "0.00000000000000000000000000001"])(
    "rejects invalid or unrepresentable decimal %s", async value => {
      const wrapper = setup();
      await flushPromises();
      await wrapper.get('[data-testid="take-profit-amount"]').setValue(value);
      await wrapper.get("form").trigger("submit");
      expect(api.updateProfitProtection).not.toHaveBeenCalled();
      expect(wrapper.get('[role="alert"]').text()).toContain("正数");
      wrapper.unmount();
    },
  );

  it("requires explicit disable rather than treating empty save as disable", async () => {
    const wrapper = setup();
    await flushPromises();
    await wrapper.get('[data-testid="take-profit-amount"]').setValue("");
    await wrapper.get('[data-testid="stop-loss-amount"]').setValue("");
    await wrapper.get("form").trigger("submit");
    expect(api.updateProfitProtection).not.toHaveBeenCalled();
    await wrapper.get('[data-testid="protection-disable"]').trigger("click");
    expect(api.updateProfitProtection).toHaveBeenCalledWith("binance", "BTCUSDT", {
      run_id: "run-a", expected_revision: 2, take_profit_amount: null, stop_loss_amount: null,
    });
    wrapper.unmount();
  });

  it("keeps legacy settings disabled and uses USDC settlement units", async () => {
    const wrapper = setup({ exchange: "trade_xyz", symbol: "MUUSDC", run_id: "legacy", running: true });
    await flushPromises();
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain("未启用");
    expect(wrapper.get('[data-testid="take-profit-amount"]').element).toHaveProperty("value", "");
    expect(wrapper.text()).toContain("USDC");
    expect(api.profitProtection).not.toHaveBeenCalled();
    expect(api.updateProfitProtection).not.toHaveBeenCalled();
    wrapper.unmount();
  });

  it("serializes reads on the existing refresh cadence and keeps edited fields", async () => {
    const wrapper = setup();
    await flushPromises();
    const waiting = deferred<ProfitProtectionStatus>();
    vi.mocked(api.profitProtection).mockReturnValue(waiting.promise);
    await wrapper.get('[data-testid="take-profit-amount"]').setValue("123");
    await wrapper.setProps({ refreshKey: 1 });
    await wrapper.setProps({ refreshKey: 2 });
    expect(api.profitProtection).toHaveBeenCalledTimes(2);
    waiting.resolve(status());
    await flushPromises();
    expect(wrapper.get('[data-testid="take-profit-amount"]').element).toHaveProperty("value", "123");
    wrapper.unmount();
  });

  it.each(["run", "symbol", "exchange"])("discards late responses after %s changes", async field => {
    const wrapper = setup();
    await flushPromises();
    const waiting = deferred<ProfitProtectionStatus>();
    vi.mocked(api.profitProtection).mockReturnValue(waiting.promise);
    await wrapper.setProps({ refreshKey: 1 });
    const next = { ...grid, profit_protection: undefined };
    if (field === "run") next.run_id = "run-b";
    if (field === "symbol") next.symbol = "ETHUSDT";
    if (field === "exchange") next.exchange = "aster";
    await wrapper.setProps({ status: next });
    waiting.resolve(status());
    await flushPromises();
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain("未启用");
    expect(wrapper.get('[data-testid="take-profit-amount"]').element).toHaveProperty("value", "");
    wrapper.unmount();
  });

  it("does not revive stale polling configuration after a save", async () => {
    const wrapper = setup();
    await flushPromises();
    const waiting = deferred<ProfitProtectionStatus>();
    vi.mocked(api.profitProtection).mockReturnValue(waiting.promise);
    await wrapper.setProps({ refreshKey: 1 });
    await wrapper.get('[data-testid="take-profit-amount"]').setValue("30");
    await wrapper.get("form").trigger("submit");
    await flushPromises();
    waiting.resolve(status({ observation: { status: "unavailable", observed_at_ms: null, net_profit: null, error: "stale" } }));
    await flushPromises();
    expect(wrapper.text()).not.toContain("stale");
    expect(wrapper.text()).toContain("版本 3");
    wrapper.unmount();
  });

  it("requires a deliberate reload for an externally changed revision", async () => {
    const wrapper = setup();
    await flushPromises();
    await wrapper.get('[data-testid="take-profit-amount"]').setValue("123");
    vi.mocked(api.profitProtection).mockResolvedValue(status({ profit_protection: { ...settings, revision: 9 } }));
    await wrapper.setProps({ refreshKey: 1 });
    await flushPromises();
    await wrapper.get("form").trigger("submit");
    expect(api.updateProfitProtection).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("配置已变更");
    await wrapper.get('[data-testid="protection-reload"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="take-profit-amount"]').setValue("25");
    await wrapper.get("form").trigger("submit");
    expect(api.updateProfitProtection).toHaveBeenCalledWith("binance", "BTCUSDT", expect.objectContaining({ expected_revision: 9 }));
    wrapper.unmount();
  });

  it("does not label a new configuration with the previous revision's verified observation", async () => {
    const wrapper = setup();
    await flushPromises();
    vi.mocked(api.profitProtection).mockReturnValue(new Promise(() => {}));
    await wrapper.setProps({
      status: { ...grid, profit_protection: { ...settings, revision: 3, take_profit_amount: "1" } },
      refreshKey: 1,
    });
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain("核对中");
    expect(wrapper.text()).not.toContain("-1.25 USDT");
    wrapper.unmount();
  });

  it.each([401, 404, 409, 503])("shows save error %s without retrying or claiming success", async code => {
    const wrapper = setup();
    await flushPromises();
    vi.mocked(api.updateProfitProtection).mockRejectedValue(new ApiError("control unavailable", code));
    await wrapper.get("form").trigger("submit");
    await flushPromises();
    expect(wrapper.get('[role="alert"]').text()).toContain("control unavailable");
    expect(wrapper.text()).not.toContain("已保存");
    expect(api.updateProfitProtection).toHaveBeenCalledTimes(1);
    wrapper.unmount();
  });

  it.each(["checking", "unavailable", "triggered"])("renders %s evidence and blocks terminal edits", async observationStatus => {
    const wrapper = setup();
    await flushPromises();
    vi.mocked(api.profitProtection).mockResolvedValue(status({
      lifecycle: observationStatus === "triggered" ? "risk_exit_requested" : "running",
      observation: { status: observationStatus, observed_at_ms: null, net_profit: null, error: observationStatus === "unavailable" ? "funding pending" : null },
      trigger: observationStatus === "triggered" ? { net_profit: "21", mark_price: "100", observed_at_ms: 15, protection_revision: 2 } : null,
    }));
    await wrapper.setProps({ refreshKey: 1 });
    await flushPromises();
    expect(wrapper.get('[data-testid="protection-status"]').text()).toContain(
      { checking: "核对中", unavailable: "暂不可用", triggered: "已触发" }[observationStatus]!,
    );
    if (observationStatus === "triggered") {
      expect(wrapper.text()).toContain("21 USDT");
      expect(wrapper.get('[data-testid="protection-save"]').attributes("disabled")).toBeDefined();
    }
    if (observationStatus === "unavailable") expect(wrapper.text()).toContain("funding pending");
    wrapper.unmount();
  });
});
