import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import type { ApiConfigResponse, GridStatus, RiskSnapshot } from "../api/types";
import AuthDialog from "./AuthDialog.vue";
import ExchangeSettingsDialog from "./ExchangeSettingsDialog.vue";
import GridConfigurationPanel from "./GridConfigurationPanel.vue";
import MarketOverview from "./MarketOverview.vue";
import StrategyDetailsPanel from "./StrategyDetailsPanel.vue";
import StrategyList from "./StrategyList.vue";
import StrategyOverview from "./StrategyOverview.vue";

describe("Vue migration components", () => {
  it("emits login data only after the form is submitted", async () => {
    const wrapper = mount(AuthDialog, {
      props: {
        status: { required: true, configured: true, authenticated: false },
        busy: false,
        error: "",
      },
    });

    const inputs = wrapper.findAll("input");
    await inputs[1]!.setValue("temporary-password");
    await inputs[2]!.setValue("123456");
    await wrapper.find("form").trigger("submit");

    expect(wrapper.emitted("submit")?.[0]?.[0]).toEqual({
      username: "admin",
      password: "temporary-password",
      code: "123456",
    });
  });

  it("saves Aster with one private key while keeping exchange status masked", async () => {
    const config: ApiConfigResponse = {
      configured: true,
      active_exchange: "aster",
      configs: {
        aster: {
          exchange: "aster",
          configured: true,
          api_key: "0x12…89",
          testnet: false,
          source: "env",
        },
      },
    };
    const wrapper = mount(ExchangeSettingsDialog, {
      props: {
        open: true,
        config,
        activeExchange: "aster",
        busy: false,
        error: "",
        message: "",
      },
    });

    expect(wrapper.text()).toContain("交易所 API 配置");
    expect(wrapper.text()).toContain("0x12…89 · Mainnet");
    const privateKey = wrapper.find('input[type="password"]');
    await privateKey.setValue("1".repeat(64));
    await wrapper.find("form").trigger("submit");
    expect(wrapper.emitted("save")?.[0]?.[0]).toEqual({
      exchange: "aster",
      private_key: "1".repeat(64),
      testnet: false,
    });
  });

  it("selects a strategy with its exchange identity intact", async () => {
    const grid: GridStatus = {
      exchange: "binance",
      symbol: "MUUSDT",
      running: true,
      direction: "short",
      grid_mode: "arithmetic",
      total_profit: 1.25,
    };
    const wrapper = mount(StrategyList, {
      props: {
        grids: [grid],
        activeExchange: "aster",
        activeSymbol: "ANSEMUSDT",
        loading: false,
      },
    });

    await wrapper.find("button.strategy-row").trigger("click");

    expect(wrapper.emitted("select")?.[0]?.[0]).toEqual(grid);
  });

  it("labels strategy-list profit as realized rather than total equity", () => {
    const wrapper = mount(StrategyList, {
      props: {
        grids: [{
          run_id: "run-profit-list",
          exchange: "aster",
          symbol: "ANSEMUSDT",
          running: true,
          realized_net_profit: "1.25",
          total_equity_profit: "9.75",
          completed_pairs: 3,
        }],
        activeExchange: "aster",
        activeSymbol: "ANSEMUSDT",
        loading: false,
      },
    });

    expect(wrapper.find(".strategy-profit strong").text()).toBe("1.25");
    expect(wrapper.text()).not.toContain("9.75");
    expect(wrapper.text()).toContain("已实现净利润 · 完成 3 次");
  });

  it("keeps fixed per-grid quantity separate from investment sizing", async () => {
    const wrapper = mount(GridConfigurationPanel, {
      props: {
        exchange: "binance",
        symbol: "MUUSDT",
        configured: true,
        fees: { maker_fee_rate: 0.0002, taker_fee_rate: 0.0005 },
        preview: null,
        busy: false,
        error: "",
      },
    });

    expect(wrapper.find('[data-testid="grid-order-qty"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="total-investment"]').exists()).toBe(false);
    await wrapper.find('[data-testid="lower-price"]').setValue("1000");
    await wrapper.find('[data-testid="upper-price"]').setValue("1020");
    await wrapper.find('[data-testid="grid-order-qty"]').setValue("0.2");
    await wrapper.find("form").trigger("submit");

    expect(wrapper.find(".form-error").exists() ? wrapper.find(".form-error").text() : "").toBe("");
    expect(wrapper.emitted("preview")?.[0]?.[0]).toMatchObject({
      exchange: "binance",
      symbol: "MUUSDT",
      grid_count: 20,
      total_investment: "0",
      position_sizing_mode: "fixed_grid_qty",
      grid_order_qty: "0.2",
      initial_order_type: "market",
      maker_fee_rate: "0.0002",
      taker_fee_rate: "0.0005",
    });

    await wrapper.find('[data-testid="sizing-mode"]').setValue("investment");
    expect(wrapper.find('[data-testid="grid-order-qty"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="total-investment"]').exists()).toBe(true);
  });

  it("directs missing exchange credentials to server configuration", () => {
    const wrapper = mount(GridConfigurationPanel, {
      props: {
        exchange: "aster",
        symbol: "ANSEMUSDT",
        configured: false,
        fees: null,
        preview: null,
        busy: false,
        error: "",
      },
    });

    expect(wrapper.text()).toContain("请先在服务器配置当前交易所并重启候选服务");
    expect(wrapper.text()).not.toContain("请先保存当前交易所配置");
  });

  it("starts only the exact configuration that completed an authoritative preview", async () => {
    const wrapper = mount(GridConfigurationPanel, {
      props: {
        exchange: "binance",
        symbol: "MUUSDT",
        configured: true,
        fees: { maker_fee_rate: "0.0002", taker_fee_rate: "0.0005" },
        preview: null,
        previewKey: "",
        busy: false,
        error: "",
        tradingEnabled: true,
      },
    });
    await wrapper.find('[data-testid="lower-price"]').setValue("1000");
    await wrapper.find('[data-testid="upper-price"]').setValue("1020");
    await wrapper.find('[data-testid="grid-order-qty"]').setValue("0.2");
    await wrapper.find("form").trigger("submit");
    expect(wrapper.find(".form-error").exists() ? wrapper.find(".form-error").text() : "").toBe("");
    const exactConfig = wrapper.emitted("preview")?.[0]?.[0];
    expect(exactConfig).toBeDefined();

    await wrapper.setProps({
      preview: {
        grid_step: "1",
        grid_profit_pct: "0.1",
        per_grid_gross_profit: "0.2",
        per_grid_fee: "0.05",
        per_grid_net_profit: "0.15",
        active_grid_count: 10,
        grid_count: 20,
        qty_per_grid_avg: "0.2",
        total_qty: "2",
        maker_fee_rate: "0.0002",
        taker_fee_rate: "0.0005",
      },
      previewKey: JSON.stringify(exactConfig),
    });
    const startButton = wrapper
      .findAll("button")
      .find((button) => button.text() === "启动已预览策略");
    expect(startButton).toBeDefined();
    expect(startButton!.attributes("disabled")).toBeUndefined();
    await startButton!.trigger("click");
    expect(wrapper.emitted("start")?.[0]?.[0]).toEqual(exactConfig);

    await wrapper.find('[data-testid="upper-price"]').setValue("1021");
    expect(startButton!.attributes("disabled")).toBeDefined();
    await startButton!.trigger("click");
    expect(wrapper.emitted("start")).toHaveLength(1);

    await wrapper.find('[data-testid="upper-price"]').setValue("1020");
    await wrapper.setProps({ tradingEnabled: false });
    expect(startButton!.attributes("disabled")).toBeDefined();
  });

  it("requires an explicit second click before stopping a running strategy", async () => {
    const wrapper = mount(StrategyOverview, {
      props: {
        status: {
          run_id: "run-safe-1",
          exchange: "aster",
          symbol: "ANSEMUSDT",
          running: true,
        },
        risk: null,
      },
    });
    const stopButton = wrapper.find("button.stop-button");
    await stopButton.trigger("click");
    expect(wrapper.emitted("stop")).toBeUndefined();
    expect(stopButton.text()).toContain("确认停止");
    await stopButton.trigger("click");
    expect(wrapper.emitted("stop")).toHaveLength(1);
  });

  it("shows total equity only from the matching authoritative risk snapshot", () => {
    const status: GridStatus = {
      run_id: "run-profit-1",
      exchange: "aster",
      symbol: "ANSEMUSDT",
      running: true,
      realized_net_profit: "1.0",
      total_profit: "1.0",
    };
    const risk: RiskSnapshot = {
      run_id: "run-profit-1",
      exchange: "aster",
      symbol: "ANSEMUSDT",
      realized_net_profit: "1.0",
      grid_unrealised_pnl: "2.0",
      total_equity_profit: "3.0",
      profit_scope: "strategy_owned_inventory",
      has_risk: false,
    };
    const wrapper = mount(StrategyOverview, { props: { status, risk } });

    const metrics = wrapper.findAll(".metric-grid > div").map((metric) => metric.text());
    expect(metrics).toContain("总权益利润3 USDT");
    expect(metrics).toContain("已实现净利润1 USDT");
    expect(metrics).toContain("网格未实现盈亏2 USDT");
    expect(wrapper.text()).not.toContain("等待当前策略");
  });

  it("rejects a stale profit snapshot from another strategy context", () => {
    const wrapper = mount(StrategyOverview, {
      props: {
        status: {
          run_id: "run-current",
          exchange: "binance",
          symbol: "MUUSDT",
          running: true,
          realized_net_profit: "1.0",
        },
        risk: {
          run_id: "run-old",
          exchange: "aster",
          symbol: "ANSEMUSDT",
          total_equity_profit: "999.0",
          grid_unrealised_pnl: "998.0",
          has_risk: true,
        },
      },
    });

    expect(wrapper.text()).not.toContain("999.0000");
    expect(wrapper.text()).not.toContain("998.0000");
    expect(wrapper.text()).not.toContain("风险核对未通过");
    expect(wrapper.text()).toContain("总权益利润等待当前策略的交易所权威风险快照");
  });

  it("exposes three distinct opening order semantics", () => {
    const wrapper = mount(GridConfigurationPanel, {
      props: {
        exchange: "aster",
        symbol: "ANSEMUSDT",
        configured: true,
        fees: { maker_fee_rate: 0.0002, taker_fee_rate: 0.0005 },
        preview: null,
        busy: false,
        error: "",
      },
    });
    const select = wrapper.find('[data-testid="initial-order-type"]')
      .element as HTMLSelectElement;

    expect(Array.from(select.options).map((option) => option.value)).toEqual([
      "market",
      "limit",
      "post_only",
    ]);
  });

  it("renders the backend balance field used by the current API", () => {
    const wrapper = mount(MarketOverview, {
      props: {
        exchange: "aster",
        symbol: "ANSEMUSDT",
        configured: true,
        price: null,
        balance: { available: "123.4567", equity: "140", unit: "USDT" },
        fees: null,
        loading: false,
      },
    });

    expect(wrapper.text()).toContain("123.4567");
    expect(wrapper.text()).toContain("可用余额 (USDT)");
    expect(wrapper.text()).toContain("账户权益 (USDT)");
  });

  it("shows authoritative order decimals without rounding them", async () => {
    const wrapper = mount(StrategyDetailsPanel, {
      props: {
        exchange: "aster",
        symbol: "ANSEMUSDT",
        configured: true,
        loading: false,
        error: "",
        positions: [],
        orders: [
          {
            order_id: "123",
            order_link_id: "g_7_B_exact",
            side: "Buy",
            price: "0.380001234567",
            qty: "70.000000001",
            status: "NEW",
            reduce_only: true,
          },
        ],
        trades: [],
        history: [],
      },
    });
    const orderTab = wrapper
      .findAll(".detail-tabs button")
      .find((button) => button.text().startsWith("挂单"));
    expect(orderTab).toBeDefined();
    await orderTab!.trigger("click");

    expect(wrapper.text()).toContain("0.380001234567");
    expect(wrapper.text()).toContain("70.000000001");
    expect(wrapper.text()).toContain("g_7_B_exact");
    expect(wrapper.text()).toContain("止盈/平仓");
  });

  it("labels converted trade fees with the quote asset instead of the charged asset", async () => {
    const wrapper = mount(StrategyDetailsPanel, {
      props: {
        exchange: "binance",
        symbol: "MUUSDT",
        configured: true,
        loading: false,
        error: "",
        positions: [],
        orders: [],
        trades: [{
          order_id: "order-1",
          trade_id: "trade-1",
          side: "Sell",
          price: "1014",
          qty: "0.2",
          volume: "202.8",
          fee: "0.0003",
          fee_usdt: "0.12",
          fee_asset: "BNB",
          fee_quote_asset: "USDT",
          is_maker: true,
          realized_pnl: "1.5",
          profit: "1.38",
          time: 1_784_102_730_940,
        }],
        history: [],
      },
    });
    const tradeTab = wrapper
      .findAll(".detail-tabs button")
      .find((button) => button.text().startsWith("成交"));
    expect(tradeTab).toBeDefined();
    await tradeTab!.trigger("click");

    expect(wrapper.text()).toContain("0.12 USDT");
    expect(wrapper.text()).not.toContain("0.12 BNB");
  });
});
