import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import type { ApiConfigResponse, GridStatus } from "../api/types";
import AuthDialog from "./AuthDialog.vue";
import ExchangeSettingsDialog from "./ExchangeSettingsDialog.vue";
import GridConfigurationPanel from "./GridConfigurationPanel.vue";
import MarketOverview from "./MarketOverview.vue";
import StrategyDetailsPanel from "./StrategyDetailsPanel.vue";
import StrategyList from "./StrategyList.vue";

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

  it("uses production wallet fields for Aster and never pre-fills secrets", async () => {
    const config: ApiConfigResponse = {
      configured: true,
      active_exchange: "aster",
      configs: {
        aster: {
          exchange: "aster",
          configured: true,
          api_key: "0x12…89",
          testnet: true,
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
      },
    });

    expect(wrapper.text()).toContain("生产钱包地址");
    expect(wrapper.text()).toContain("生产钱包私钥");
    expect(wrapper.find('input[type="checkbox"]').exists()).toBe(false);
    await wrapper.find("form").trigger("submit");
    expect(wrapper.emitted("save")?.[0]?.[0]).toMatchObject({
      exchange: "aster",
      testnet: false,
    });
    for (const input of wrapper.findAll('input:not([type="checkbox"])')) {
      expect((input.element as HTMLInputElement).value).toBe("");
    }
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

    expect(wrapper.emitted("preview")?.[0]?.[0]).toMatchObject({
      exchange: "binance",
      symbol: "MUUSDT",
      grid_count: 20,
      total_investment: 0,
      position_sizing_mode: "fixed_grid_qty",
      grid_order_qty: 0.2,
      initial_order_type: "market",
      maker_fee_rate: 0.0002,
      taker_fee_rate: 0.0005,
    });

    await wrapper.find('[data-testid="sizing-mode"]').setValue("investment");
    expect(wrapper.find('[data-testid="grid-order-qty"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="total-investment"]').exists()).toBe(true);
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
        balance: { available: "123.4567", equity: "140" },
        fees: null,
        loading: false,
      },
    });

    expect(wrapper.text()).toContain("123.4567");
  });

  it("shows authoritative open order quantities without normalizing them", async () => {
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
            price: "0.3800000",
            qty: "70",
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

    expect(wrapper.text()).toContain("70");
    expect(wrapper.text()).toContain("g_7_B_exact");
    expect(wrapper.text()).toContain("止盈/平仓");
  });
});
