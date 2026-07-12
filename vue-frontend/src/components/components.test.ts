import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import type { ApiConfigResponse, GridStatus } from "../api/types";
import AuthDialog from "./AuthDialog.vue";
import ExchangeSettingsDialog from "./ExchangeSettingsDialog.vue";
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
});
