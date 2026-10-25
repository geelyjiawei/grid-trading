import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import type { GridOrder } from "../api/types";
import { comparePrices } from "../order-display";
import StrategyDetailsPanel from "./StrategyDetailsPanel.vue";
import ExchangeRecordsPanel from "./ExchangeRecordsPanel.vue";
import "../../test-support/dialog-mock";

const props = {
  exchange: "trade_xyz" as const,
  symbol: "PONSUSDC",
  configured: true,
  loading: false,
  error: "",
  embedded: true,
  positions: [],
  orders: [] as GridOrder[],
  trades: [],
  history: [],
};
const order = (
  id: string,
  price: string,
  side: "Buy" | "Sell" = "Buy",
): GridOrder => ({
  order_id: id,
  order_link_id: `client-${id}`,
  price,
  side,
  qty: "100.0",
  status: "NEW",
  reduce_only: side === "Buy",
});
const tab = (wrapper: ReturnType<typeof mount>, name: string) =>
  wrapper
    .findAll(".detail-tabs button")
    .find((button) => button.text().startsWith(name))!;

describe("readable bot records", () => {
  it("sorts exact prices, not client identifiers or floating point approximations", async () => {
    const orders = [
      order("a", "10"),
      order("z", "0.700000000000000002"),
      order("b", "0.700000000000000001"),
    ];
    const wrapper = mount(StrategyDetailsPanel, {
      props: { ...props, orders },
    });
    expect(wrapper.findAll(".order-price").map((row) => row.text())).toEqual([
      "0.700000000000000001",
      "0.700000000000000002",
      "10",
    ]);
    expect(orders.map((item) => item.order_id)).toEqual(["a", "z", "b"]);
    await wrapper.get('select[aria-label="挂单排序"]').setValue("desc");
    expect(wrapper.findAll(".order-price").map((row) => row.text())).toEqual([
      "10",
      "0.700000000000000002",
      "0.700000000000000001",
    ]);
    expect(comparePrices("0001.00", "1")).toBe(0);
  });

  it("paginates, filters and clamps the page when orders disappear after a refresh", async () => {
    const orders = Array.from({ length: 23 }, (_, i) =>
      order(String(i), String(i + 1), i % 2 ? "Sell" : "Buy"),
    );
    const wrapper = mount(StrategyDetailsPanel, {
      props: { ...props, orders },
    });
    expect(wrapper.findAll(".order-row")).toHaveLength(10);
    await wrapper.get('[aria-label="下一页"]').trigger("click");
    expect(wrapper.find(".order-price").text()).toBe("11");
    await wrapper.get('select[aria-label="筛选买卖方向"]').setValue("Sell");
    expect(wrapper.find(".order-price").text()).toBe("2");
    expect(
      wrapper.findAll(".side-tag").every((tag) => tag.text() === "卖出"),
    ).toBe(true);
    await wrapper.get('[aria-label="下一页"]').trigger("click");
    expect(wrapper.findAll(".order-row")).toHaveLength(1);
    await wrapper.setProps({ orders: [orders[1]!] });
    expect(wrapper.find(".order-price").text()).toBe("2");
    expect(wrapper.find(".record-pagination").text()).toContain("1 / 1");
  });

  it("resets filters and expanded identifiers when exchange/symbol changes", async () => {
    const wrapper = mount(StrategyDetailsPanel, {
      props: { ...props, orders: [order("42", ".7", "Sell")] },
    });
    await wrapper.get('select[aria-label="筛选买卖方向"]').setValue("Sell");
    await wrapper.get(".record-expand").trigger("click");
    expect(wrapper.text()).toContain("client-42");
    await wrapper.setProps({
      exchange: "binance",
      symbol: "MUUSDT",
      orders: [order("43", "1000")],
    });
    expect(wrapper.text()).not.toContain("client-42");
    expect(wrapper.find(".order-price").text()).toBe("1,000");
    expect(wrapper.find(".expanded-row").exists()).toBe(false);
  });

  it("filters account-wide history to the current exchange and symbol", async () => {
    const wrapper = mount(StrategyDetailsPanel, {
      props: {
        ...props,
        history: [
          {
            run_id: "wanted",
            exchange: "trade_xyz",
            symbol: "PONSUSDC",
            status: "stopped",
            started_at: 1788830000000,
            net_profit: "12.345",
            completed_pairs: 23,
          },
          { exchange: "aster", symbol: "PONSUSDC", net_profit: "7777" },
          { exchange: "trade_xyz", symbol: "MUUSDC", net_profit: "8888" },
        ],
      },
    });
    await tab(wrapper, "历史策略").trigger("click");
    expect(wrapper.findAll(".execution-item")).toHaveLength(1);
    expect(wrapper.text()).toContain("12.345");
    expect(wrapper.text()).not.toContain("7777");
    expect(wrapper.text()).not.toContain("8888");
    expect(wrapper.text()).toContain("已停止");
    await wrapper.find(".execution-summary").trigger("click");
    expect(wrapper.find(".record-definition").text()).toContain("完成配对23");
  });

  it("keeps stale order rows visible but labels counts unknown, not zero", async () => {
    const wrapper = mount(StrategyDetailsPanel, {
      props: {
        ...props,
        orders: [order("42", "0.70123")],
        availability: {
          orders: false,
          trades: true,
          positions: true,
          history: true,
        },
      },
    });
    expect(tab(wrapper, "挂单").text()).toContain("--");
    expect(wrapper.find(".record-totals").text()).toContain("买单 --");
    expect(wrapper.find(".order-price").text()).toBe("0.70123");
    expect(wrapper.text()).toContain("保留上次成功快照");
    expect(wrapper.text()).toContain("上次快照");
  });

  it("distinguishes maker, taker and unknown fills without inventing pairing profits", async () => {
    const wrapper = mount(StrategyDetailsPanel, {
      props: {
        ...props,
        trades: [
          {
            side: "Buy",
            price: "0.71",
            qty: "13",
            trade_id: "1",
            time: 1788830000,
            is_maker: true,
          },
          {
            side: "Sell",
            price: "0.72",
            qty: "3",
            trade_id: "2",
            time: 1788830001000,
            is_maker: false,
          },
          { side: "Buy", price: "0.70", qty: "2", trade_id: "3" },
        ],
      },
    });
    await tab(wrapper, "成交").trigger("click");
    expect(wrapper.findAll(".execution-item")[0]!.text()).toContain("吃单成交");
    expect(wrapper.text()).toContain("挂单成交 · Maker");
    expect(wrapper.text()).toContain("成交类型未知");
    expect(wrapper.text()).toContain("不等于网格配对净利润");
    await wrapper.find(".execution-summary").trigger("click");
    expect(wrapper.find(".record-definition").text()).toContain(
      "单笔已实现盈亏-- USDC",
    );
  });

  it("opens the requested exchange category directly without strategy controls or extra requests", async () => {
    const wrapper = mount(ExchangeRecordsPanel, {
      props: {
        ...props,
        orders: [order("42", "0.7")],
        history: [
          { exchange: "trade_xyz", symbol: "PONSUSDC" },
          { exchange: "aster", symbol: "PONSUSDC" },
        ],
        availability: {
          positions: true,
          orders: true,
          trades: true,
          history: true,
        },
      },
    });
    expect(wrapper.find(".bot-records").exists()).toBe(false);
    expect(wrapper.find(".strategy-specification").exists()).toBe(false);
    expect(wrapper.get('[aria-label="查看历史"]').text()).toContain("1");
    const drawer = wrapper.get("dialog");
    for (const name of ["持仓", "挂单", "成交", "历史"]) {
      await wrapper.get(`[aria-label="查看${name}"]`).trigger("click");
      expect((drawer.element as HTMLDialogElement).open).toBe(true);
      expect(drawer.get('.detail-tabs [aria-pressed="true"]').text()).toContain(
        name,
      );
      expect(drawer.find(".strategy-metrics").exists()).toBe(false);
      expect(drawer.find(".strategy-specification").exists()).toBe(false);
      expect(drawer.find(".stop-button").exists()).toBe(false);
      await drawer.get(".bot-drawer-close").trigger("click");
    }
    await wrapper.get('[aria-label="查看挂单"]').trigger("click");
    // Refreshing the same context must preserve the drawer and selected tab.
    await wrapper.setProps({ orders: [order("43", "0.71")] });
    expect((drawer.element as HTMLDialogElement).open).toBe(true);
    expect(drawer.get(".order-price").text()).toBe("0.71");
    expect(wrapper.emitted("stop")).toBeUndefined();
    expect(wrapper.emitted("refresh")).toBeUndefined();
    await wrapper.find(".records-toolbar > button").trigger("click");
    expect(wrapper.emitted("refresh")).toHaveLength(1);

    await wrapper.setProps({ symbol: "MUUSDC" });
    expect((drawer.element as HTMLDialogElement).open).toBe(false);
    expect(drawer.find(".bot-records").exists()).toBe(false);
    wrapper.unmount();
  });

  it("restores page scrolling and focus on close, cancel, backdrop and unmount", async () => {
    const originalOverflow = document.body.style.overflow;
    document.body.style.overflow = "auto";
    const wrapper = mount(ExchangeRecordsPanel, {
      attachTo: document.body,
      props: {
        ...props,
        availability: {
          positions: true,
          orders: true,
          trades: true,
          history: true,
        },
      },
    });
    const opener = wrapper.get('[aria-label="查看持仓"]');
    const drawer = wrapper.get("dialog");
    await opener.trigger("click");
    expect(document.body.style.overflow).toBe("hidden");
    const closeButton = wrapper.get(".bot-drawer-close");
    await closeButton.trigger("keydown", { key: "Tab", shiftKey: true });
    const lastButton = drawer.get(".records-toolbar > button");
    expect(document.activeElement).toBe(lastButton.element);
    await lastButton.trigger("keydown", { key: "Tab" });
    expect(document.activeElement).toBe(closeButton.element);
    await wrapper.get(".bot-drawer-close").trigger("click");
    expect(document.body.style.overflow).toBe("auto");
    expect(document.activeElement).toBe(opener.element);
    expect(wrapper.find(".bot-records").exists()).toBe(false);
    await opener.trigger("click");
    await drawer.trigger("cancel");
    expect((drawer.element as HTMLDialogElement).open).toBe(false);
    await opener.trigger("click");
    await drawer.trigger("click", { clientX: -1, clientY: -1 });
    expect((drawer.element as HTMLDialogElement).open).toBe(false);
    await opener.trigger("click");
    wrapper.unmount();
    expect(document.body.style.overflow).toBe("auto");
    document.body.style.overflow = originalOverflow;
  });
});
