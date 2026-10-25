<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type {
  Exchange,
  GridHistoryRun,
  GridOrder,
  GridTrade,
  PositionSnapshot,
} from "../api/types";
import {
  directionName,
  exchangeName,
  formatExactDecimal,
  formatNumber,
  formatTimestamp,
  quoteAsset,
  strategyStatusLabel,
} from "../format";
import {
  comparePrices,
  orderIdentity,
  orderStatusLabel,
  timestampMillis,
} from "../order-display";

type DetailTab = "positions" | "orders" | "trades" | "history";
const props = withDefaults(
  defineProps<{
    exchange: Exchange;
    symbol: string;
    configured: boolean;
    loading: boolean;
    error: string;
    embedded?: boolean;
    initialTab?: DetailTab;
    availability?: Record<DetailTab, boolean>;
    positions: PositionSnapshot[];
    orders: GridOrder[];
    trades: GridTrade[];
    history: GridHistoryRun[];
  }>(),
  {
    embedded: false,
    availability: () => ({
      positions: true,
      orders: true,
      trades: true,
      history: true,
    }),
  },
);
const emit = defineEmits<{ refresh: [] }>();
const activeTab = ref<DetailTab>(
  props.initialTab ?? (props.embedded ? "orders" : "positions"),
);
const side = ref("all");
const sort = ref("asc");
const page = ref(1);
const pageSize = 10;
const expanded = ref<string | null>(null);
const quote = computed(() => quoteAsset(props.exchange));
const base = computed(() =>
  props.symbol.endsWith(quote.value)
    ? props.symbol.slice(0, -quote.value.length)
    : props.symbol,
);
const tabs: Array<{ key: DetailTab; label: string }> = [
  { key: "positions", label: "持仓" },
  { key: "orders", label: "挂单" },
  { key: "trades", label: "成交" },
  { key: "history", label: "历史策略" },
];
// History is returned account-wide; never mix exchanges or symbols in this workspace.
const scopedHistory = computed(() =>
  props.history
    .filter(
      (run) => run.exchange === props.exchange && run.symbol === props.symbol,
    )
    .slice()
    .sort(
      (a, b) => timestampMillis(b.started_at) - timestampMillis(a.started_at),
    ),
);
const filteredOrders = computed(() =>
  props.orders
    .filter((order) => side.value === "all" || order.side === side.value)
    .slice()
    .sort((a, b) => {
      const delta =
        comparePrices(a.price, b.price) * (sort.value === "desc" ? -1 : 1);
      return delta || orderIdentity(a).localeCompare(orderIdentity(b));
    }),
);
const filteredTrades = computed(() =>
  props.trades
    .filter((trade) => side.value === "all" || trade.side === side.value)
    .slice()
    .sort((a, b) => timestampMillis(b.time) - timestampMillis(a.time)),
);
const total = computed(() =>
  activeTab.value === "orders"
    ? filteredOrders.value.length
    : activeTab.value === "trades"
      ? filteredTrades.value.length
      : scopedHistory.value.length,
);
const pages = computed(() => Math.max(1, Math.ceil(total.value / pageSize)));
const start = computed(() => (page.value - 1) * pageSize);
const orderRows = computed(() =>
  filteredOrders.value.slice(start.value, start.value + pageSize),
);
const tradeRows = computed(() =>
  filteredTrades.value.slice(start.value, start.value + pageSize),
);
const historyRows = computed(() =>
  scopedHistory.value.slice(start.value, start.value + pageSize),
);
const buyCount = computed(
  () => props.orders.filter((order) => order.side === "Buy").length,
);
const makerCount = computed(
  () => props.trades.filter((trade) => trade.is_maker === true).length,
);
const takerCount = computed(
  () => props.trades.filter((trade) => trade.is_maker === false).length,
);
watch(
  [() => props.exchange, () => props.symbol, () => props.initialTab],
  () => {
    activeTab.value =
      props.initialTab ?? (props.embedded ? "orders" : "positions");
    side.value = "all";
    sort.value = "asc";
    page.value = 1;
    expanded.value = null;
  },
);
watch([activeTab, side, sort], () => {
  page.value = 1;
  expanded.value = null;
});
watch(pages, (count) => {
  page.value = Math.min(page.value, count);
});
watch(page, () => {
  expanded.value = null;
});

function tabCount(tab: DetailTab): number | string {
  if (!props.availability[tab]) return "--";
  return tab === "positions"
    ? props.positions.length
    : tab === "orders"
      ? props.orders.length
      : tab === "trades"
        ? props.trades.length
        : scopedHistory.value.length;
}
function unavailableMessage(tab: DetailTab): string {
  const count =
    tab === "positions"
      ? props.positions.length
      : tab === "orders"
        ? props.orders.length
        : tab === "trades"
          ? props.trades.length
          : scopedHistory.value.length;
  return count > 0
    ? "本次实时读取失败，以下保留上次成功快照，不能视为当前实时数据。"
    : "本次实时读取失败，当前数量未知，不能按 0 处理。";
}
function reduceOnly(order: GridOrder): boolean {
  return Boolean(order.reduce_only ?? order.reduceOnly);
}
function orderQuantity(order: GridOrder): string {
  const remaining = formatExactDecimal(order.remaining_qty ?? order.qty);
  return order.status === "PARTIALLY_FILLED" && order.original_qty !== undefined
    ? `${remaining} / ${formatExactDecimal(order.original_qty)}`
    : remaining;
}
function feeDisplayAsset(trade: GridTrade): string {
  return trade.fee_usdt != null
    ? trade.fee_quote_asset || quote.value
    : trade.fee_asset || "";
}
function runProfit(run: GridHistoryRun) {
  return run.total_equity_profit ?? run.net_profit;
}
function toggle(id: string) {
  expanded.value = expanded.value === id ? null : id;
}
function tradeKey(trade: GridTrade, index: number) {
  return `trade:${trade.order_id}:${trade.trade_id ?? `${trade.time}:${index}`}`;
}
function runKey(run: GridHistoryRun, index: number) {
  return `run:${run.run_id ?? `${run.started_at}:${index}`}`;
}
function runStatus(run: GridHistoryRun): string {
  if (!run.status) return "--";
  return strategyStatusLabel({
    exchange: props.exchange,
    symbol: props.symbol,
    running: run.status === "running" || run.status === "active",
    lifecycle: run.status,
  });
}
</script>

<template>
  <section
    class="bot-records"
    :class="{ 'panel-card dashboard-span': !embedded }"
    aria-label="交易所订单记录"
  >
    <header v-if="!embedded" class="section-header">
      <div>
        <p class="eyebrow">交易所权威数据</p>
        <h2>{{ exchangeName(exchange) }} · {{ symbol }}</h2>
      </div>
    </header>
    <div class="records-toolbar">
      <nav class="detail-tabs record-tabs" aria-label="交易记录分类">
        <button
          v-for="tab in tabs"
          :key="tab.key"
          type="button"
          :class="{ active: activeTab === tab.key }"
          :aria-pressed="activeTab === tab.key"
          @click="activeTab = tab.key"
        >
          {{ tab.label }} <span>{{ tabCount(tab.key) }}</span>
        </button>
      </nav>
      <button
        class="ghost-button"
        type="button"
        :disabled="loading || !configured"
        @click="emit('refresh')"
      >
        {{ loading ? "读取中…" : "刷新明细" }}
      </button>
    </div>
    <p class="bot-footnote">
      {{ exchangeName(exchange) }} · {{ symbol }} ·
      订单与成交为该交易对账户数据，可能包含手动订单；历史策略仅显示本交易所、本交易对。
    </p>
    <p v-if="!configured" class="empty-state">
      请先配置当前交易所，再读取真实持仓和订单。
    </p>
    <template v-else>
      <p v-if="error" class="callout danger" role="status">{{ error }}</p>
      <p v-if="!availability[activeTab]" class="callout" role="status">
        {{ loading ? "正在读取快照。" : "" }}{{ unavailableMessage(activeTab) }}
      </p>
      <div
        v-if="activeTab === 'orders' || activeTab === 'trades'"
        class="records-controls"
      >
        <div v-if="activeTab === 'orders'" class="record-totals">
          <span class="positive"
            >买单
            <strong>{{ availability.orders ? buyCount : "--" }}</strong></span
          ><span class="negative"
            >卖单
            <strong>{{
              availability.orders ? orders.length - buyCount : "--"
            }}</strong></span
          >
        </div>
        <div v-else class="record-totals">
          <span
            >已加载成交 <strong>{{ tabCount("trades") }}</strong> 笔</span
          ><span
            >挂单成交 {{ availability.trades ? makerCount : "--" }} / 吃单成交
            {{ availability.trades ? takerCount : "--" }}</span
          >
        </div>
        <div class="record-filters">
          <label
            >方向<select v-model="side" aria-label="筛选买卖方向">
              <option value="all">全部方向</option>
              <option value="Buy">买入</option>
              <option value="Sell">卖出</option>
            </select></label
          >
          <label v-if="activeTab === 'orders'"
            >排序<select v-model="sort" aria-label="挂单排序">
              <option value="asc">价格从低到高</option>
              <option value="desc">价格从高到低</option>
            </select></label
          >
        </div>
      </div>
      <div v-if="activeTab === 'orders'" class="order-table-wrap">
        <table class="bot-table order-table">
          <thead>
            <tr>
              <th>方向 / 用途</th>
              <th class="numeric">
                委托价格 <small>{{ quote }}</small>
              </th>
              <th class="numeric">
                剩余 / 原始数量 <small>{{ base }}</small>
              </th>
              <th>状态</th>
              <th><span class="sr-only">订单详情</span></th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="orderRows.length === 0 && availability.orders">
              <td colspan="5" class="empty-state">
                {{
                  orders.length ? "当前筛选下没有挂单" : "交易所当前没有挂单"
                }}
              </td>
            </tr>
            <template v-for="order in orderRows" :key="orderIdentity(order)">
              <tr class="order-row">
                <td data-label="方向 / 用途">
                  <span
                    class="side-tag"
                    :class="order.side === 'Buy' ? 'buy' : 'sell'"
                    >{{ order.side === "Buy" ? "买入" : "卖出" }}</span
                  ><small class="order-purpose">{{
                    reduceOnly(order) ? "只减仓" : "非只减仓"
                  }}</small>
                </td>
                <td data-label="委托价格" class="numeric order-price">
                  {{ formatExactDecimal(order.price) }}
                </td>
                <td data-label="剩余 / 原始数量" class="numeric">
                  {{ orderQuantity(order) }}
                </td>
                <td data-label="状态">
                  <span
                    class="order-state"
                    :class="{ partial: order.status === 'PARTIALLY_FILLED' }"
                    >{{ orderStatusLabel(order.status) }}</span
                  >
                </td>
                <td class="row-action">
                  <button
                    class="record-expand"
                    type="button"
                    :aria-label="`订单 ${orderIdentity(order)} 详情`"
                    :aria-expanded="expanded === orderIdentity(order)"
                    @click="toggle(orderIdentity(order))"
                  >
                    详情
                    <span aria-hidden="true">{{
                      expanded === orderIdentity(order) ? "−" : "+"
                    }}</span>
                  </button>
                </td>
              </tr>
              <tr v-if="expanded === orderIdentity(order)" class="expanded-row">
                <td colspan="5">
                  <dl class="record-definition">
                    <div>
                      <dt>交易所订单号</dt>
                      <dd>{{ order.order_id ?? order.orderId ?? "--" }}</dd>
                    </div>
                    <div>
                      <dt>客户端订单号</dt>
                      <dd>
                        {{ order.order_link_id ?? order.orderLinkId ?? "--" }}
                      </dd>
                    </div>
                    <div>
                      <dt>已成交数量</dt>
                      <dd>
                        {{ formatExactDecimal(order.executed_qty) }} {{ base }}
                      </dd>
                    </div>
                    <div>
                      <dt>只减仓标记</dt>
                      <dd>
                        {{
                          reduceOnly(order)
                            ? "是，仅减少持仓"
                            : "否，不代表一定开仓"
                        }}
                      </dd>
                    </div>
                    <div>
                      <dt>委托时间</dt>
                      <dd>{{ formatTimestamp(order.created_time) }}</dd>
                    </div>
                  </dl>
                  <p class="bot-footnote">
                    剩余数量来自交易所，不强制取整。“只减仓”是订单标记，不用于推断订单归属。
                  </p>
                </td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
      <div v-else-if="activeTab === 'trades'" class="execution-list">
        <p class="bot-footnote">
          按成交时间倒序；一张订单可能分多笔成交。单笔已实现盈亏不等于网格配对净利润，数量仅代表当前已加载记录。
        </p>
        <p
          v-if="tradeRows.length === 0 && availability.trades"
          class="empty-state"
        >
          暂无符合筛选的成交
        </p>
        <article
          v-for="(trade, index) in tradeRows"
          :key="tradeKey(trade, index)"
          class="execution-item"
        >
          <button
            class="execution-summary"
            type="button"
            :aria-expanded="expanded === tradeKey(trade, index)"
            @click="toggle(tradeKey(trade, index))"
          >
            <span class="execution-identity"
              ><span
                class="side-tag"
                :class="trade.side === 'Buy' ? 'buy' : 'sell'"
                >{{ trade.side === "Buy" ? "买入" : "卖出" }}</span
              ><span
                ><strong
                  >{{ formatExactDecimal(trade.price) }}
                  <small>{{ quote }}</small></strong
                ><time>{{ formatTimestamp(trade.time) }}</time></span
              ></span
            >
            <span class="execution-quantity"
              ><strong
                >{{ formatExactDecimal(trade.qty) }}
                <small>{{ base }}</small></strong
              ><small>{{
                trade.is_maker === true
                  ? "挂单成交 · Maker"
                  : trade.is_maker === false
                    ? "吃单成交 · Taker"
                    : "成交类型未知"
              }}</small></span
            >
            <span aria-hidden="true" class="expand-glyph">{{
              expanded === tradeKey(trade, index) ? "−" : "+"
            }}</span>
          </button>
          <dl
            v-if="expanded === tradeKey(trade, index)"
            class="record-definition"
          >
            <div>
              <dt>成交金额</dt>
              <dd>{{ formatExactDecimal(trade.volume) }} {{ quote }}</dd>
            </div>
            <div>
              <dt>手续费折算</dt>
              <dd>
                {{ formatExactDecimal(trade.fee_usdt ?? trade.fee) }}
                {{ feeDisplayAsset(trade) }}
              </dd>
            </div>
            <div>
              <dt>单笔已实现盈亏</dt>
              <dd>
                {{ formatExactDecimal(trade.realized_pnl ?? trade.profit) }}
                {{ quote }}
              </dd>
            </div>
            <div>
              <dt>订单号 / 成交号</dt>
              <dd>
                {{ trade.order_id || "--" }} / {{ trade.trade_id || "--" }}
              </dd>
            </div>
          </dl>
        </article>
      </div>
      <div v-else-if="activeTab === 'history'" class="execution-list">
        <p class="bot-footnote">
          以下是历次策略运行记录，不是单笔成交配对记录。
        </p>
        <p
          v-if="historyRows.length === 0 && availability.history"
          class="empty-state"
        >
          该交易所、交易对暂无历史策略
        </p>
        <article
          v-for="(run, index) in historyRows"
          :key="runKey(run, index)"
          class="execution-item"
        >
          <button
            class="execution-summary"
            type="button"
            :aria-expanded="expanded === runKey(run, index)"
            @click="toggle(runKey(run, index))"
          >
            <span class="execution-identity"
              ><span
                ><strong
                  >{{ directionName(run.direction) }} ·
                  {{
                    run.grid_mode === "geometric"
                      ? "等比网格"
                      : run.grid_mode === "arithmetic"
                        ? "等差网格"
                        : "--"
                  }}</strong
                ><time>{{ formatTimestamp(run.started_at) }}</time></span
              ><span class="order-state">{{ runStatus(run) }}</span></span
            >
            <span class="execution-quantity"
              ><strong
                :class="{
                  positive:
                    runProfit(run) != null && Number(runProfit(run)) >= 0,
                  negative: Number(runProfit(run)) < 0,
                }"
                >{{ formatNumber(runProfit(run), 6) }}
                <small>{{ quote }}</small></strong
              ><small>{{
                run.total_equity_profit != null ? "总权益利润" : "净利润"
              }}</small></span
            ><span class="expand-glyph" aria-hidden="true">{{
              expanded === runKey(run, index) ? "−" : "+"
            }}</span>
          </button>
          <dl v-if="expanded === runKey(run, index)" class="record-definition">
            <div>
              <dt>完成配对</dt>
              <dd>{{ run.completed_pairs ?? "--" }}</dd>
            </div>
            <div>
              <dt>手续费</dt>
              <dd>{{ formatNumber(run.total_fee, 6) }} {{ quote }}</dd>
            </div>
            <div>
              <dt>总交易量</dt>
              <dd>{{ formatNumber(run.total_volume, 2) }} {{ quote }}</dd>
            </div>
            <div>
              <dt>每格数量</dt>
              <dd>{{ formatExactDecimal(run.grid_order_qty) }} {{ base }}</dd>
            </div>
            <div>
              <dt>开仓限价</dt>
              <dd>
                {{ formatExactDecimal(run.initial_order_price) }} {{ quote }}
              </dd>
            </div>
          </dl>
        </article>
      </div>
      <div v-else class="position-list">
        <p class="bot-footnote">
          交易所账户持仓可能包含手动仓位；不等同于本轮网格净仓。
        </p>
        <p
          v-if="positions.length === 0 && availability.positions"
          class="empty-state"
        >
          交易所当前没有该交易对持仓
        </p>
        <article
          v-for="position in positions"
          :key="`${position.side}:${position.entry_price}`"
          class="position-item"
        >
          <header>
            <strong :class="position.side === 'Buy' ? 'positive' : 'negative'"
              >{{ position.side === "Buy" ? "多仓" : "空仓" }} ·
              {{ base }}</strong
            ><span>{{
              position.leverage ? `${position.leverage}x` : "--"
            }}</span>
          </header>
          <dl class="record-definition">
            <div>
              <dt>数量</dt>
              <dd>{{ formatExactDecimal(position.size) }}</dd>
            </div>
            <div>
              <dt>开仓均价</dt>
              <dd>{{ formatExactDecimal(position.entry_price) }}</dd>
            </div>
            <div>
              <dt>标记价</dt>
              <dd>{{ formatExactDecimal(position.mark_price) }}</dd>
            </div>
            <div>
              <dt>未实现盈亏 ({{ quote }})</dt>
              <dd>{{ formatExactDecimal(position.unrealised_pnl) }}</dd>
            </div>
            <div>
              <dt>强平价</dt>
              <dd>{{ formatExactDecimal(position.liq_price) }}</dd>
            </div>
          </dl>
        </article>
      </div>
      <footer
        v-if="activeTab !== 'positions' && total > 0"
        class="record-pagination"
      >
        <span
          >已加载 {{ total }} {{ activeTab === "history" ? "轮" : "条" }} · 每页
          {{ pageSize }} 条<span v-if="!availability[activeTab]">
            · 上次快照</span
          ></span
        >
        <div>
          <button
            class="ghost-button"
            type="button"
            :disabled="page <= 1"
            aria-label="上一页"
            @click="page--"
          >
            上一页</button
          ><span>{{ page }} / {{ pages }}</span
          ><button
            class="ghost-button"
            type="button"
            :disabled="page >= pages"
            aria-label="下一页"
            @click="page++"
          >
            下一页
          </button>
        </div>
      </footer>
    </template>
  </section>
</template>
