<script setup lang="ts">
import { ref } from "vue";
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
} from "../format";

type DetailTab = "positions" | "orders" | "trades" | "history";

const props = withDefaults(defineProps<{
  exchange: Exchange;
  symbol: string;
  configured: boolean;
  loading: boolean;
  error: string;
  availability?: Record<DetailTab, boolean>;
  positions: PositionSnapshot[];
  orders: GridOrder[];
  trades: GridTrade[];
  history: GridHistoryRun[];
}>(), {
  availability: () => ({
    positions: true,
    orders: true,
    trades: true,
    history: true,
  }),
});

const emit = defineEmits<{
  refresh: [];
}>();

const activeTab = ref<DetailTab>("positions");
const tabs: Array<{ key: DetailTab; label: string }> = [
  { key: "positions", label: "持仓" },
  { key: "orders", label: "挂单" },
  { key: "trades", label: "成交" },
  { key: "history", label: "历史" },
];

function tabCount(tab: DetailTab): number | string {
  if (!props.availability[tab]) return "--";
  if (tab === "positions") return props.positions.length;
  if (tab === "orders") return props.orders.length;
  if (tab === "trades") return props.trades.length;
  return props.history.length;
}

function unavailableMessage(tab: DetailTab): string {
  const hasPreviousData = tab === "positions"
    ? props.positions.length > 0
    : tab === "orders"
      ? props.orders.length > 0
      : tab === "trades"
        ? props.trades.length > 0
        : props.history.length > 0;
  return hasPreviousData
    ? "本次实时读取失败，以下保留上次成功快照，不能视为当前实时数据。"
    : "本次实时读取失败，当前数量未知，不能按 0 处理。";
}

function runProfit(run: GridHistoryRun): string | number | undefined {
  return run.total_equity_profit ?? run.net_profit;
}

function reduceOnly(order: GridOrder): boolean {
  return Boolean(order.reduce_only ?? order.reduceOnly);
}

function orderQuantity(order: GridOrder): string {
  const remaining = formatExactDecimal(order.remaining_qty ?? order.qty);
  if (order.status !== "PARTIALLY_FILLED" || order.original_qty === undefined) {
    return remaining;
  }
  return `${remaining} / ${formatExactDecimal(order.original_qty)}`;
}

function feeDisplayAsset(trade: GridTrade): string {
  if (trade.fee_usdt !== null && trade.fee_usdt !== undefined) {
    return trade.fee_quote_asset || "USDT";
  }
  return trade.fee_asset || "";
}
</script>

<template>
  <section class="panel-card detail-panel dashboard-span">
    <header class="section-header detail-header">
      <div>
        <p class="eyebrow">交易所权威数据</p>
        <h2>{{ exchangeName(exchange) }} · {{ symbol }}</h2>
      </div>
      <button class="ghost-button" type="button" :disabled="loading || !configured" @click="emit('refresh')">
        {{ loading ? "读取中…" : "刷新明细" }}
      </button>
    </header>

    <nav class="detail-tabs" aria-label="策略明细">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        type="button"
        :class="{ active: activeTab === tab.key }"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
        <span>{{ tabCount(tab.key) }}</span>
      </button>
    </nav>

    <p v-if="!configured" class="empty-state">请先配置当前交易所，再读取真实持仓和订单。</p>
    <p v-else-if="error" class="callout danger">{{ error }}</p>

    <div v-if="configured && activeTab === 'positions'" class="detail-content card-list">
      <p v-if="!availability.positions" class="empty-state">{{ unavailableMessage("positions") }}</p>
      <p v-else-if="positions.length === 0" class="empty-state">交易所当前没有该交易对持仓</p>
      <article v-for="position in positions" :key="`${position.side}:${position.entry_price}`" class="position-item">
        <header>
          <strong :class="position.side === 'Buy' ? 'positive' : 'negative'">
            {{ position.side === "Buy" ? "多仓" : "空仓" }}
          </strong>
          <span>{{ position.leverage ? `${position.leverage}x` : "--" }}</span>
        </header>
        <div class="detail-metrics">
          <div><span>数量</span><strong>{{ formatExactDecimal(position.size) }}</strong></div>
          <div><span>开仓均价</span><strong>{{ formatExactDecimal(position.entry_price) }}</strong></div>
          <div><span>标记价</span><strong>{{ formatExactDecimal(position.mark_price) }}</strong></div>
          <div><span>未实现盈亏</span><strong>{{ formatExactDecimal(position.unrealised_pnl) }}</strong></div>
          <div><span>强平价</span><strong>{{ formatExactDecimal(position.liq_price) }}</strong></div>
        </div>
      </article>
    </div>

    <div v-else-if="configured && activeTab === 'orders'" class="detail-content table-scroll">
      <p v-if="!availability.orders" class="empty-state">{{ unavailableMessage("orders") }}</p>
      <table>
        <thead><tr><th>方向</th><th>价格</th><th>数量</th><th>用途</th><th>状态</th><th>客户端订单号</th></tr></thead>
        <tbody>
          <tr v-if="availability.orders && orders.length === 0"><td colspan="6" class="empty-state">交易所当前没有挂单</td></tr>
          <tr v-for="order in orders" :key="order.order_id ?? order.orderId ?? order.order_link_id ?? order.orderLinkId">
            <td :class="order.side === 'Buy' ? 'positive' : 'negative'">{{ order.side }}</td>
            <td>{{ formatExactDecimal(order.price) }}</td>
            <td>{{ orderQuantity(order) }}</td>
            <td>{{ reduceOnly(order) ? "止盈/平仓" : "开仓/补仓" }}</td>
            <td>{{ order.status || "--" }}</td>
            <td class="mono-cell">{{ order.order_link_id ?? order.orderLinkId ?? "--" }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else-if="configured && activeTab === 'trades'" class="detail-content table-scroll">
      <p v-if="!availability.trades" class="empty-state">{{ unavailableMessage("trades") }}</p>
      <table>
        <thead><tr><th>方向</th><th>价格</th><th>数量</th><th>交易量</th><th>手续费折算</th><th>流动性</th><th>已实现盈亏</th><th>时间</th></tr></thead>
        <tbody>
          <tr v-if="availability.trades && trades.length === 0"><td colspan="8" class="empty-state">暂无成交</td></tr>
          <tr v-for="trade in trades" :key="`${trade.order_id}:${trade.trade_id}`">
            <td :class="trade.side === 'Buy' ? 'positive' : 'negative'">{{ trade.side }}</td>
            <td>{{ formatExactDecimal(trade.price) }}</td>
            <td>{{ formatExactDecimal(trade.qty) }}</td>
            <td>{{ formatExactDecimal(trade.volume) }}</td>
            <td>{{ formatExactDecimal(trade.fee_usdt ?? trade.fee) }} {{ feeDisplayAsset(trade) }}</td>
            <td>{{ trade.is_maker === true ? "挂单" : trade.is_maker === false ? "吃单" : trade.liquidity || "--" }}</td>
            <td>{{ formatExactDecimal(trade.realized_pnl ?? trade.profit) }}</td>
            <td>{{ formatTimestamp(trade.time) }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else-if="configured" class="detail-content table-scroll">
      <p v-if="!availability.history" class="empty-state">{{ unavailableMessage("history") }}</p>
      <table>
        <thead><tr><th>开始时间</th><th>交易所</th><th>交易对</th><th>方向</th><th>模式</th><th>状态</th><th>净利润</th><th>手续费</th><th>交易量</th><th>配对</th></tr></thead>
        <tbody>
          <tr v-if="availability.history && history.length === 0"><td colspan="10" class="empty-state">暂无历史</td></tr>
          <tr v-for="run in history" :key="`${run.started_at}:${run.exchange}:${run.symbol}`">
            <td>{{ formatTimestamp(run.started_at) }}</td>
            <td>{{ run.exchange ? exchangeName(run.exchange) : "--" }}</td>
            <td>{{ run.symbol || "--" }}</td>
            <td>{{ directionName(run.direction) }}</td>
            <td>{{ run.grid_mode === "geometric" ? "等比" : "等差" }}</td>
            <td>{{ run.status || "--" }}</td>
            <td :class="Number(runProfit(run) ?? 0) >= 0 ? 'positive' : 'negative'">{{ formatNumber(runProfit(run), 6) }}</td>
            <td>{{ formatNumber(run.total_fee, 6) }}</td>
            <td>{{ formatNumber(run.total_volume, 2) }}</td>
            <td>{{ run.completed_pairs ?? 0 }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
