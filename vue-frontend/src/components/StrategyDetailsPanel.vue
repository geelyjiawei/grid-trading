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
  formatNumber,
  formatTimestamp,
} from "../format";

defineProps<{
  exchange: Exchange;
  symbol: string;
  configured: boolean;
  loading: boolean;
  error: string;
  positions: PositionSnapshot[];
  orders: GridOrder[];
  trades: GridTrade[];
  history: GridHistoryRun[];
}>();

const emit = defineEmits<{
  refresh: [];
}>();

type DetailTab = "positions" | "orders" | "trades" | "history";
const activeTab = ref<DetailTab>("positions");
const tabs: Array<{ key: DetailTab; label: string }> = [
  { key: "positions", label: "持仓" },
  { key: "orders", label: "挂单" },
  { key: "trades", label: "成交" },
  { key: "history", label: "历史" },
];

function runProfit(run: GridHistoryRun): string | number | undefined {
  return run.total_equity_profit ?? run.net_profit;
}

function reduceOnly(order: GridOrder): boolean {
  return Boolean(order.reduce_only ?? order.reduceOnly);
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
        <span v-if="tab.key === 'positions'">{{ positions.length }}</span>
        <span v-else-if="tab.key === 'orders'">{{ orders.length }}</span>
        <span v-else-if="tab.key === 'trades'">{{ trades.length }}</span>
        <span v-else>{{ history.length }}</span>
      </button>
    </nav>

    <p v-if="!configured" class="empty-state">请先配置当前交易所，再读取真实持仓和订单。</p>
    <p v-else-if="error" class="callout danger">{{ error }}</p>

    <div v-if="configured && activeTab === 'positions'" class="detail-content card-list">
      <p v-if="positions.length === 0" class="empty-state">交易所当前没有该交易对持仓</p>
      <article v-for="position in positions" :key="`${position.side}:${position.entry_price}`" class="position-item">
        <header>
          <strong :class="position.side === 'Buy' ? 'positive' : 'negative'">
            {{ position.side === "Buy" ? "多仓" : "空仓" }}
          </strong>
          <span>{{ position.leverage ? `${position.leverage}x` : "--" }}</span>
        </header>
        <div class="detail-metrics">
          <div><span>数量</span><strong>{{ formatNumber(position.size, 8) }}</strong></div>
          <div><span>开仓均价</span><strong>{{ formatNumber(position.entry_price, 8) }}</strong></div>
          <div><span>标记价</span><strong>{{ formatNumber(position.mark_price, 8) }}</strong></div>
          <div><span>未实现盈亏</span><strong>{{ formatNumber(position.unrealised_pnl, 6) }}</strong></div>
          <div><span>强平价</span><strong>{{ formatNumber(position.liq_price, 8) }}</strong></div>
        </div>
      </article>
    </div>

    <div v-else-if="configured && activeTab === 'orders'" class="detail-content table-scroll">
      <table>
        <thead><tr><th>方向</th><th>价格</th><th>数量</th><th>用途</th><th>状态</th><th>客户端订单号</th></tr></thead>
        <tbody>
          <tr v-if="orders.length === 0"><td colspan="6" class="empty-state">交易所当前没有挂单</td></tr>
          <tr v-for="order in orders" :key="order.order_id ?? order.orderId ?? order.order_link_id ?? order.orderLinkId">
            <td :class="order.side === 'Buy' ? 'positive' : 'negative'">{{ order.side }}</td>
            <td>{{ formatNumber(order.price, 8) }}</td>
            <td>{{ formatNumber(order.qty, 8) }}</td>
            <td>{{ reduceOnly(order) ? "止盈/平仓" : "开仓/补仓" }}</td>
            <td>{{ order.status || "--" }}</td>
            <td class="mono-cell">{{ order.order_link_id ?? order.orderLinkId ?? "--" }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else-if="configured && activeTab === 'trades'" class="detail-content table-scroll">
      <table>
        <thead><tr><th>方向</th><th>价格</th><th>数量</th><th>交易量</th><th>手续费</th><th>流动性</th><th>已实现盈亏</th><th>时间</th></tr></thead>
        <tbody>
          <tr v-if="trades.length === 0"><td colspan="8" class="empty-state">暂无成交</td></tr>
          <tr v-for="trade in trades" :key="`${trade.order_id}:${trade.trade_id}`">
            <td :class="trade.side === 'Buy' ? 'positive' : 'negative'">{{ trade.side }}</td>
            <td>{{ formatNumber(trade.price, 8) }}</td>
            <td>{{ formatNumber(trade.qty, 8) }}</td>
            <td>{{ formatNumber(trade.volume, 8) }}</td>
            <td>{{ formatNumber(trade.fee_usdt ?? trade.fee, 8) }} {{ trade.fee_asset || "" }}</td>
            <td>{{ trade.is_maker === true ? "挂单" : trade.is_maker === false ? "吃单" : trade.liquidity || "--" }}</td>
            <td>{{ formatNumber(trade.realized_pnl ?? trade.profit, 8) }}</td>
            <td>{{ formatTimestamp(trade.time) }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else-if="configured" class="detail-content table-scroll">
      <table>
        <thead><tr><th>开始时间</th><th>交易所</th><th>交易对</th><th>方向</th><th>模式</th><th>状态</th><th>净利润</th><th>手续费</th><th>交易量</th><th>配对</th></tr></thead>
        <tbody>
          <tr v-if="history.length === 0"><td colspan="10" class="empty-state">暂无历史</td></tr>
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
