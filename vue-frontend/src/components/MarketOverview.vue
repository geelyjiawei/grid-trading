<script setup lang="ts">
import type { BalanceSnapshot, Exchange, FeeRates, PriceSnapshot } from "../api/types";
import { exchangeName, finiteNumber, formatNumber, formatPercent } from "../format";

const props = defineProps<{
  exchange: Exchange;
  symbol: string;
  configured: boolean;
  price: PriceSnapshot | null;
  balance: BalanceSnapshot | null;
  fees: FeeRates | null;
  loading: boolean;
}>();

function changePercent(): number | null {
  const value = finiteNumber(props.price?.price_24h_pcnt);
  return value === null ? null : value * 100;
}
</script>

<template>
  <section class="market-card panel-card">
    <header class="section-header">
      <div>
        <p class="eyebrow">{{ exchangeName(exchange) }} · {{ symbol }}</p>
        <h2>{{ formatNumber(price?.last_price, 8) }}</h2>
      </div>
      <span
        class="market-change"
        :class="(changePercent() ?? 0) >= 0 ? 'positive' : 'negative'"
      >
        {{ formatPercent(changePercent()) }}
      </span>
    </header>

    <div class="metric-grid compact-grid">
      <div><span>标记价格</span><strong>{{ formatNumber(price?.mark_price, 8) }}</strong></div>
      <div><span>24H 成交量</span><strong>{{ formatNumber(price?.volume_24h, 2) }}</strong></div>
      <div><span>可用余额</span><strong>{{ configured ? formatNumber(balance?.available_balance, 4) : "未配置" }}</strong></div>
      <div><span>账户权益</span><strong>{{ configured ? formatNumber(balance?.equity ?? balance?.wallet_balance, 4) : "未配置" }}</strong></div>
      <div><span>未实现盈亏</span><strong>{{ configured ? formatNumber(balance?.unrealised_pnl, 4) : "未配置" }}</strong></div>
      <div><span>Maker / Taker</span><strong>{{ fees ? `${formatNumber(fees.maker_fee_rate * 100, 6)}% / ${formatNumber(fees.taker_fee_rate * 100, 6)}%` : "--" }}</strong></div>
    </div>
    <p v-if="loading" class="loading-line">正在同步交易所数据…</p>
  </section>
</template>
