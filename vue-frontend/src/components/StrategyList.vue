<script setup lang="ts">
import type { Exchange, GridStatus, RiskSnapshot } from "../api/types";
import { directionName, exchangeName, formatNumber, quoteAsset, strategyStatusLabel } from "../format";
import { matchingStrategyRisk, profitTone, strategyNetProfit } from "../strategy-profit";

const props = withDefaults(defineProps<{
  grids: GridStatus[];
  activeExchange: Exchange;
  activeSymbol: string;
  loading: boolean;
  currentRisk?: RiskSnapshot | null;
  risks?: RiskSnapshot[];
}>(), { currentRisk: null, risks: () => [] });

const emit = defineEmits<{
  select: [grid: GridStatus];
}>();

function riskFor(grid: GridStatus): RiskSnapshot | null {
  if (grid.exchange === props.activeExchange && grid.symbol === props.activeSymbol) {
    return matchingStrategyRisk(grid, props.currentRisk);
  }
  return props.risks.find(risk => matchingStrategyRisk(grid, risk)) ?? null;
}

function netProfit(grid: GridStatus) {
  return strategyNetProfit(grid, riskFor(grid));
}
</script>

<template>
  <section class="strategy-panel panel-card">
    <header class="section-header">
      <div>
        <p class="eyebrow">多交易所策略</p>
        <h2>活动策略</h2>
      </div>
      <strong class="count-badge">{{ grids.length }}</strong>
    </header>

    <p v-if="loading && grids.length === 0" class="empty-state">正在读取策略…</p>
    <p v-else-if="grids.length === 0" class="empty-state">当前没有活动策略</p>
    <button
      v-for="grid in grids"
      :key="`${grid.exchange}:${grid.symbol}`"
      class="strategy-row"
      :class="{ active: grid.exchange === activeExchange && grid.symbol === activeSymbol }"
      type="button"
      @click="emit('select', grid)"
    >
      <span class="strategy-identity">
        <span class="strategy-symbol-line">
          <strong>{{ grid.symbol }}</strong>
          <i>{{ strategyStatusLabel(grid) }}</i>
        </span>
        <small>{{ exchangeName(grid.exchange) }} · {{ directionName(grid.direction) }} · {{ grid.grid_mode === "geometric" ? "等比" : "等差" }} · {{ strategyStatusLabel(grid) }}</small>
      </span>
      <span class="strategy-profit">
        <strong :class="profitTone(netProfit(grid))" title="本轮已实现盈亏＋本轮浮动盈亏－成交手续费＋已结算资金费；资金费核对最多延迟60秒；未来平仓手续费尚未发生，未计入">
          {{ netProfit(grid) == null ? "待核对" : `${formatNumber(netProfit(grid), 4)} ${quoteAsset(grid.exchange)}` }}
        </strong>
        <small>净收益（含浮盈亏及资金费） · 完成 {{ riskFor(grid)?.completed_pairs ?? grid.completed_pairs ?? 0 }} 次</small>
      </span>
    </button>
  </section>
</template>
