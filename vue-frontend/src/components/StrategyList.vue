<script setup lang="ts">
import type { Exchange, GridStatus } from "../api/types";
import { directionName, exchangeName, formatNumber } from "../format";

defineProps<{
  grids: GridStatus[];
  activeExchange: Exchange;
  activeSymbol: string;
  loading: boolean;
}>();

const emit = defineEmits<{
  select: [grid: GridStatus];
}>();
</script>

<template>
  <section class="strategy-panel panel-card">
    <header class="section-header">
      <div>
        <p class="eyebrow">多交易所策略</p>
        <h2>运行中策略</h2>
      </div>
      <strong class="count-badge">{{ grids.length }}</strong>
    </header>

    <p v-if="loading && grids.length === 0" class="empty-state">正在读取策略…</p>
    <p v-else-if="grids.length === 0" class="empty-state">当前没有运行中的策略</p>
    <button
      v-for="grid in grids"
      :key="`${grid.exchange}:${grid.symbol}`"
      class="strategy-row"
      :class="{ active: grid.exchange === activeExchange && grid.symbol === activeSymbol }"
      type="button"
      @click="emit('select', grid)"
    >
      <span>
        <strong>{{ grid.symbol }}</strong>
        <small>{{ exchangeName(grid.exchange) }} · {{ directionName(grid.direction) }} · {{ grid.grid_mode === "geometric" ? "等比" : "等差" }}</small>
      </span>
      <span class="strategy-profit">
        <strong :class="Number(grid.realized_net_profit ?? grid.total_profit ?? 0) >= 0 ? 'positive' : 'negative'">
          {{ formatNumber(grid.realized_net_profit ?? grid.total_profit, 4) }}
        </strong>
        <small>已实现净利润 · 完成 {{ grid.completed_pairs ?? 0 }} 次</small>
      </span>
    </button>
  </section>
</template>
