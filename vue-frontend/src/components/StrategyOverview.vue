<script setup lang="ts">
import { ref, watch } from "vue";
import type { GridStatus, RiskSnapshot } from "../api/types";
import { directionName, formatNumber } from "../format";

const props = withDefaults(defineProps<{
  status: GridStatus | null;
  risk: RiskSnapshot | null;
  stopBusy?: boolean;
  stopError?: string;
}>(), {
  stopBusy: false,
  stopError: "",
});

const emit = defineEmits<{ stop: [] }>();
const stopConfirmation = ref(false);

watch(
  () => [props.status?.run_id, props.status?.running, props.stopBusy],
  () => {
    if (!props.status?.running || props.stopBusy) stopConfirmation.value = false;
  },
);

function requestStop(): void {
  if (!props.status?.running || props.stopBusy) return;
  if (!stopConfirmation.value) {
    stopConfirmation.value = true;
    return;
  }
  stopConfirmation.value = false;
  emit("stop");
}
</script>

<template>
  <section class="panel-card strategy-overview">
    <header class="section-header">
      <div>
        <p class="eyebrow">策略实时状态</p>
        <h2>{{ status?.symbol || "未选择策略" }}</h2>
      </div>
      <div class="strategy-actions">
        <span class="live-pill" :class="status?.running ? 'running' : 'stopped'">
          {{ status?.waiting_initial_order ? "等待开仓" : status?.waiting_trigger ? "等待触发" : status?.running ? "运行中" : "未运行" }}
        </span>
        <button
          v-if="status?.running"
          class="ghost-button stop-button"
          type="button"
          :disabled="stopBusy"
          @click="requestStop"
        >
          {{ stopBusy ? "正在停止…" : stopConfirmation ? "确认停止（只撤单）" : "停止策略" }}
        </button>
      </div>
    </header>

    <p v-if="!status" class="empty-state">从上方策略列表选择一个交易对查看明细。</p>
    <template v-else>
      <div v-if="risk?.has_risk" class="callout danger">
        风险核对未通过。当前页面仅展示状态，不执行自动补救操作。
      </div>
      <div class="metric-grid">
        <div><span>方向</span><strong>{{ directionName(status.direction) }}</strong></div>
        <div><span>模式</span><strong>{{ status.grid_mode === "geometric" ? "等比" : "等差" }}</strong></div>
        <div><span>总权益利润</span><strong :class="Number(status.total_equity_profit ?? status.total_profit ?? 0) >= 0 ? 'positive' : 'negative'">{{ formatNumber(status.total_equity_profit ?? status.total_profit, 4) }} USDT</strong></div>
        <div><span>已实现净利润</span><strong>{{ formatNumber(status.realized_net_profit ?? status.total_profit, 4) }} USDT</strong></div>
        <div><span>未实现盈亏</span><strong>{{ formatNumber(status.unrealised_pnl, 4) }} USDT</strong></div>
        <div><span>手续费</span><strong>{{ formatNumber(status.total_fee, 4) }} USDT</strong></div>
        <div><span>总交易量</span><strong>{{ formatNumber(status.total_volume, 2) }} USDT</strong></div>
        <div><span>网格净持仓</span><strong>{{ formatNumber(status.grid_position_net_qty, 8) }}</strong></div>
        <div><span>完成配对</span><strong>{{ status.completed_pairs ?? 0 }}</strong></div>
      </div>
      <p v-if="status.trigger_message" class="form-hint">{{ status.trigger_message }}</p>
      <p v-if="stopError" class="form-error">{{ stopError }}</p>
    </template>
  </section>
</template>
