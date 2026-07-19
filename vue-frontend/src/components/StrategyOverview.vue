<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { GridStatus, RiskSnapshot } from "../api/types";
import {
  directionName,
  formatNumber,
  strategyCanStop,
  strategyStatusLabel,
  strategyStatusTone,
} from "../format";

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
const currentRisk = computed(() => {
  if (!props.status?.run_id || !props.risk) return null;
  if (
    props.risk.run_id !== props.status.run_id
    || props.risk.exchange !== props.status.exchange
    || props.risk.symbol !== props.status.symbol
  ) {
    return null;
  }
  return props.risk;
});
const realizedNetProfit = computed(
  () => currentRisk.value?.realized_net_profit ?? props.status?.realized_net_profit ?? props.status?.total_profit,
);
const totalEquityProfit = computed(() => currentRisk.value?.total_equity_profit ?? null);
const gridUnrealizedProfit = computed(
  () => currentRisk.value?.grid_unrealised_pnl ?? currentRisk.value?.unrealised_pnl ?? null,
);
const totalFee = computed(() => currentRisk.value?.total_fee ?? props.status?.total_fee);
const totalVolume = computed(() => currentRisk.value?.total_volume ?? props.status?.total_volume);
const completedPairs = computed(
  () => currentRisk.value?.completed_pairs ?? props.status?.completed_pairs ?? 0,
);
const canStop = computed(() => strategyCanStop(props.status));
const statusLabel = computed(() => strategyStatusLabel(props.status));
const statusTone = computed(() => strategyStatusTone(props.status));
const manualStopPending = computed(
  () => props.status?.manual_stop_pending === true || props.status?.lifecycle === "stop_requested",
);

watch(
  () => [props.status?.run_id, props.status?.lifecycle, canStop.value, props.stopBusy],
  () => {
    if (!canStop.value || props.stopBusy) stopConfirmation.value = false;
  },
);

function requestStop(): void {
  if (!canStop.value || props.stopBusy) return;
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
        <span class="live-pill" :class="statusTone">
          {{ statusLabel }}
        </span>
        <button
          v-if="canStop"
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
      <div v-if="manualStopPending" class="callout">
        停止请求已保存。程序正在核对成交、手续费并确认策略订单全部终态；期间不会继续补单，也不会主动平仓。
      </div>
      <div v-if="currentRisk?.has_risk" class="callout danger">
        风险核对未通过。当前页面仅展示状态，不执行自动补救操作。
      </div>
      <div class="metric-grid">
        <div><span>方向</span><strong>{{ directionName(status.direction) }}</strong></div>
        <div><span>模式</span><strong>{{ status.grid_mode === "geometric" ? "等比" : "等差" }}</strong></div>
        <div><span>总权益利润</span><strong :class="Number(totalEquityProfit ?? 0) >= 0 ? 'positive' : 'negative'">{{ formatNumber(totalEquityProfit, 4) }} USDT</strong></div>
        <div><span>已实现净利润</span><strong>{{ formatNumber(realizedNetProfit, 4) }} USDT</strong></div>
        <div><span>网格未实现盈亏</span><strong>{{ formatNumber(gridUnrealizedProfit, 4) }} USDT</strong></div>
        <div><span>手续费</span><strong>{{ formatNumber(totalFee, 4) }} USDT</strong></div>
        <div><span>总交易量</span><strong>{{ formatNumber(totalVolume, 2) }} USDT</strong></div>
        <div><span>网格净持仓</span><strong>{{ formatNumber(status.grid_position_net_qty, 8) }}</strong></div>
        <div><span>完成配对</span><strong>{{ completedPairs }}</strong></div>
      </div>
      <p v-if="!currentRisk" class="form-hint">总权益利润等待当前策略的交易所权威风险快照，不使用旧数据或账户整仓盈亏代替。</p>
      <p v-if="status.trigger_message" class="form-hint">{{ status.trigger_message }}</p>
      <p v-if="stopError" class="form-error">{{ stopError }}</p>
    </template>
  </section>
</template>
