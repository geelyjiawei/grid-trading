<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { GridStatus, RiskSnapshot } from "../api/types";
import {
  directionName,
  formatExactDecimal,
  formatNumber,
  quoteAsset,
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
const settlementAsset = computed(() => props.status ? quoteAsset(props.status.exchange) : "USDT");
const quantityAsset = computed(() => {
  const symbol = props.status?.symbol ?? "";
  const quote = settlementAsset.value;
  return symbol.endsWith(quote) ? symbol.slice(0, -quote.length) : "标的";
});
const hasGridSpecification = computed(
  () => props.status?.lower_price != null
    && props.status?.upper_price != null
    && props.status?.grid_count != null,
);
const sizingLabel = computed(() => (
  props.status?.position_sizing_mode === "fixed_grid_qty" ? "固定每格数量" : "按投入金额"
));
const openingOrderLabel = computed(() => {
  const type = props.status?.initial_order_type;
  if (type === "market") return "市价";
  if (type === "post_only") return "Post Only 限价";
  if (type === "limit") return "限价";
  return "--";
});
const openingOrderDisplay = computed(() => {
  const price = props.status?.initial_order_price;
  return price == null
    ? openingOrderLabel.value
    : `${openingOrderLabel.value} · ${formatExactDecimal(price)}`;
});
const activeGridDisplay = computed(() => {
  const active = props.status?.active_grid_count;
  const participating = props.status?.participating_level_count;
  if (active == null) return "-- 格";
  return participating == null ? `${active} 格` : `${active} / ${participating} 格`;
});
const baselineSignedQuantity = computed(
  () => props.status?.baseline_position?.signed_qty
    ?? signedQuantity(props.status?.baseline_position?.side, props.status?.baseline_position?.qty),
);
const actualPosition = computed(() => currentRisk.value?.actual_position_net_qty ?? null);
const unmanagedPositionDelta = computed(() => currentRisk.value?.unmanaged_delta_qty ?? null);
const riskPositionSummary = computed(() => {
  if (!currentRisk.value?.unmanaged_position) return "";
  return `台账应有 ${positionLabel(props.status?.expected_position_net_qty)}，交易所实际 ${
    positionLabel(actualPosition.value)
  }，未归属差额 ${positionLabel(unmanagedPositionDelta.value)}。`;
});

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

function signedQuantity(
  side: "Buy" | "Sell" | undefined,
  quantity: string | number | undefined,
): string | null {
  const text = typeof quantity === "string" ? quantity.trim() : String(quantity ?? "");
  const match = /^[+-]?(\d+(?:\.\d+)?)$/.exec(text);
  if (!match) return null;
  if (side === "Sell") return `-${match[1]}`;
  return match[1];
}

function positionLabel(value: unknown): string {
  const text = typeof value === "string" ? value.trim() : String(value ?? "");
  const match = /^([+-]?)(\d+(?:\.\d+)?)$/.exec(text);
  if (!match) return "--";
  if (/^0+(?:\.0+)?$/.test(match[2])) return `0 ${quantityAsset.value}`;
  const side = match[1] === "-" ? "空" : "多";
  return `${side} ${formatExactDecimal(match[2])} ${quantityAsset.value}`;
}
</script>

<template>
  <section class="panel-card strategy-overview">
    <header class="section-header">
      <div>
        <p class="eyebrow">策略实时状态</p>
        <h2>{{ status?.symbol || "未选择策略" }}</h2>
        <p v-if="status" class="section-subtitle">
          {{ directionName(status.direction) }} · {{ status.grid_mode === "geometric" ? "等比网格" : "等差网格" }} · 交易所权威核对
        </p>
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
        <strong>风险核对未通过，当前页面仅展示状态，不执行自动补救操作。</strong>
        <span v-if="riskPositionSummary">{{ riskPositionSummary }}</span>
      </div>
      <section v-if="hasGridSpecification" class="strategy-specification" aria-label="网格策略参数">
        <header class="strategy-specification-header">
          <div>
            <p class="eyebrow">本轮策略参数</p>
            <h3>网格设置与仓位归属</h3>
          </div>
          <span class="strategy-specification-badge">{{ sizingLabel }}</span>
        </header>
        <dl class="strategy-specification-grid">
          <div>
            <dt>价格区间</dt>
            <dd>{{ formatExactDecimal(status.lower_price) }} → {{ formatExactDecimal(status.upper_price) }}</dd>
          </div>
          <div>
            <dt>网格数量</dt>
            <dd>{{ status.grid_count }} 格</dd>
          </div>
          <div>
            <dt>每格数量</dt>
            <dd>
              <template v-if="status.position_sizing_mode === 'fixed_grid_qty'">
                {{ formatExactDecimal(status.grid_order_qty) }} {{ quantityAsset }}
              </template>
              <template v-else>动态分配</template>
            </dd>
          </div>
          <div>
            <dt>杠杆</dt>
            <dd>{{ status.leverage != null ? `${status.leverage}x` : "--" }}</dd>
          </div>
          <div>
            <dt>开仓方式</dt>
            <dd>{{ openingOrderDisplay }}</dd>
          </div>
          <div>
            <dt>启动参与网格</dt>
            <dd>{{ activeGridDisplay }}</dd>
          </div>
          <div>
            <dt>计划初始网格仓</dt>
            <dd>{{ formatExactDecimal(status.planned_total_qty) }} {{ quantityAsset }}</dd>
          </div>
          <div>
            <dt>已成交初始网格仓</dt>
            <dd>{{ formatExactDecimal(status.opening_filled_qty) }} {{ quantityAsset }}</dd>
          </div>
        </dl>
        <dl class="strategy-position-ledger">
          <div>
            <dt>启动前旧仓</dt>
            <dd>{{ positionLabel(baselineSignedQuantity) }}</dd>
            <small>独立保留，不归本轮网格</small>
          </div>
          <div>
            <dt>当前网格净仓</dt>
            <dd>{{ positionLabel(status.grid_position_net_qty) }}</dd>
            <small>仅统计本轮策略成交</small>
          </div>
          <div>
            <dt>台账应有总仓</dt>
            <dd>{{ positionLabel(status.expected_position_net_qty) }}</dd>
            <small>旧仓与网格仓合计</small>
          </div>
          <div>
            <dt>交易所实际总仓</dt>
            <dd>{{ positionLabel(actualPosition) }}</dd>
            <small>来自当前权威风险快照</small>
          </div>
          <div :class="{ 'position-ledger-risk': currentRisk?.unmanaged_position }">
            <dt>未归属差额</dt>
            <dd>{{ positionLabel(unmanagedPositionDelta) }}</dd>
            <small>不属于本轮策略台账</small>
          </div>
        </dl>
      </section>
      <div class="metric-grid strategy-metrics">
        <div class="metric-primary"><span>总权益利润</span><strong :class="Number(totalEquityProfit ?? 0) >= 0 ? 'positive' : 'negative'">{{ formatNumber(totalEquityProfit, 4) }} {{ settlementAsset }}</strong></div>
        <div><span>已实现净利润</span><strong>{{ formatNumber(realizedNetProfit, 4) }} {{ settlementAsset }}</strong></div>
        <div><span>网格未实现盈亏</span><strong>{{ formatNumber(gridUnrealizedProfit, 4) }} {{ settlementAsset }}</strong></div>
        <div><span>手续费</span><strong>{{ formatNumber(totalFee, 4) }} {{ settlementAsset }}</strong></div>
        <div><span>总交易量</span><strong>{{ formatNumber(totalVolume, 2) }} {{ settlementAsset }}</strong></div>
        <div><span>网格净持仓</span><strong>{{ formatNumber(status.grid_position_net_qty, 8) }}</strong></div>
        <div><span>完成配对</span><strong>{{ completedPairs }}</strong></div>
        <div><span>方向 / 模式</span><strong>{{ directionName(status.direction) }} · {{ status.grid_mode === "geometric" ? "等比" : "等差" }}</strong></div>
      </div>
      <p v-if="!currentRisk" class="form-hint">总权益利润等待当前策略的交易所权威风险快照，不使用旧数据或账户整仓盈亏代替。</p>
      <p v-if="status.trigger_message" class="form-hint">{{ status.trigger_message }}</p>
      <p v-if="stopError" class="form-error">{{ stopError }}</p>
    </template>
  </section>
</template>
