<script setup lang="ts">
import { computed, onUnmounted, ref, watch } from "vue";
import { ApiError, api } from "../api/client";
import type { GridStatus, ProfitProtection, ProfitProtectionStatus } from "../api/types";
import { quoteAsset } from "../format";

const props = defineProps<{ status: GridStatus; refreshKey: number }>();
const emit = defineEmits<{ unauthorized: [] }>();
const disabledSettings = (): ProfitProtection => ({
  revision: 0, take_profit_amount: null, stop_loss_amount: null, updated_at_ms: null,
});
const configuration = ref(disabledSettings());
const snapshot = ref<ProfitProtectionStatus | null>(null);
const takeProfit = ref("");
const stopLoss = ref("");
const busy = ref(false);
const error = ref("");
const readError = ref("");
const message = ref("");
const dirty = ref(false);
const conflict = ref(false);
let generation = 0;
let readRunning = false;
let disposed = false;
const identity = computed(() => `${props.status.exchange}:${props.status.symbol}:${props.status.run_id ?? ""}`);
const unit = computed(() => quoteAsset(props.status.exchange));
const enabled = computed(() => configuration.value.take_profit_amount !== null || configuration.value.stop_loss_amount !== null);
const trigger = computed(() => props.status.profit_protection_trigger ?? snapshot.value?.trigger);
const editableLifecycles = new Set(["awaiting_opening", "deploying_grid", "running"]);
const editable = computed(() => Boolean(props.status.run_id) && props.status.running
  && (!props.status.lifecycle || editableLifecycles.has(props.status.lifecycle))
  && (!snapshot.value || editableLifecycles.has(snapshot.value.lifecycle)) && !trigger.value
  && !props.status.manual_stop_pending && !props.status.risk_shutdown_pending
  && Number.isSafeInteger(configuration.value.revision));
const label = computed(() => {
  if (trigger.value) return "已触发";
  if (readError.value) return "暂不可用";
  if (!enabled.value) return "未启用";
  return ({ verified: "监控中", monitoring: "监控中", checking: "核对中", unavailable: "暂不可用", triggered: "已触发" } as Record<string, string>)[snapshot.value?.observation.status ?? "checking"] ?? "核对中";
});

function resetDraft(): void {
  takeProfit.value = configuration.value.take_profit_amount ?? "";
  stopLoss.value = configuration.value.stop_loss_amount ?? "";
  dirty.value = false;
  conflict.value = false;
}

function acceptConfiguration(next: ProfitProtection): void {
  if (next.revision < configuration.value.revision) return;
  if (next.revision !== configuration.value.revision) {
    snapshot.value = null;
    readError.value = message.value = "";
    if (dirty.value) conflict.value = true;
  }
  configuration.value = { ...next };
  if (!dirty.value) resetDraft();
}

watch(identity, () => {
  generation += 1;
  configuration.value = { ...(props.status.profit_protection ?? disabledSettings()) };
  snapshot.value = null;
  error.value = readError.value = message.value = "";
  busy.value = false;
  resetDraft();
  if (enabled.value) void refresh();
}, { immediate: true, flush: "sync" });

watch(() => props.status, next => {
  if (next.profit_protection) acceptConfiguration(next.profit_protection);
});
watch(() => props.refreshKey, () => {
  if (enabled.value && !busy.value) void refresh();
});

function matches(value: ProfitProtectionStatus): boolean {
  return value.run_id === props.status.run_id && value.exchange === props.status.exchange
    && value.symbol === props.status.symbol;
}

function describeError(reason: unknown): string {
  if (reason instanceof ApiError && reason.status === 401) emit("unauthorized");
  return reason instanceof Error ? reason.message : "收益保护服务暂不可用";
}

async function refresh(reload = false): Promise<void> {
  if (readRunning || busy.value || !props.status.run_id || disposed) return;
  const selected = props.status;
  const key = identity.value;
  const version = generation;
  readRunning = true;
  try {
    const value = await api.profitProtection(selected.exchange, selected.symbol, selected.run_id!);
    if (disposed || key !== identity.value || version !== generation) return;
    if (!matches(value)) throw new Error("运行身份已变化，请重新选择策略");
    if (value.profit_protection.revision < configuration.value.revision) return;
    acceptConfiguration(value.profit_protection);
    snapshot.value = value;
    readError.value = "";
    if (reload) { resetDraft(); error.value = message.value = ""; }
  } catch (reason) {
    if (!disposed && key === identity.value && version === generation) {
      snapshot.value = null;
      readError.value = describeError(reason);
    }
  } finally {
    readRunning = false;
    if (!disposed && key !== identity.value && enabled.value) void refresh();
  }
}

// Validate Decimal's exact 96-bit coefficient and scale, without Number rounding.
function validAmount(value: string): boolean {
  if (!/^\d+(?:\.\d+)?$/.test(value)) return false;
  const [whole, fraction = ""] = value.split(".");
  const decimals = fraction.replace(/0+$/, "");
  const digits = `${whole}${decimals}`.replace(/^0+/, "");
  return decimals.length <= 28 && digits.length > 0 && digits.length <= 29
    && BigInt(digits) <= 79228162514264337593543950335n;
}

async function save(disable = false): Promise<void> {
  if (busy.value || !editable.value || disposed) return;
  message.value = error.value = "";
  if (conflict.value) { error.value = "配置已变更，请重新载入配置后再保存"; return; }
  const tp = disable ? null : takeProfit.value.trim() || null;
  const sl = disable ? null : stopLoss.value.trim() || null;
  if (!disable && tp === null && sl === null) {
    error.value = "请设置至少一项正数金额，或使用停用保护";
    return;
  }
  if ((tp !== null && !validAmount(tp)) || (sl !== null && !validAmount(sl))) {
    error.value = "金额必须为可精确表示的正数十进制（最多 28 位小数），不能为 0 或负数";
    return;
  }
  const selected = props.status;
  const key = identity.value;
  const version = ++generation;
  busy.value = true;
  try {
    const value = await api.updateProfitProtection(selected.exchange, selected.symbol, {
      run_id: selected.run_id!, expected_revision: configuration.value.revision,
      take_profit_amount: tp, stop_loss_amount: sl,
    });
    if (disposed || key !== identity.value || version !== generation) return;
    if (!matches(value) || value.profit_protection.revision < configuration.value.revision) {
      throw new Error("运行身份或配置版本已变化，请重新载入配置");
    }
    configuration.value = { ...value.profit_protection };
    snapshot.value = value;
    readError.value = "";
    resetDraft();
    message.value = disable ? "保护已停用" : "已保存；无需重启策略，等待下一次有效核对";
  } catch (reason) {
    if (!disposed && key === identity.value && version === generation) {
      error.value = describeError(reason);
      if (reason instanceof ApiError && [404, 409].includes(reason.status)) conflict.value = true;
    }
  } finally {
    if (!disposed && key === identity.value && version === generation) busy.value = false;
  }
}

function edited(): void { dirty.value = true; message.value = ""; }
onUnmounted(() => { disposed = true; generation += 1; });
</script>

<template>
  <section class="panel-card profit-protection-panel">
    <header class="section-header">
      <div>
        <p class="eyebrow">运行中策略 · 金额保护</p>
        <h2>净收益止盈 / 止损</h2>
        <p class="section-subtitle">仅配置当前运行；旧策略默认未启用 · 版本 {{ configuration.revision }}</p>
      </div>
      <span class="live-pill" data-testid="protection-status" aria-live="polite">{{ label }}</span>
    </header>
    <p class="protection-explanation">
      净收益＝已实现毛收益－实际成交手续费＋策略浮盈亏＋已结算资金费；只计算本策略，不使用全账户盈亏。
      实际退出净收益可能因平仓手续费和滑点变化。仅撤本策略订单、平本策略归属仓位；
      保留启动时已核对的手动基准仓位；仓位与台账不一致或归属不明时阻止平仓。
    </p>
    <p class="protection-explanation" data-testid="protection-server-warning">这是服务端监控，不是交易所托管止损；服务器停机、断网或数据无法核实时不能保证及时退出。</p>
    <p v-if="trigger" class="callout danger">
      收益保护已触发：净收益 {{ trigger.net_profit }} {{ unit }}，标记价格 {{ trigger.mark_price }}，
      配置版本 {{ trigger.protection_revision }}。正在按策略归属核对退出，触发不代表已完成平仓。
    </p>
    <p v-else-if="enabled && snapshot?.observation.net_profit != null" class="protection-observation">
      最近有效净收益 {{ snapshot.observation.net_profit }} {{ unit }}
      <span v-if="snapshot.observation.observed_at_ms"> · {{ new Date(snapshot.observation.observed_at_ms).toLocaleString() }}</span>
    </p>
    <p v-if="readError || snapshot?.observation.error" class="callout danger" role="status">
      暂不可用：{{ readError || snapshot?.observation.error }}；不会用未知收益或零值代替有效核对。
    </p>
    <form class="grid-form" @submit.prevent="save()">
      <fieldset class="protection-fields" :disabled="busy || !editable">
        <div class="form-grid">
          <label><span>止盈金额（{{ unit }}，正数）</span>
            <input v-model="takeProfit" data-testid="take-profit-amount" type="text" inputmode="decimal" placeholder="留空不设止盈" autocomplete="off" @input="edited" />
          </label>
          <label><span>止损金额（{{ unit }}，正数）</span>
            <input v-model="stopLoss" data-testid="stop-loss-amount" type="text" inputmode="decimal" placeholder="留空不设止损" autocomplete="off" @input="edited" />
          </label>
        </div>
      </fieldset>
      <p class="protection-explanation">净收益 ≥ 止盈金额或 ≤ 负的止损金额时触发。保存已越过的限额可能在下一次有效核对立即触发退出；保存本身不下单、不撤单、不重建网格。</p>
      <p v-if="!editable" class="callout">仅可配置正在运行且尚未触发退出的策略；请先从运行策略中明确配置。</p>
      <p v-if="conflict" class="callout danger">配置已变更，请重新载入配置并确认后再保存。</p>
      <p v-if="error" class="callout danger" role="alert">{{ error }}</p>
      <p v-if="message" class="callout" role="status">{{ message }}</p>
      <div class="protection-actions">
        <button class="primary-button" type="submit" data-testid="protection-save" :disabled="busy || !editable || conflict">{{ busy ? "正在保存…" : "保存保护" }}</button>
        <button class="ghost-button" type="button" data-testid="protection-disable" :disabled="busy || !editable || conflict || !enabled" @click="save(true)">停用保护</button>
        <button v-if="conflict || readError" class="ghost-button" type="button" data-testid="protection-reload" :disabled="busy" @click="refresh(true)">重新载入配置</button>
      </div>
    </form>
  </section>
</template>

<style scoped>
.profit-protection-panel { min-width: 0; }
.protection-explanation { color: var(--text-soft); font-size: 0.82rem; line-height: 1.8; }
.protection-fields { border: 0; padding: 0; margin: 1rem 0; min-width: 0; }
.protection-actions { display: flex; flex-wrap: wrap; gap: 0.65rem; }
.protection-observation { font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
@media (max-width: 600px) { .protection-fields .form-grid { grid-template-columns: 1fr; } }
</style>
