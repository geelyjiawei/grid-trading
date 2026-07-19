<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import type {
  Direction,
  Exchange,
  FeeRates,
  GridConfigRequest,
  GridPreview,
  InitialOrderType,
  PositionSizingMode,
} from "../api/types";
import { formatNumber, quoteAsset } from "../format";

const props = withDefaults(defineProps<{
  exchange: Exchange;
  symbol: string;
  configured: boolean;
  fees: FeeRates | null;
  preview: GridPreview | null;
  previewKey?: string;
  busy: boolean;
  error: string;
  startBusy?: boolean;
  startError?: string;
  startMessage?: string;
  strategyRunning?: boolean;
  tradingEnabled?: boolean;
}>(), {
  previewKey: "",
  startBusy: false,
  startError: "",
  startMessage: "",
  strategyRunning: false,
  tradingEnabled: false,
});

const emit = defineEmits<{
  preview: [config: GridConfigRequest];
  start: [config: GridConfigRequest];
}>();

interface FormState {
  direction: Direction;
  gridMode: "arithmetic" | "geometric";
  gridCount: string | number;
  initialOrderType: InitialOrderType;
  initialOrderPrice: string | number;
  gridOrderPostOnly: boolean;
  upperPrice: string | number;
  lowerPrice: string | number;
  positionSizingMode: PositionSizingMode;
  totalInvestment: string | number;
  gridOrderQty: string | number;
  triggerPrice: string | number;
  stopLossPrice: string | number;
  takeProfitPrice: string | number;
  leverage: string | number;
}

const form = reactive<FormState>({
  direction: "long",
  gridMode: "arithmetic",
  gridCount: "20",
  initialOrderType: "market",
  initialOrderPrice: "",
  gridOrderPostOnly: false,
  upperPrice: "",
  lowerPrice: "",
  positionSizingMode: "fixed_grid_qty",
  totalInvestment: "500",
  gridOrderQty: "",
  triggerPrice: "",
  stopLossPrice: "",
  takeProfitPrice: "",
  leverage: "3",
});
const localError = ref("");

const fixedQuantity = computed(() => form.positionSizingMode === "fixed_grid_qty");
const settlementAsset = computed(() => quoteAsset(props.exchange));
const canPreview = computed(
  () => props.configured && Boolean(props.fees) && !props.busy && !props.startBusy && Boolean(props.symbol),
);
const currentConfig = computed(() => {
  try {
    return buildConfig();
  } catch {
    return null;
  }
});
const currentConfigKey = computed(() => currentConfig.value ? JSON.stringify(currentConfig.value) : "");
const previewMatchesForm = computed(
  () => Boolean(props.preview) && Boolean(props.previewKey) && props.previewKey === currentConfigKey.value,
);
const canStart = computed(
  () => canPreview.value && previewMatchesForm.value && !props.strategyRunning && props.tradingEnabled,
);

watch(
  () => [props.exchange, props.symbol],
  () => {
    localError.value = "";
  },
);

function positiveDecimal(value: string | number, label: string): string {
  const trimmed = String(value).trim();
  if (!/^\d+(?:\.\d+)?$/.test(trimmed)) throw new Error(`${label}必须是正数`);
  const [integerPart, fractionPart = ""] = trimmed.split(".");
  const integer = integerPart!.replace(/^0+(?=\d)/, "");
  const fraction = fractionPart.replace(/0+$/, "");
  const canonical = fraction ? `${integer}.${fraction}` : integer;
  if (canonical === "0") throw new Error(`${label}必须大于 0`);
  return canonical;
}

function optionalPositive(value: string | number, label: string): string | null {
  if (!String(value).trim()) return null;
  return positiveDecimal(value, label);
}

function comparePositiveDecimals(left: string, right: string): number {
  const [leftInteger, leftFraction = ""] = left.split(".");
  const [rightInteger, rightFraction = ""] = right.split(".");
  if (leftInteger!.length !== rightInteger!.length) {
    return leftInteger!.length > rightInteger!.length ? 1 : -1;
  }
  if (leftInteger !== rightInteger) return leftInteger! > rightInteger! ? 1 : -1;
  const width = Math.max(leftFraction.length, rightFraction.length);
  const normalizedLeft = leftFraction.padEnd(width, "0");
  const normalizedRight = rightFraction.padEnd(width, "0");
  if (normalizedLeft === normalizedRight) return 0;
  return normalizedLeft > normalizedRight ? 1 : -1;
}

function positiveInteger(value: string | number, label: string, minimum: number, maximum: number): number {
  if (!/^\d+$/.test(String(value).trim())) throw new Error(`${label}必须是整数`);
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    throw new Error(`${label}必须是 ${minimum} 到 ${maximum} 的整数`);
  }
  return number;
}

function buildConfig(): GridConfigRequest {
  const upperPrice = positiveDecimal(form.upperPrice, "价格上限");
  const lowerPrice = positiveDecimal(form.lowerPrice, "价格下限");
  if (comparePositiveDecimals(upperPrice, lowerPrice) <= 0) {
    throw new Error("价格上限必须高于价格下限");
  }
  const gridCount = positiveInteger(form.gridCount, "网格数量", 2, 100);
  const leverage = positiveInteger(form.leverage, "杠杆倍数", 1, 125);
  if (!props.fees) throw new Error("账户实际费率尚未读取，不能预览");
  const normalizedSymbol = props.symbol.trim().toUpperCase();
  if (!/^[A-Z0-9]+$/.test(normalizedSymbol)) throw new Error("交易对格式不正确");

  const totalInvestment = fixedQuantity.value
    ? "0"
    : positiveDecimal(form.totalInvestment, "总投入金额");
  const gridOrderQty = fixedQuantity.value
    ? positiveDecimal(form.gridOrderQty, "每格开仓数量")
    : null;

  return {
    exchange: props.exchange,
    symbol: normalizedSymbol,
    direction: form.direction,
    grid_mode: form.gridMode,
    upper_price: upperPrice,
    lower_price: lowerPrice,
    grid_count: gridCount,
    total_investment: totalInvestment,
    leverage,
    position_sizing_mode: form.positionSizingMode,
    grid_order_qty: gridOrderQty,
    fee_rate: String(props.fees.taker_fee_rate),
    maker_fee_rate: String(props.fees.maker_fee_rate),
    taker_fee_rate: String(props.fees.taker_fee_rate),
    initial_order_type: form.initialOrderType,
    initial_order_price: form.initialOrderType === "market"
      ? null
      : optionalPositive(form.initialOrderPrice, "开仓限价"),
    grid_order_post_only: form.gridOrderPostOnly,
    trigger_price: optionalPositive(form.triggerPrice, "触发价格"),
    stop_loss_price: optionalPositive(form.stopLossPrice, "止损价格"),
    take_profit_price: optionalPositive(form.takeProfitPrice, "止盈价格"),
  };
}

function submitPreview(): void {
  localError.value = "";
  try {
    emit("preview", buildConfig());
  } catch (reason) {
    localError.value = reason instanceof Error ? reason.message : "参数填写不完整";
  }
}

function submitStart(): void {
  localError.value = "";
  try {
    const config = buildConfig();
    if (!props.preview || props.previewKey !== JSON.stringify(config)) {
      throw new Error("参数已变化，请重新校验并预览后再启动");
    }
    emit("start", config);
  } catch (reason) {
    localError.value = reason instanceof Error ? reason.message : "参数填写不完整";
  }
}

function quantityText(): string {
  if (!props.preview) return "--";
  const min = Number(props.preview.qty_per_grid_min ?? props.preview.qty_per_grid_avg);
  const max = Number(props.preview.qty_per_grid_max ?? props.preview.qty_per_grid_avg);
  return min === max
    ? formatNumber(props.preview.qty_per_grid_avg, 8)
    : `${formatNumber(min, 8)} - ${formatNumber(max, 8)}`;
}
</script>

<template>
  <section class="panel-card configuration-panel dashboard-span">
    <header class="section-header">
      <div>
        <p class="eyebrow">后端权威预览</p>
        <h2>网格参数</h2>
      </div>
      <span class="preview-lock">Rust 状态机控制</span>
    </header>

    <div class="configuration-layout">
      <form class="grid-form" @submit.prevent="submitPreview">
        <fieldset>
          <legend>方向</legend>
          <div class="direction-switch">
            <button
              v-for="direction in (['long', 'short', 'neutral'] as Direction[])"
              :key="direction"
              type="button"
              :class="{ active: form.direction === direction, short: direction === 'short' }"
              @click="form.direction = direction"
            >
              {{ direction === "long" ? "做多" : direction === "short" ? "做空" : "中性" }}
            </button>
          </div>
        </fieldset>

        <div class="form-grid">
          <label>
            <span>网格模式</span>
            <select v-model="form.gridMode" data-testid="grid-mode">
              <option value="arithmetic">等差网格</option>
              <option value="geometric">等比网格</option>
            </select>
          </label>
          <label>
            <span>网格数量</span>
            <input v-model="form.gridCount" data-testid="grid-count" type="number" min="2" max="100" step="1" />
          </label>
          <label>
            <span>开仓方式</span>
            <select v-model="form.initialOrderType" data-testid="initial-order-type">
              <option value="market">市价开仓（立即成交）</option>
              <option value="limit">普通限价开仓（GTC）</option>
              <option value="post_only">Post Only 限价开仓</option>
            </select>
          </label>
          <label>
            <span>开仓限价（可选）</span>
            <input v-model="form.initialOrderPrice" type="number" min="0" step="any" placeholder="市价模式可留空" />
          </label>
          <label>
            <span>价格下限</span>
            <input v-model="form.lowerPrice" data-testid="lower-price" type="number" min="0" step="any" />
          </label>
          <label>
            <span>价格上限</span>
            <input v-model="form.upperPrice" data-testid="upper-price" type="number" min="0" step="any" />
          </label>
          <label>
            <span>开仓数量模式</span>
            <select v-model="form.positionSizingMode" data-testid="sizing-mode">
              <option value="fixed_grid_qty">按每格固定数量</option>
              <option value="investment">按总投入金额</option>
            </select>
          </label>
          <label v-if="fixedQuantity">
            <span>每格开仓数量</span>
            <input v-model="form.gridOrderQty" data-testid="grid-order-qty" type="number" min="0" step="any" />
          </label>
          <label v-else>
            <span>总投入金额（{{ settlementAsset }}）</span>
            <input v-model="form.totalInvestment" data-testid="total-investment" type="number" min="0" step="any" />
          </label>
          <label>
            <span>触发价格（可选）</span>
            <input v-model="form.triggerPrice" type="number" min="0" step="any" />
          </label>
          <label>
            <span>止损价格（可选）</span>
            <input v-model="form.stopLossPrice" type="number" min="0" step="any" />
          </label>
          <label>
            <span>止盈价格（可选）</span>
            <input v-model="form.takeProfitPrice" type="number" min="0" step="any" />
          </label>
        </div>

        <label class="check-row form-check">
          <input v-model="form.gridOrderPostOnly" type="checkbox" />
          <span>网格挂单使用 Post Only（默认关闭）</span>
        </label>
        <label class="leverage-control">
          <span>杠杆倍数</span>
          <input v-model="form.leverage" type="range" min="1" max="50" step="1" />
          <strong>{{ form.leverage }}x</strong>
        </label>

        <p class="form-hint">
          Maker {{ fees ? `${formatNumber(Number(fees.maker_fee_rate) * 100, 6)}%` : "--" }} ·
          Taker {{ fees ? `${formatNumber(Number(fees.taker_fee_rate) * 100, 6)}%` : "--" }}。
          费率和数量均由后端再次校验。
        </p>
        <p v-if="!configured" class="form-error">请先在服务器配置当前交易所并重启候选服务。</p>
        <p v-else-if="!fees" class="form-error">账户实际费率尚未读取，预览已锁定。</p>
        <p v-if="localError || error || startError" class="form-error">{{ localError || error || startError }}</p>
        <p v-if="startMessage" class="form-hint">{{ startMessage }}</p>
        <button class="primary-button" type="submit" :disabled="!canPreview">
          {{ busy ? "正在计算…" : "校验并预览" }}
        </button>
      </form>

      <aside class="preview-card" aria-live="polite">
        <p class="eyebrow">计算结果</p>
        <h3>{{ preview ? `${preview.active_grid_count} / ${preview.grid_count} 格参与` : "等待参数" }}</h3>
        <p v-if="!preview" class="empty-state">填写参数后由后端返回精确数量，不使用浏览器估算。</p>
        <div v-else class="preview-metrics">
          <div><span>网格间距</span><strong>{{ formatNumber(preview.grid_step, 8) }}</strong></div>
          <div><span>单格收益率</span><strong>{{ formatNumber(preview.grid_profit_pct, 4) }}%</strong></div>
          <div><span>每格数量</span><strong>{{ quantityText() }}</strong></div>
          <div><span>总预估开仓量</span><strong>{{ formatNumber(preview.total_qty, 8) }}</strong></div>
          <div><span>单格毛收益</span><strong>{{ formatNumber(preview.per_grid_gross_profit, 6) }} {{ settlementAsset }}</strong></div>
          <div><span>单格手续费</span><strong>{{ formatNumber(preview.per_grid_fee, 6) }} {{ settlementAsset }}</strong></div>
          <div><span>单格净收益</span><strong>{{ formatNumber(preview.per_grid_net_profit, 6) }} {{ settlementAsset }}</strong></div>
          <div><span>交易所最低金额</span><strong>{{ formatNumber(preview.min_notional, 4) }} {{ settlementAsset }}</strong></div>
        </div>
        <p v-if="preview && !previewMatchesForm" class="form-error">参数已变化，本次预览已失效。</p>
        <p v-if="strategyRunning" class="form-hint">当前交易所与交易对已有活动策略（含停止确认中），不能重复启动。</p>
        <p v-else-if="!tradingEnabled" class="form-hint">Rust 实盘写入开关尚未启用，只能预览。</p>
        <button class="primary-button" type="button" :disabled="!canStart" @click="submitStart">
          {{ startBusy ? "正在持久化并启动…" : "启动已预览策略" }}
        </button>
      </aside>
    </div>
  </section>
</template>
