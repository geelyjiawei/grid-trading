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
import { formatNumber } from "../format";

const props = defineProps<{
  exchange: Exchange;
  symbol: string;
  configured: boolean;
  fees: FeeRates | null;
  preview: GridPreview | null;
  busy: boolean;
  error: string;
}>();

const emit = defineEmits<{
  preview: [config: GridConfigRequest];
}>();

interface FormState {
  direction: Direction;
  gridMode: "arithmetic" | "geometric";
  gridCount: string;
  initialOrderType: InitialOrderType;
  initialOrderPrice: string;
  gridOrderPostOnly: boolean;
  upperPrice: string;
  lowerPrice: string;
  positionSizingMode: PositionSizingMode;
  totalInvestment: string;
  gridOrderQty: string;
  triggerPrice: string;
  stopLossPrice: string;
  takeProfitPrice: string;
  leverage: string;
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
const canPreview = computed(
  () => props.configured && Boolean(props.fees) && !props.busy && Boolean(props.symbol),
);

watch(
  () => [props.exchange, props.symbol],
  () => {
    localError.value = "";
  },
);

function positive(value: string, label: string): number {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) throw new Error(`${label}必须大于 0`);
  return number;
}

function optionalPositive(value: string, label: string): number | null {
  if (!value.trim()) return null;
  return positive(value, label);
}

function submitPreview(): void {
  localError.value = "";
  try {
    const upperPrice = positive(form.upperPrice, "价格上限");
    const lowerPrice = positive(form.lowerPrice, "价格下限");
    if (upperPrice <= lowerPrice) throw new Error("价格上限必须高于价格下限");
    const gridCount = positive(form.gridCount, "网格数量");
    if (!Number.isInteger(gridCount) || gridCount < 2 || gridCount > 100) {
      throw new Error("网格数量必须是 2 到 100 的整数");
    }
    const leverage = positive(form.leverage, "杠杆倍数");
    if (!Number.isInteger(leverage)) throw new Error("杠杆倍数必须是整数");
    if (!props.fees) throw new Error("账户实际费率尚未读取，不能预览");

    const totalInvestment = fixedQuantity.value
      ? 0
      : positive(form.totalInvestment, "总投入金额");
    const gridOrderQty = fixedQuantity.value
      ? positive(form.gridOrderQty, "每格开仓数量")
      : null;

    emit("preview", {
      exchange: props.exchange,
      symbol: props.symbol.trim().toUpperCase(),
      direction: form.direction,
      grid_mode: form.gridMode,
      upper_price: upperPrice,
      lower_price: lowerPrice,
      grid_count: gridCount,
      total_investment: totalInvestment,
      leverage,
      position_sizing_mode: form.positionSizingMode,
      grid_order_qty: gridOrderQty,
      fee_rate: props.fees.taker_fee_rate,
      maker_fee_rate: props.fees.maker_fee_rate,
      taker_fee_rate: props.fees.taker_fee_rate,
      initial_order_type: form.initialOrderType,
      initial_order_price: optionalPositive(form.initialOrderPrice, "开仓限价"),
      grid_order_post_only: form.gridOrderPostOnly,
      trigger_price: optionalPositive(form.triggerPrice, "触发价格"),
      stop_loss_price: optionalPositive(form.stopLossPrice, "止损价格"),
      take_profit_price: optionalPositive(form.takeProfitPrice, "止盈价格"),
    });
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
      <span class="preview-lock">真实启动暂未开放</span>
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
            <span>总投入金额（USDT）</span>
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
          Maker {{ fees ? `${formatNumber(fees.maker_fee_rate * 100, 6)}%` : "--" }} ·
          Taker {{ fees ? `${formatNumber(fees.taker_fee_rate * 100, 6)}%` : "--" }}。
          费率和数量均由后端再次校验。
        </p>
        <p v-if="!configured" class="form-error">请先保存当前交易所配置。</p>
        <p v-else-if="!fees" class="form-error">账户实际费率尚未读取，预览已锁定。</p>
        <p v-if="localError || error" class="form-error">{{ localError || error }}</p>
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
          <div><span>单格毛收益</span><strong>{{ formatNumber(preview.per_grid_gross_profit, 6) }} USDT</strong></div>
          <div><span>单格手续费</span><strong>{{ formatNumber(preview.per_grid_fee, 6) }} USDT</strong></div>
          <div><span>单格净收益</span><strong>{{ formatNumber(preview.per_grid_net_profit, 6) }} USDT</strong></div>
          <div><span>交易所最低金额</span><strong>{{ formatNumber(preview.min_notional, 4) }} USDT</strong></div>
        </div>
        <button class="disabled-trade-button" type="button" disabled>等待 Rust 交易状态机完成后开放启动</button>
      </aside>
    </div>
  </section>
</template>
