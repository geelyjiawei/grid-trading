<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, useId, watch } from "vue";
import type {
  Exchange,
  GridOrder,
  GridTrade,
  GridHistoryRun,
  PositionSnapshot,
} from "../api/types";
import { exchangeName } from "../format";
import StrategyDetailsPanel from "./StrategyDetailsPanel.vue";

type DetailTab = "positions" | "orders" | "trades" | "history";
const props = defineProps<{
  exchange: Exchange;
  symbol: string;
  configured: boolean;
  loading: boolean;
  error: string;
  availability: Record<"positions" | "orders" | "trades" | "history", boolean>;
  positions: PositionSnapshot[];
  orders: GridOrder[];
  trades: GridTrade[];
  history: GridHistoryRun[];
}>();
const emit = defineEmits<{ refresh: [] }>();
const drawerId = useId();
const drawer = ref<HTMLDialogElement | null>(null);
const isOpen = ref(false);
let opener: HTMLElement | null = null;
let savedOverflow = "";
let scrollLocked = false;
const initialTab = ref<DetailTab>("positions");
const tabs = [
  { key: "positions", label: "持仓" },
  { key: "orders", label: "挂单" },
  { key: "trades", label: "成交" },
  { key: "history", label: "历史" },
] as const;
const counts = computed(() => ({
  positions: props.positions.length,
  orders: props.orders.length,
  trades: props.trades.length,
  history: props.history.filter(
    (run) => run.exchange === props.exchange && run.symbol === props.symbol,
  ).length,
}));
watch(
  [() => props.exchange, () => props.symbol, () => props.configured],
  () => {
    closeDetails();
    initialTab.value = "positions";
  },
);

async function openDetails(event: MouseEvent, tab: DetailTab): Promise<void> {
  if (isOpen.value) return;
  opener = event.currentTarget as HTMLElement;
  initialTab.value = tab;
  isOpen.value = true;
  await nextTick();
  if (!isOpen.value || !drawer.value) return;
  // Native modal dialogs keep keyboard focus inside and make the page inert.
  drawer.value.showModal();
  savedOverflow = document.body.style.overflow;
  document.body.style.overflow = "hidden";
  scrollLocked = true;
}

function finishClose(): void {
  isOpen.value = false;
  if (scrollLocked) {
    document.body.style.overflow = savedOverflow;
    scrollLocked = false;
  }
  if (opener?.isConnected) opener.focus();
  opener = null;
}

function closeDetails(): void {
  if (drawer.value?.open) drawer.value.close();
  finishClose();
}

function handleNativeClose(): void {
  // A queued close event from the previous opening must not close a new one.
  if (!drawer.value?.open) finishClose();
}

function handleBackdrop(event: MouseEvent): void {
  if (event.target !== drawer.value || !drawer.value) return;
  const bounds = drawer.value.getBoundingClientRect();
  if (
    event.clientX < bounds.left ||
    event.clientX > bounds.right ||
    event.clientY < bounds.top ||
    event.clientY > bounds.bottom
  )
    closeDetails();
}

function keepFocusInDrawer(event: KeyboardEvent): void {
  const controls = Array.from(
    drawer.value?.querySelectorAll<HTMLElement>(
      'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href], [tabindex="0"]',
    ) ?? [],
  ).filter((control) => !control.hidden && !control.closest("[hidden]"));
  const first = controls[0];
  const last = controls[controls.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last?.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first?.focus();
  }
}

onBeforeUnmount(closeDetails);
</script>

<template>
  <section class="panel-card dashboard-span exchange-records-panel">
    <header class="section-header">
      <div>
        <p class="eyebrow">交易所权威数据</p>
        <h2>{{ exchangeName(exchange) }} · {{ symbol }}</h2>
        <p class="section-note">
          点击下方分类，在右侧查看持仓、挂单、成交和历史明细。
        </p>
      </div>
    </header>
    <nav class="record-launchers" aria-label="打开交易所明细">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        type="button"
        class="record-launch"
        :aria-label="`查看${tab.label}`"
        aria-haspopup="dialog"
        :aria-controls="drawerId"
        :aria-expanded="isOpen"
        @click="openDetails($event, tab.key)"
      >
        {{ tab.label }}
        <span class="record-launch-count">{{
          configured && availability[tab.key] ? counts[tab.key] : "--"
        }}</span>
        <span class="record-launch-arrow" aria-hidden="true">↗</span>
      </button>
    </nav>
    <p v-if="error" class="callout danger" role="status">{{ error }}</p>
    <dialog
      ref="drawer"
      :id="drawerId"
      class="bot-drawer"
      :aria-labelledby="`${drawerId}-title`"
      @cancel.prevent="closeDetails"
      @close="handleNativeClose"
      @click="handleBackdrop"
      @keydown.tab="keepFocusInDrawer"
    >
      <template v-if="isOpen">
        <header class="bot-drawer-bar">
          <div>
            <p class="eyebrow">交易所权威数据</p>
            <h2 :id="`${drawerId}-title`">
              {{ exchangeName(exchange) }} · {{ symbol }}
            </h2>
          </div>
          <button
            type="button"
            class="bot-drawer-close"
            aria-label="关闭交易所详情"
            autofocus
            @click="closeDetails"
          >
            关闭 <span aria-hidden="true">×</span>
          </button>
        </header>
        <div class="bot-drawer-content">
          <StrategyDetailsPanel
            :key="`${exchange}:${symbol}`"
            embedded
            :initial-tab="initialTab"
            :exchange="exchange"
            :symbol="symbol"
            :configured="configured"
            :loading="loading"
            :error="error"
            :availability="availability"
            :positions="positions"
            :orders="orders"
            :trades="trades"
            :history="history"
            @refresh="emit('refresh')"
          />
        </div>
      </template>
    </dialog>
  </section>
</template>
