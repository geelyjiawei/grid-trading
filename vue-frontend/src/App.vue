<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { ApiError, api } from "./api/client";
import type {
  ApiConfigResponse,
  AuthStatus,
  BalanceSnapshot,
  Exchange,
  FeeRates,
  GridConfigRequest,
  GridPreview,
  GridHistoryRun,
  GridOrder,
  GridStatus,
  GridTrade,
  LoginRequest,
  PriceSnapshot,
  PositionSnapshot,
  RiskSnapshot,
  SaveApiConfigRequest,
} from "./api/types";
import AuthDialog from "./components/AuthDialog.vue";
import ExchangeSettingsDialog from "./components/ExchangeSettingsDialog.vue";
import GridConfigurationPanel from "./components/GridConfigurationPanel.vue";
import MarketOverview from "./components/MarketOverview.vue";
import StrategyList from "./components/StrategyList.vue";
import StrategyDetailsPanel from "./components/StrategyDetailsPanel.vue";
import StrategyOverview from "./components/StrategyOverview.vue";
import { exchangeName } from "./format";

const exchanges: Exchange[] = ["binance", "aster", "bybit"];
const authStatus = ref<AuthStatus | null>(null);
const authenticated = ref(false);
const authBusy = ref(false);
const authError = ref("");
const config = ref<ApiConfigResponse | null>(null);
const activeExchange = ref<Exchange>("bybit");
const symbol = ref("BTCUSDT");
const price = ref<PriceSnapshot | null>(null);
const balance = ref<BalanceSnapshot | null>(null);
const fees = ref<FeeRates | null>(null);
const grids = ref<GridStatus[]>([]);
const selectedStatus = ref<GridStatus | null>(null);
const risk = ref<RiskSnapshot | null>(null);
const positions = ref<PositionSnapshot[]>([]);
const openOrders = ref<GridOrder[]>([]);
const trades = ref<GridTrade[]>([]);
const history = ref<GridHistoryRun[]>([]);
const detailsLoading = ref(false);
const detailsError = ref("");
const preview = ref<GridPreview | null>(null);
const previewContext = ref("");
const previewBusy = ref(false);
const previewError = ref("");
const loading = ref(true);
const strategyError = ref("");
const marketError = ref("");
const settingsOpen = ref(false);
const settingsBusy = ref(false);
const settingsError = ref("");
let statusTimer: number | undefined;
let marketTimer: number | undefined;
let statusRefreshRunning = false;
let marketRefreshRunning = false;
let previewRequestSequence = 0;
let detailsRequestSequence = 0;

const configured = computed(
  () => Boolean(config.value?.configs[activeExchange.value]?.configured),
);
const workspaceError = computed(() =>
  [...new Set([strategyError.value, marketError.value].filter(Boolean))].join("；"),
);
const currentPreviewContext = computed(
  () => `${activeExchange.value}:${symbol.value}:${fees.value?.maker_fee_rate ?? ""}:${fees.value?.taker_fee_rate ?? ""}`,
);
const visiblePreview = computed(() =>
  previewContext.value === currentPreviewContext.value ? preview.value : null,
);

function messageFrom(reason: unknown, fallback: string): string {
  if (reason instanceof ApiError) {
    if (reason.status === 401) authenticated.value = false;
    return reason.message;
  }
  return reason instanceof Error ? reason.message : fallback;
}

function normalizeSymbol(): void {
  symbol.value = symbol.value.trim().toUpperCase();
}

async function loadConfig(): Promise<void> {
  const response = await api.config();
  config.value = response;
  const preferred = response.active_exchange ?? response.exchange;
  if (preferred && exchanges.includes(preferred)) activeExchange.value = preferred;
}

async function refreshStrategies(): Promise<void> {
  if (statusRefreshRunning || !authenticated.value) return;
  statusRefreshRunning = true;
  try {
    const response = await api.gridStatus();
    grids.value = (response.grids ?? []).filter((grid) => grid.running);
    selectedStatus.value =
      response.grids.find(
        (grid) => grid.exchange === activeExchange.value && grid.symbol === symbol.value,
      ) ?? null;
    strategyError.value = "";
  } catch (reason) {
    strategyError.value = messageFrom(reason, "无法读取运行策略");
  } finally {
    statusRefreshRunning = false;
    loading.value = false;
  }
}

async function refreshMarket(): Promise<void> {
  if (marketRefreshRunning || !authenticated.value || !symbol.value) return;
  marketRefreshRunning = true;
  const exchange = activeExchange.value;
  const requestedSymbol = symbol.value;
  try {
    const priceRequest = api.price(exchange, requestedSymbol);
    const balanceRequest = configured.value
      ? api.balance(exchange)
      : Promise.resolve<BalanceSnapshot | null>(null);
    const feeRequest = configured.value
      ? api.feeRates(exchange, requestedSymbol)
      : Promise.resolve<FeeRates | null>(null);
    const riskRequest = configured.value
      ? api.risk(exchange, requestedSymbol)
      : Promise.resolve<RiskSnapshot | null>(null);

    const results = await Promise.allSettled([
      priceRequest,
      balanceRequest,
      feeRequest,
      riskRequest,
    ]);
    if (exchange !== activeExchange.value || requestedSymbol !== symbol.value) return;

    const [priceResult, balanceResult, feeResult, riskResult] = results;
    if (priceResult.status === "fulfilled") price.value = priceResult.value;
    if (balanceResult.status === "fulfilled") balance.value = balanceResult.value;
    if (feeResult.status === "fulfilled") fees.value = feeResult.value;
    if (riskResult.status === "fulfilled") risk.value = riskResult.value;

    const failures = results
      .filter((result): result is PromiseRejectedResult => result.status === "rejected")
      .map((result) => messageFrom(result.reason, "交易所数据读取失败"));
    marketError.value = [...new Set(failures)].join("；");
  } catch (reason) {
    if (exchange === activeExchange.value && requestedSymbol === symbol.value) {
      marketError.value = messageFrom(reason, "交易所数据读取失败");
    }
  } finally {
    marketRefreshRunning = false;
    loading.value = false;
  }
}

async function refreshWorkspace(): Promise<void> {
  normalizeSymbol();
  loading.value = true;
  try {
    await Promise.all([refreshStrategies(), refreshMarket(), refreshDetails()]);
  } finally {
    loading.value = false;
  }
}

async function refreshDetails(): Promise<void> {
  if (!authenticated.value || !configured.value || !symbol.value) {
    positions.value = [];
    openOrders.value = [];
    trades.value = [];
    return;
  }
  const exchange = activeExchange.value;
  const requestedSymbol = symbol.value;
  const requestSequence = ++detailsRequestSequence;
  detailsLoading.value = true;
  detailsError.value = "";
  const results = await Promise.allSettled([
    api.positions(exchange, requestedSymbol),
    api.openOrders(exchange, requestedSymbol),
    api.trades(exchange, requestedSymbol),
    api.history(),
  ]);
  if (
    requestSequence !== detailsRequestSequence
    || exchange !== activeExchange.value
    || requestedSymbol !== symbol.value
  ) {
    return;
  }

  const [positionResult, orderResult, tradeResult, historyResult] = results;
  if (positionResult.status === "fulfilled") positions.value = positionResult.value.positions;
  if (orderResult.status === "fulfilled") {
    openOrders.value = orderResult.value.orders ?? orderResult.value.result?.list ?? [];
  }
  if (tradeResult.status === "fulfilled") {
    trades.value = tradeResult.value.trades ?? tradeResult.value.result?.list ?? [];
  }
  if (historyResult.status === "fulfilled") history.value = historyResult.value.runs;

  const failures = results
    .filter((result): result is PromiseRejectedResult => result.status === "rejected")
    .map((result) => messageFrom(result.reason, "策略明细读取失败"));
  detailsError.value = [...new Set(failures)].join("；");
  detailsLoading.value = false;
}

async function requestPreview(configRequest: GridConfigRequest): Promise<void> {
  const requestSequence = ++previewRequestSequence;
  const context = currentPreviewContext.value;
  previewBusy.value = true;
  previewError.value = "";
  try {
    const result = await api.preview(configRequest);
    if (requestSequence !== previewRequestSequence || context !== currentPreviewContext.value) {
      return;
    }
    preview.value = result;
    previewContext.value = context;
  } catch (reason) {
    if (requestSequence === previewRequestSequence && context === currentPreviewContext.value) {
      preview.value = null;
      previewContext.value = context;
      previewError.value = messageFrom(reason, "网格预览失败");
    }
  } finally {
    if (requestSequence === previewRequestSequence) previewBusy.value = false;
  }
}

function startPolling(): void {
  window.clearInterval(statusTimer);
  window.clearInterval(marketTimer);
  statusTimer = window.setInterval(() => void refreshStrategies(), 3000);
  marketTimer = window.setInterval(() => void refreshMarket(), 5000);
}

async function initializeWorkspace(): Promise<void> {
  await loadConfig();
  await refreshWorkspace();
  startPolling();
}

async function checkAuth(): Promise<void> {
  loading.value = true;
  try {
    const status = await api.authStatus();
    authStatus.value = status;
    authenticated.value = !status.required || status.authenticated;
    if (authenticated.value) await initializeWorkspace();
  } catch (reason) {
    authError.value = messageFrom(reason, "认证状态读取失败");
  } finally {
    loading.value = false;
  }
}

async function login(credentials: LoginRequest): Promise<void> {
  authBusy.value = true;
  authError.value = "";
  try {
    await api.login(credentials);
    authenticated.value = true;
    await initializeWorkspace();
  } catch (reason) {
    authError.value = messageFrom(reason, "登录失败");
  } finally {
    authBusy.value = false;
  }
}

async function selectExchange(exchange: Exchange): Promise<void> {
  activeExchange.value = exchange;
  price.value = null;
  balance.value = null;
  fees.value = null;
  risk.value = null;
  positions.value = [];
  openOrders.value = [];
  trades.value = [];
  marketError.value = "";
  await refreshWorkspace();
}

async function selectStrategy(grid: GridStatus): Promise<void> {
  activeExchange.value = grid.exchange;
  symbol.value = grid.symbol;
  selectedStatus.value = grid;
  await Promise.all([refreshMarket(), refreshDetails()]);
}

async function saveConfig(payload: SaveApiConfigRequest): Promise<void> {
  settingsBusy.value = true;
  settingsError.value = "";
  try {
    await api.saveConfig(payload);
    activeExchange.value = payload.exchange;
    await loadConfig();
    settingsOpen.value = false;
    await refreshWorkspace();
  } catch (reason) {
    settingsError.value = messageFrom(reason, "配置保存失败");
  } finally {
    settingsBusy.value = false;
  }
}

onMounted(() => void checkAuth());
onUnmounted(() => {
  window.clearInterval(statusTimer);
  window.clearInterval(marketTimer);
});
</script>

<template>
  <main class="app-shell">
    <header class="topbar">
      <div class="brand-block">
        <span class="brand-mark">G</span>
        <div>
          <p class="eyebrow">Vue + Rust migration</p>
          <h1>合约网格控制台</h1>
        </div>
      </div>
      <div class="topbar-actions">
        <span class="migration-lock">交易写入迁移锁定</span>
        <button class="ghost-button" type="button" @click="settingsOpen = true">API 设置</button>
        <button class="primary-button compact" type="button" :disabled="loading" @click="refreshWorkspace">
          {{ loading ? "同步中…" : "立即刷新" }}
        </button>
      </div>
    </header>

    <section class="workspace-bar">
      <div class="exchange-tabs" aria-label="交易所工作区">
        <button
          v-for="exchange in exchanges"
          :key="exchange"
          type="button"
          :class="{ active: activeExchange === exchange }"
          @click="selectExchange(exchange)"
        >
          {{ exchangeName(exchange) }}
          <span :class="config?.configs[exchange]?.configured ? 'configured-dot' : 'empty-dot'"></span>
        </button>
      </div>
      <label class="symbol-control">
        <span>交易对</span>
        <input v-model="symbol" spellcheck="false" @change="refreshWorkspace" @keyup.enter="refreshWorkspace" />
      </label>
    </section>

    <p class="migration-note">
      当前 Vue 页面只接入读取与账户配置；启动、停止和撤单将在 Rust 交易状态机完成双实现核对后开放。
    </p>
    <p v-if="workspaceError || (!authenticated && authError)" class="callout danger global-error">
      {{ workspaceError || authError }}
    </p>

    <section class="dashboard-grid">
      <MarketOverview
        :exchange="activeExchange"
        :symbol="symbol"
        :configured="configured"
        :price="price"
        :balance="balance"
        :fees="fees"
        :loading="loading"
      />
      <StrategyList
        :grids="grids"
        :active-exchange="activeExchange"
        :active-symbol="symbol"
        :loading="loading"
        @select="selectStrategy"
      />
      <GridConfigurationPanel
        :exchange="activeExchange"
        :symbol="symbol"
        :configured="configured"
        :fees="fees"
        :preview="visiblePreview"
        :busy="previewBusy"
        :error="previewError"
        @preview="requestPreview"
      />
      <StrategyOverview class="dashboard-span" :status="selectedStatus" :risk="risk" />
      <StrategyDetailsPanel
        :exchange="activeExchange"
        :symbol="symbol"
        :configured="configured"
        :loading="detailsLoading"
        :error="detailsError"
        :positions="positions"
        :orders="openOrders"
        :trades="trades"
        :history="history"
        @refresh="refreshDetails"
      />
    </section>

    <AuthDialog
      v-if="authStatus?.required && !authenticated"
      :status="authStatus"
      :busy="authBusy"
      :error="authError"
      @submit="login"
    />
    <ExchangeSettingsDialog
      :open="settingsOpen"
      :config="config"
      :active-exchange="activeExchange"
      :busy="settingsBusy"
      :error="settingsError"
      @close="settingsOpen = false"
      @save="saveConfig"
    />
  </main>
</template>
