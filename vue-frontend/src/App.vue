<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { ApiError, api } from "./api/client";
import type {
  ApiConfigResponse,
  AuthStatus,
  BalanceSnapshot,
  Exchange,
  ExchangeConfigRequest,
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
const configBusy = ref(false);
const configError = ref("");
const configMessage = ref("");
const activeExchange = ref<Exchange>("bybit");
const symbol = ref("BTCUSDT");
const price = ref<PriceSnapshot | null>(null);
const balance = ref<BalanceSnapshot | null>(null);
const fees = ref<FeeRates | null>(null);
const grids = ref<GridStatus[]>([]);
const tradingEnabled = ref(false);
const selectedStatus = ref<GridStatus | null>(null);
const risk = ref<RiskSnapshot | null>(null);
const positions = ref<PositionSnapshot[]>([]);
const openOrders = ref<GridOrder[]>([]);
const trades = ref<GridTrade[]>([]);
const history = ref<GridHistoryRun[]>([]);
const detailsLoading = ref(false);
const detailsError = ref("");
const detailAvailability = ref({
  positions: false,
  orders: false,
  trades: false,
  history: false,
});
const preview = ref<GridPreview | null>(null);
const previewContext = ref("");
const previewConfig = ref<GridConfigRequest | null>(null);
const previewKey = ref("");
const previewBusy = ref(false);
const previewError = ref("");
const startBusy = ref(false);
const startError = ref("");
const startMessage = ref("");
const stopBusy = ref(false);
const stopError = ref("");
const loading = ref(true);
const strategyError = ref("");
const marketError = ref("");
const settingsOpen = ref(false);
let statusTimer: number | undefined;
let marketTimer: number | undefined;
let statusRefreshRunning = false;
let previewRequestSequence = 0;
let marketRequestSequence = 0;
let detailsRequestSequence = 0;
let marketContext = "";
let balanceContext = "";
let detailsContext = "";

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
const visiblePreviewKey = computed(() =>
  previewContext.value === currentPreviewContext.value ? previewKey.value : "",
);
const strategyRunning = computed(() =>
  grids.value.some(
    (grid) => grid.running && grid.exchange === activeExchange.value && grid.symbol === symbol.value,
  ),
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

function prepareWorkspaceContext(): void {
  const exchange = activeExchange.value;
  const context = `${exchange}:${symbol.value}`;
  if (marketContext !== context) {
    marketContext = context;
    marketRequestSequence += 1;
    price.value = null;
    fees.value = null;
    risk.value = null;
    marketError.value = "";
  }
  if (balanceContext !== exchange) {
    balanceContext = exchange;
    balance.value = null;
  }
  if (detailsContext !== context) {
    detailsContext = context;
    detailsRequestSequence += 1;
    positions.value = [];
    openOrders.value = [];
    trades.value = [];
    history.value = [];
    detailAvailability.value = {
      positions: false,
      orders: false,
      trades: false,
      history: false,
    };
    detailsError.value = "";
    detailsLoading.value = false;
  }
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
    tradingEnabled.value = response.trading_enabled === true;
    grids.value = (response.grids ?? []).filter((grid) => grid.running);
    selectedStatus.value =
      response.grids.find(
        (grid) => grid.exchange === activeExchange.value && grid.symbol === symbol.value,
      ) ?? null;
    strategyError.value = "";
  } catch (reason) {
    tradingEnabled.value = false;
    strategyError.value = messageFrom(reason, "无法读取运行策略");
  } finally {
    statusRefreshRunning = false;
    loading.value = false;
  }
}

async function refreshMarket(): Promise<void> {
  if (!authenticated.value || !symbol.value) return;
  prepareWorkspaceContext();
  const exchange = activeExchange.value;
  const requestedSymbol = symbol.value;
  const requestSequence = ++marketRequestSequence;
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
    if (
      requestSequence !== marketRequestSequence
      || exchange !== activeExchange.value
      || requestedSymbol !== symbol.value
    ) {
      return;
    }

    const [priceResult, balanceResult, feeResult, riskResult] = results;
    price.value = priceResult.status === "fulfilled" ? priceResult.value : null;
    balance.value = balanceResult.status === "fulfilled" ? balanceResult.value : null;
    fees.value = feeResult.status === "fulfilled" ? feeResult.value : null;
    risk.value = riskResult.status === "fulfilled" ? riskResult.value : null;

    const failures = results
      .filter((result): result is PromiseRejectedResult => result.status === "rejected")
      .map((result) => messageFrom(result.reason, "交易所数据读取失败"));
    marketError.value = [...new Set(failures)].join("；");
  } catch (reason) {
    if (
      requestSequence === marketRequestSequence
      && exchange === activeExchange.value
      && requestedSymbol === symbol.value
    ) {
      price.value = null;
      balance.value = null;
      fees.value = null;
      risk.value = null;
      marketError.value = messageFrom(reason, "交易所数据读取失败");
    }
  } finally {
    if (requestSequence === marketRequestSequence) loading.value = false;
  }
}

async function refreshWorkspace(): Promise<void> {
  normalizeSymbol();
  prepareWorkspaceContext();
  loading.value = true;
  try {
    await Promise.all([refreshStrategies(), refreshMarket(), refreshDetails()]);
  } finally {
    loading.value = false;
  }
}

async function refreshDetails(): Promise<void> {
  prepareWorkspaceContext();
  if (!authenticated.value || !configured.value || !symbol.value) {
    detailsRequestSequence += 1;
    positions.value = [];
    openOrders.value = [];
    trades.value = [];
    history.value = [];
    detailAvailability.value = {
      positions: false,
      orders: false,
      trades: false,
      history: false,
    };
    detailsLoading.value = false;
    detailsError.value = "";
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
  if (positionResult.status === "fulfilled") {
    positions.value = positionResult.value.positions;
  }
  if (orderResult.status === "fulfilled") {
    openOrders.value = orderResult.value.orders ?? orderResult.value.result?.list ?? [];
  }
  if (tradeResult.status === "fulfilled") {
    trades.value = tradeResult.value.trades ?? tradeResult.value.result?.list ?? [];
  }
  if (historyResult.status === "fulfilled") {
    history.value = historyResult.value.runs;
  }
  detailAvailability.value = {
    positions: positionResult.status === "fulfilled",
    orders: orderResult.status === "fulfilled",
    trades: tradeResult.status === "fulfilled",
    history: historyResult.status === "fulfilled",
  };

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
  startError.value = "";
  startMessage.value = "";
  try {
    const result = await api.preview(configRequest);
    if (requestSequence !== previewRequestSequence || context !== currentPreviewContext.value) {
      return;
    }
    preview.value = result;
    previewContext.value = context;
    previewConfig.value = structuredClone(configRequest);
    previewKey.value = JSON.stringify(configRequest);
  } catch (reason) {
    if (requestSequence === previewRequestSequence && context === currentPreviewContext.value) {
      preview.value = null;
      previewConfig.value = null;
      previewKey.value = "";
      previewContext.value = context;
      previewError.value = messageFrom(reason, "网格预览失败");
    }
  } finally {
    if (requestSequence === previewRequestSequence) previewBusy.value = false;
  }
}

async function startStrategy(configRequest: GridConfigRequest): Promise<void> {
  startError.value = "";
  startMessage.value = "";
  if (!tradingEnabled.value) {
    startError.value = "Rust 实盘写入尚未启用";
    return;
  }
  if (strategyRunning.value) {
    startError.value = "当前交易所与交易对已有运行策略";
    return;
  }
  if (
    previewContext.value !== currentPreviewContext.value
    || !previewConfig.value
    || !visiblePreview.value
    || previewKey.value !== JSON.stringify(configRequest)
  ) {
    startError.value = "预览已失效，请重新校验参数后再启动";
    return;
  }
  startBusy.value = true;
  try {
    const response = await api.start(previewConfig.value);
    startMessage.value = `策略 ${response.run_id} 已持久化，状态：${response.lifecycle}`;
    await Promise.all([refreshStrategies(), refreshMarket(), refreshDetails()]);
  } catch (reason) {
    startError.value = messageFrom(reason, "网格启动失败");
  } finally {
    startBusy.value = false;
  }
}

async function stopStrategy(): Promise<void> {
  const status = selectedStatus.value;
  if (!status?.running || stopBusy.value) return;
  stopBusy.value = true;
  stopError.value = "";
  startMessage.value = "";
  try {
    const response = await api.stop(status.exchange, status.symbol);
    startMessage.value = `停止请求已持久化，状态：${response.lifecycle}；只撤策略订单，不主动平仓`;
    await Promise.all([refreshStrategies(), refreshMarket(), refreshDetails()]);
  } catch (reason) {
    stopError.value = messageFrom(reason, "网格停止失败");
  } finally {
    stopBusy.value = false;
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
  selectedStatus.value = null;
  prepareWorkspaceContext();
  preview.value = null;
  previewConfig.value = null;
  previewKey.value = "";
  startError.value = "";
  startMessage.value = "";
  marketError.value = "";
  await refreshWorkspace();
}

async function saveExchangeConfig(request: ExchangeConfigRequest): Promise<void> {
  configBusy.value = true;
  configError.value = "";
  configMessage.value = "";
  try {
    await api.saveConfig(request);
    await loadConfig();
    activeExchange.value = request.exchange;
    configMessage.value = `${exchangeName(request.exchange)} API 配置已验证并加密保存`;
    await refreshWorkspace();
  } catch (reason) {
    configError.value = messageFrom(reason, "API 配置保存失败");
  } finally {
    configBusy.value = false;
  }
}

async function selectStrategy(grid: GridStatus): Promise<void> {
  activeExchange.value = grid.exchange;
  symbol.value = grid.symbol;
  selectedStatus.value = grid;
  prepareWorkspaceContext();
  preview.value = null;
  previewConfig.value = null;
  previewKey.value = "";
  startError.value = "";
  startMessage.value = "";
  await Promise.all([refreshMarket(), refreshDetails()]);
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
        <span class="migration-lock">{{ tradingEnabled ? "Rust 实盘写入已启用" : "Rust 只读预览" }}</span>
        <button class="ghost-button" type="button" @click="settingsOpen = true">API 配置状态</button>
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
      启动前必须通过交易所权威预览；启动请求持久化后由 Rust 状态机执行。停止只撤销本策略订单，不主动平仓，也不改动启动前已有仓位。
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
        :preview-key="visiblePreviewKey"
        :busy="previewBusy"
        :error="previewError"
        :start-busy="startBusy"
        :start-error="startError"
        :start-message="startMessage"
        :strategy-running="strategyRunning"
        :trading-enabled="tradingEnabled"
        @preview="requestPreview"
        @start="startStrategy"
      />
      <StrategyOverview
        class="dashboard-span"
        :status="selectedStatus"
        :risk="risk"
        :stop-busy="stopBusy"
        :stop-error="stopError"
        @stop="stopStrategy"
      />
      <StrategyDetailsPanel
        :exchange="activeExchange"
        :symbol="symbol"
        :configured="configured"
        :loading="detailsLoading"
        :error="detailsError"
        :availability="detailAvailability"
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
      :busy="configBusy"
      :error="configError"
      :message="configMessage"
      @close="settingsOpen = false"
      @save="saveExchangeConfig"
    />
  </main>
</template>
