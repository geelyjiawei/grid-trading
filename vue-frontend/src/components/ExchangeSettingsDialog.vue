<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { ApiConfigResponse, Exchange, ExchangeConfigRequest } from "../api/types";
import { exchangeName } from "../format";

const props = defineProps<{
  open: boolean;
  config: ApiConfigResponse | null;
  activeExchange: Exchange;
  busy: boolean;
  error: string;
  message: string;
}>();

const emit = defineEmits<{
  close: [];
  save: [request: ExchangeConfigRequest];
}>();

const exchange = ref<Exchange>(props.activeExchange);
const apiKey = ref("");
const apiSecret = ref("");
const privateKey = ref("");
const testnet = ref(false);

const summaries = computed(() =>
  (["binance", "aster", "bybit", "trade_xyz"] as Exchange[]).map((item) => ({
    exchange: item,
    config: props.config?.configs[item],
  })),
);

const selectedConfig = computed(() => props.config?.configs[exchange.value]);

function clearSecrets(): void {
  apiKey.value = "";
  apiSecret.value = "";
  privateKey.value = "";
}

function selectExchange(selected: Exchange): void {
  exchange.value = selected;
  testnet.value = props.config?.configs[selected]?.testnet === true;
  clearSecrets();
}

function submit(): void {
  const request: ExchangeConfigRequest = {
    exchange: exchange.value,
    testnet: testnet.value,
  };
  if (exchange.value === "aster") {
    request.private_key = privateKey.value;
  } else if (exchange.value === "trade_xyz") {
    request.api_key = apiKey.value;
    request.private_key = privateKey.value;
  } else {
    request.api_key = apiKey.value;
    request.api_secret = apiSecret.value;
  }
  emit("save", request);
}

watch(
  () => props.open,
  (open) => {
    if (!open) return;
    exchange.value = props.activeExchange;
    testnet.value = props.config?.configs[props.activeExchange]?.testnet === true;
    clearSecrets();
  },
);

watch(
  () => props.message,
  (message) => {
    if (message) clearSecrets();
  },
);
</script>

<template>
  <div v-if="open" class="modal-layer" role="presentation" @mousedown.self="emit('close')">
    <section class="modal-card settings-card" role="dialog" aria-modal="true" aria-label="交易所 API 配置">
      <header class="modal-header">
        <div>
          <p class="eyebrow">独立凭据仓库</p>
          <h2>交易所 API 配置</h2>
        </div>
        <button class="icon-button" type="button" :disabled="busy" @click="emit('close')">关闭</button>
      </header>

      <div class="exchange-config-list">
        <button
          v-for="item in summaries"
          :key="item.exchange"
          type="button"
          :class="{ active: exchange === item.exchange }"
          :disabled="busy"
          @click="selectExchange(item.exchange)"
        >
          <strong>{{ exchangeName(item.exchange) }}</strong>
          <span :class="item.config?.configured ? 'positive' : 'muted'">
            {{ item.config?.configured ? `${item.config.api_key || '已加载'} · ${item.config.testnet ? 'Testnet' : 'Mainnet'}` : "未配置" }}
          </span>
        </button>
      </div>

      <form class="settings-form" @submit.prevent="submit">
        <p class="form-hint">
          每个交易所独立保存，不需要先配置其他交易所。保存前会读取账户余额验证凭据；同交易所有运行策略时禁止更换。
        </p>

        <template v-if="exchange === 'aster'">
          <label>
            <span>Aster 生产钱包私钥</span>
            <input
              v-model="privateKey"
              type="password"
              autocomplete="new-password"
              placeholder="0x…"
              :disabled="busy"
              required
            />
          </label>
          <p class="form-hint">钱包地址由 Rust 从私钥推导并核对，无需单独填写。</p>
        </template>
        <template v-else-if="exchange === 'trade_xyz'">
          <label>
            <span>TRADE.XYZ 主账户地址</span>
            <input
              v-model="apiKey"
              autocomplete="off"
              placeholder="0x…"
              :disabled="busy"
              required
            />
          </label>
          <label>
            <span>Hyperliquid Agent 私钥</span>
            <input
              v-model="privateKey"
              type="password"
              autocomplete="new-password"
              placeholder="0x…"
              :disabled="busy"
              required
            />
          </label>
          <p class="form-hint">
            主账户地址用于读取 TRADE.XYZ 持仓；Agent 私钥只用于签名。保存时会核对授权关系；暂不支持子账户或 Vault。
          </p>
        </template>
        <template v-else>
          <label>
            <span>{{ exchangeName(exchange) }} API Key</span>
            <input v-model="apiKey" autocomplete="off" :disabled="busy" required />
          </label>
          <label>
            <span>{{ exchangeName(exchange) }} API Secret</span>
            <input
              v-model="apiSecret"
              type="password"
              autocomplete="new-password"
              :disabled="busy"
              required
            />
          </label>
        </template>

        <label class="check-row">
          <input v-model="testnet" type="checkbox" :disabled="busy" />
          <span>使用 Testnet</span>
        </label>

        <p class="callout">
          凭据使用服务器 <code>GRID_CONFIG_KEY</code> 加密后原子写入，页面只回显掩码。请仅通过 HTTPS 或本机回环地址提交真实密钥。
        </p>
        <p v-if="selectedConfig?.source" class="form-hint">
          当前来源：{{ selectedConfig.source === "file" ? "服务器加密文件" : "服务器环境变量" }}
        </p>
        <p v-if="error" class="form-error">{{ error }}</p>
        <p v-if="message" class="form-success">{{ message }}</p>
        <button class="primary-button" type="submit" :disabled="busy">
          {{ busy ? "正在验证并保存…" : `验证并保存 ${exchangeName(exchange)}` }}
        </button>
      </form>
    </section>
  </div>
</template>
