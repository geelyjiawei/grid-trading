<script setup lang="ts">
import { computed, reactive, watch } from "vue";
import type {
  ApiConfigResponse,
  Exchange,
  SaveApiConfigRequest,
} from "../api/types";
import { exchangeName } from "../format";

const props = defineProps<{
  open: boolean;
  config: ApiConfigResponse | null;
  activeExchange: Exchange;
  busy: boolean;
  error: string;
}>();

const emit = defineEmits<{
  close: [];
  save: [config: SaveApiConfigRequest];
}>();

const form = reactive<SaveApiConfigRequest>({
  exchange: props.activeExchange,
  api_key: "",
  api_secret: "",
  testnet: false,
});

watch(
  () => [props.open, props.activeExchange] as const,
  ([open, exchange]) => {
    if (!open) return;
    form.exchange = exchange;
    form.api_key = "";
    form.api_secret = "";
    form.testnet = exchange === "aster"
      ? false
      : Boolean(props.config?.configs[exchange]?.testnet);
  },
  { immediate: true },
);

watch(
  () => form.exchange,
  (exchange) => {
    form.testnet = exchange === "aster"
      ? false
      : Boolean(props.config?.configs[exchange]?.testnet);
    form.api_key = "";
    form.api_secret = "";
  },
);

const summaries = computed(() =>
  (["binance", "aster", "bybit"] as Exchange[]).map((exchange) => ({
    exchange,
    config: props.config?.configs[exchange],
  })),
);

function save(): void {
  emit("save", { ...form });
}
</script>

<template>
  <div v-if="open" class="modal-layer" role="presentation" @mousedown.self="emit('close')">
    <section class="modal-card settings-card" role="dialog" aria-modal="true" aria-label="交易所 API 设置">
      <header class="modal-header">
        <div>
          <p class="eyebrow">独立账户配置</p>
          <h2>交易所 API 设置</h2>
        </div>
        <button class="icon-button" type="button" @click="emit('close')">关闭</button>
      </header>

      <div class="exchange-config-list">
        <article v-for="item in summaries" :key="item.exchange">
          <strong>{{ exchangeName(item.exchange) }}</strong>
          <span :class="item.config?.configured ? 'positive' : 'muted'">
            {{ item.config?.configured ? `${item.config.api_key || '已保存'} · ${item.config.testnet ? 'Testnet' : 'Mainnet'}` : "未配置" }}
          </span>
        </article>
      </div>

      <p v-if="config?.storage_error" class="callout danger">
        加密配置存储异常。为保护现有密钥，后端已暂停覆盖写入。
      </p>

      <form class="settings-form" @submit.prevent="save">
        <label>
          <span>正在编辑</span>
          <select v-model="form.exchange">
            <option value="binance">Binance</option>
            <option value="aster">AsterDEX</option>
            <option value="bybit">Bybit</option>
          </select>
        </label>
        <label>
          <span>{{ form.exchange === "aster" ? "生产钱包地址" : "API Key" }}</span>
          <input v-model.trim="form.api_key" autocomplete="off" required />
        </label>
        <label>
          <span>{{ form.exchange === "aster" ? "生产钱包私钥" : "API Secret" }}</span>
          <input v-model.trim="form.api_secret" type="password" autocomplete="new-password" required />
        </label>
        <label v-if="form.exchange !== 'aster'" class="check-row">
          <input v-model="form.testnet" type="checkbox" />
          <span>使用测试网络</span>
        </label>
        <p class="form-hint">
          {{ form.exchange === "aster" ? "Aster 只保存生产钱包与对应私钥。" : "可分别保存正式网和测试网配置。" }}
          只更新当前选择的交易所，不会清除其他交易所配置。
        </p>
        <p v-if="error" class="form-error">{{ error }}</p>
        <button class="primary-button" type="submit" :disabled="busy || Boolean(config?.storage_error)">
          {{ busy ? "正在安全保存…" : `保存 ${exchangeName(form.exchange)}` }}
        </button>
      </form>
    </section>
  </div>
</template>
