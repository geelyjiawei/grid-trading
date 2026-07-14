<script setup lang="ts">
import { computed } from "vue";
import type { ApiConfigResponse, Exchange } from "../api/types";
import { exchangeName } from "../format";

const props = defineProps<{
  open: boolean;
  config: ApiConfigResponse | null;
  activeExchange: Exchange;
}>();

const emit = defineEmits<{
  close: [];
}>();

const summaries = computed(() =>
  (["binance", "aster", "bybit"] as Exchange[]).map((exchange) => ({
    exchange,
    config: props.config?.configs[exchange],
  })),
);

const environmentVariables: Record<Exchange, string> = {
  binance: "BINANCE_API_KEY / BINANCE_API_SECRET",
  aster: "ASTER_USER_ADDRESS / ASTER_SIGNER_PRIVATE_KEY",
  bybit: "BYBIT_API_KEY / BYBIT_API_SECRET",
};
</script>

<template>
  <div v-if="open" class="modal-layer" role="presentation" @mousedown.self="emit('close')">
    <section class="modal-card settings-card" role="dialog" aria-modal="true" aria-label="交易所连接状态">
      <header class="modal-header">
        <div>
          <p class="eyebrow">服务器启动配置</p>
          <h2>交易所连接状态</h2>
        </div>
        <button class="icon-button" type="button" @click="emit('close')">关闭</button>
      </header>

      <div class="exchange-config-list">
        <article v-for="item in summaries" :key="item.exchange">
          <strong>{{ exchangeName(item.exchange) }}</strong>
          <span :class="item.config?.configured ? 'positive' : 'muted'">
            {{ item.config?.configured ? `${item.config.api_key || '已加载'} · ${item.config.testnet ? 'Testnet' : 'Mainnet'}` : "未配置" }}
          </span>
          <small :class="item.exchange === activeExchange ? 'positive' : 'muted'">
            {{ environmentVariables[item.exchange] }}{{ item.exchange === activeExchange ? " · 当前工作区" : "" }}
          </small>
        </article>
      </div>

      <p class="callout">
        Rust 候选版只在服务器启动时从 <code>.env</code> 读取凭据。浏览器不会接收、回显或写入私钥，避免把生产密钥留在网页请求和日志中。
      </p>
      <p class="form-hint">
        修改服务器 <code>/opt/grid-trading/.env</code> 后，需要重建或重启候选容器，再回来确认状态。真实的密钥文件不得提交到 GitHub。
      </p>
    </section>
  </div>
</template>
