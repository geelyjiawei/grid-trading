<script setup lang="ts">
import { onMounted, onUnmounted, ref } from "vue";
import { ApiError, api } from "./api/client";
import type { GridStatus } from "./api/types";

const loading = ref(true);
const error = ref("");
const authenticated = ref(false);
const grids = ref<GridStatus[]>([]);
let pollTimer: number | undefined;

async function refresh(): Promise<void> {
  try {
    const status = await api.gridStatus();
    grids.value = status.grids ?? [];
    error.value = "";
  } catch (reason) {
    error.value = reason instanceof ApiError ? reason.message : "无法读取策略状态";
  } finally {
    loading.value = false;
  }
}

onMounted(async () => {
  try {
    const status = await api.authStatus();
    authenticated.value = !status.required || status.authenticated;
    if (authenticated.value) await refresh();
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : "认证状态读取失败";
    loading.value = false;
  }
  pollTimer = window.setInterval(() => {
    if (authenticated.value) void refresh();
  }, 3000);
});

onUnmounted(() => window.clearInterval(pollTimer));
</script>

<template>
  <main class="shell">
    <header class="masthead">
      <div>
        <p class="eyebrow">Vue 3 migration</p>
        <h1>合约网格控制台</h1>
        <p class="subtitle">交易写入仍由已验证的旧服务承担，Rust 对照通过前不会切换。</p>
      </div>
      <button type="button" :disabled="loading" @click="refresh">刷新状态</button>
    </header>

    <section v-if="error" class="notice danger">{{ error }}</section>
    <section v-else-if="!authenticated" class="notice">需要先在现有登录入口完成认证。</section>
    <section v-else class="strategy-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">Live contract</p>
          <h2>运行中策略</h2>
        </div>
        <strong>{{ grids.length }}</strong>
      </div>

      <p v-if="loading" class="empty">正在读取策略...</p>
      <p v-else-if="grids.length === 0" class="empty">当前没有运行中的策略</p>
      <article v-for="grid in grids" :key="`${grid.exchange}:${grid.symbol}`" class="strategy">
        <div>
          <h3>{{ grid.symbol }}</h3>
          <p>{{ grid.exchange }} · {{ grid.direction ?? "--" }} · {{ grid.grid_mode ?? "--" }}</p>
        </div>
        <div class="strategy-metrics">
          <strong>{{ Number(grid.total_profit ?? 0).toFixed(4) }}</strong>
          <span>完成 {{ grid.completed_pairs ?? 0 }} 次</span>
        </div>
      </article>
    </section>
  </main>
</template>
