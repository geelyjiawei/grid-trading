<script setup lang="ts">
import { reactive } from "vue";
import type { AuthStatus, LoginRequest } from "../api/types";

defineProps<{
  status: AuthStatus | null;
  busy: boolean;
  error: string;
}>();

const emit = defineEmits<{
  submit: [credentials: LoginRequest];
}>();

const form = reactive<LoginRequest>({
  username: "admin",
  password: "",
  code: "",
});

function submit(): void {
  emit("submit", { ...form });
}
</script>

<template>
  <div class="modal-layer auth-layer" role="presentation">
    <form class="modal-card auth-card" @submit.prevent="submit">
      <p class="eyebrow">安全入口</p>
      <h2>登录控制台</h2>
      <p v-if="status && !status.configured" class="callout danger">
        服务器尚未配置完整的登录保护，请先完成服务端安全设置。
      </p>
      <p v-else-if="status?.totp_secret" class="callout">
        首次绑定验证器密钥：<strong>{{ status.totp_secret }}</strong>
      </p>

      <label>
        <span>用户名</span>
        <input v-model.trim="form.username" autocomplete="username" required />
      </label>
      <label>
        <span>密码</span>
        <input v-model="form.password" type="password" autocomplete="current-password" required />
      </label>
      <label>
        <span>动态验证码</span>
        <input
          v-model.trim="form.code"
          inputmode="numeric"
          maxlength="6"
          autocomplete="one-time-code"
          placeholder="6 位数字"
          required
        />
      </label>
      <p v-if="error" class="form-error">{{ error }}</p>
      <button class="primary-button" type="submit" :disabled="busy || !status?.configured">
        {{ busy ? "正在验证…" : "进入控制台" }}
      </button>
    </form>
  </div>
</template>
