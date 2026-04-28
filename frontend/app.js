let gridRunning = false;
let allGridStatuses = [];
let currentDirection = "long";
let toastTimer = null;
let latestPrice = NaN;
let authRequired = false;

document.addEventListener("DOMContentLoaded", async () => {
  bindEvents();
  const authenticated = await checkAuth();
  if (!authenticated) return;

  await loadConfig();
  await fetchPrice();
  await pollGridStatus();

  window.setInterval(fetchPrice, 5000);
  window.setInterval(pollGridStatus, 3000);
});

async function checkAuth() {
  try {
    const status = await api("/api/auth/status", "GET", null, { skipAuthRedirect: true });
    authRequired = Boolean(status.required);
    if (!status.required || status.authenticated) {
      hideAuthModal();
      return true;
    }

    showAuthModal(status);
    return false;
  } catch (error) {
    showAuthModal({ configured: false, error: error.message });
    return false;
  }
}

function showAuthModal(status = {}) {
  const modal = document.getElementById("auth-modal");
  const setup = document.getElementById("auth-setup");
  modal.classList.remove("hidden");

  if (!status.configured) {
    setup.textContent = "服务器尚未配置登录保护，请先在 .env 中设置 ADMIN_PASSWORD_HASH、TOTP_SECRET 和 SESSION_SECRET。";
    setup.classList.remove("hidden");
  } else if (status.totp_secret) {
    setup.textContent = `首次绑定 Google Authenticator：手动输入密钥 ${status.totp_secret}`;
    setup.classList.remove("hidden");
  } else {
    setup.classList.add("hidden");
  }
}

function hideAuthModal() {
  document.getElementById("auth-modal").classList.add("hidden");
}

async function login(event) {
  event.preventDefault();
  const errorEl = document.getElementById("auth-error");
  errorEl.classList.add("hidden");

  try {
    await api("/api/auth/login", "POST", {
      username: document.getElementById("auth-username").value.trim(),
      password: document.getElementById("auth-password").value,
      code: document.getElementById("auth-code").value.trim(),
    }, { skipAuthRedirect: true });

    hideAuthModal();
    await loadConfig();
    await fetchPrice();
    await pollGridStatus();
    window.setInterval(fetchPrice, 5000);
    window.setInterval(pollGridStatus, 3000);
  } catch (error) {
    errorEl.textContent = error.message;
    errorEl.classList.remove("hidden");
  }
}

function bindEvents() {
  const slider = document.getElementById("leverage-slider");
  slider.addEventListener("input", () => {
    document.getElementById("leverage").value = slider.value;
    document.getElementById("leverage-val").textContent = `${slider.value}x`;
    updatePreview();
  });

  ["upper-price", "lower-price", "grid-count", "total-investment", "grid-mode"].forEach((id) => {
    document.getElementById(id).addEventListener("input", updatePreview);
    document.getElementById(id).addEventListener("change", updatePreview);
  });

  document.getElementById("symbol-input").addEventListener("change", async () => {
    await fetchPrice();
    updatePreview();
    if (!isSymbolRunning(getSymbol())) {
      clearPositions();
    }
    await pollGridStatus();
  });
}

async function loadConfig() {
  try {
    const config = await api("/api/config");
    document.getElementById("cfg-testnet").checked = Boolean(config.testnet);
    renderConfigStatus(config);
    if (config.configured) {
      await fetchBalance();
    }
  } catch (_) {
    // Ignore on first load.
  }
}

function renderConfigStatus(config) {
  const statusEl = document.getElementById("cfg-status");
  if (!statusEl) return;

  if (!config.configured) {
    statusEl.textContent = "当前未保存 API 配置";
    statusEl.className = "config-status";
    return;
  }

  const source = config.source === "env" ? "环境变量" : "本地加密文件";
  statusEl.textContent = `已配置：${config.api_key} · ${config.testnet ? "Testnet" : "Mainnet"} · ${source}`;
  statusEl.className = "config-status configured";
}

function setDirection(direction) {
  currentDirection = direction;
  document.getElementById("direction").value = direction;
  document.getElementById("btn-long").className = `dir-btn${direction === "long" ? " active long" : ""}`;
  document.getElementById("btn-short").className = `dir-btn${direction === "short" ? " active short" : ""}`;
  document.getElementById("btn-neutral").className = `dir-btn${direction === "neutral" ? " active" : ""}`;
}

function updatePreview() {
  const upper = parseFloat(document.getElementById("upper-price").value);
  const lower = parseFloat(document.getElementById("lower-price").value);
  const count = parseInt(document.getElementById("grid-count").value, 10);
  const investment = parseFloat(document.getElementById("total-investment").value);
  const leverage = parseInt(document.getElementById("leverage").value, 10);
  const gridMode = document.getElementById("grid-mode").value;
  const box = document.getElementById("grid-preview");

  if (!upper || !lower || !count || !investment || upper <= lower) {
    box.classList.add("hidden");
    return;
  }

  const referencePrice = Number.isFinite(latestPrice) ? latestPrice : (upper + lower) / 2;
  let step;
  let gridPct;

  if (gridMode === "geometric") {
    const ratio = Math.pow(upper / lower, 1 / count);
    gridPct = (ratio - 1) * 100;
    step = referencePrice * (ratio - 1);
  } else {
    step = (upper - lower) / count;
    gridPct = referencePrice > 0 ? (step / referencePrice) * 100 : 0;
  }

  const totalQty = referencePrice > 0 ? (investment * leverage) / referencePrice : 0;
  const perGridQty = totalQty / count;
  const perGridProfit = step * perGridQty;

  document.getElementById("prev-step").textContent = gridMode === "geometric" ? `${step.toFixed(6)} / ${(gridPct).toFixed(3)}%` : step.toFixed(6);
  document.getElementById("prev-profit-pct").textContent = `${gridPct.toFixed(3)}%`;
  document.getElementById("prev-profit").textContent = perGridProfit.toFixed(4);
  document.getElementById("prev-qty").textContent = perGridQty.toFixed(6);
  document.getElementById("prev-total-qty").textContent = totalQty.toFixed(6);
  box.classList.remove("hidden");
}

async function fetchPrice() {
  const symbol = getSymbol();
  if (!symbol) return;

  try {
    const data = await api(`/api/price/${symbol}`);
    latestPrice = parseFloat(data.last_price);
    const markPrice = parseFloat(data.mark_price);
    const pct = parseFloat(data.price_24h_pcnt || "0") * 100;

    document.getElementById("last-price").textContent = fmtMarket(latestPrice);
    document.getElementById("mark-price").textContent = fmtMarket(markPrice);
    document.getElementById("vol-24h").textContent = formatVolume(data.volume_24h);

    const changeEl = document.getElementById("price-change");
    changeEl.textContent = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
    changeEl.className = `price-change ${pct >= 0 ? "up" : "down"}`;
    updatePreview();
  } catch (error) {
    showToast(`获取行情失败：${error.message}`, "error");
  }
}

async function fetchBalance() {
  try {
    const data = await api("/api/balance");
    document.getElementById("balance-avail").textContent = fmtNum(data.available);
    document.getElementById("balance-equity").textContent = fmtNum(data.equity);
    const pnl = Number(data.unrealised_pnl || 0);
    const pnlEl = document.getElementById("balance-upnl");
    pnlEl.textContent = fmtNum(data.unrealised_pnl);
    pnlEl.className = pnl >= 0 ? "positive" : "negative";
  } catch (error) {
    showToast(`获取余额失败：${error.message}`, "error");
  }
}

async function startGrid() {
  const payload = {
    symbol: getSymbol(),
    direction: currentDirection,
    grid_mode: document.getElementById("grid-mode").value,
    upper_price: parseFloat(document.getElementById("upper-price").value),
    lower_price: parseFloat(document.getElementById("lower-price").value),
    grid_count: parseInt(document.getElementById("grid-count").value, 10),
    total_investment: parseFloat(document.getElementById("total-investment").value),
    leverage: parseInt(document.getElementById("leverage").value, 10),
    trigger_price: parseOptionalNumber("trigger-price"),
    stop_loss_price: parseOptionalNumber("stop-loss-price"),
    take_profit_price: parseOptionalNumber("take-profit-price"),
  };

  if (!payload.symbol) {
    showToast("请输入交易对", "error");
    return;
  }
  if (!payload.upper_price || !payload.lower_price || payload.upper_price <= payload.lower_price) {
    showToast("价格区间填写不正确", "error");
    return;
  }
  if (!payload.grid_count || payload.grid_count < 2) {
    showToast("网格数量至少为 2", "error");
    return;
  }
  if (!payload.total_investment || payload.total_investment <= 0) {
    showToast("总投入金额必须大于 0", "error");
    return;
  }

  try {
    const result = await api("/api/grid/start", "POST", payload);
    showToast(result.message, "success");
    await fetchBalance();
    await pollGridStatus();
  } catch (error) {
    showToast(`启动失败：${error.message}`, "error");
  }
}

async function stopGrid() {
  const symbol = getSymbol();
  if (!window.confirm(`确认停止 ${symbol} 网格并撤销该交易对全部挂单吗？`)) return;

  try {
    const result = await api(`/api/grid/stop/${symbol}`, "POST");
    showToast(result.message, "success");
    await pollGridStatus();
    clearPositions();
  } catch (error) {
    showToast(`停止失败：${error.message}`, "error");
  }
}

async function pollGridStatus() {
  try {
    const summary = await api("/api/grid/status");
    allGridStatuses = Array.isArray(summary.engines) ? summary.engines : [];
    const currentStatus = findStatusForSymbol(getSymbol()) || { running: false, symbol: getSymbol() };
    renderRunningGrids(allGridStatuses);
    renderStatus(currentStatus, summary);
    if (currentStatus.running && currentStatus.grid_ready) {
      await fetchPositions();
    } else {
      clearPositions();
    }
  } catch (_) {
    // Keep UI quiet during polling.
  }
}

function renderStatus(status, summary = {}) {
  gridRunning = Boolean(status.running);

  const dot = document.getElementById("nav-status");
  const text = document.getElementById("nav-status-text");
  const liveTag = document.getElementById("live-tag-text");
  const anyRunning = Boolean(summary.running || gridRunning);
  const runningCount = Number(summary.running_count || (gridRunning ? 1 : 0));
  dot.className = `status-dot ${anyRunning ? "running" : "stopped"}`;

  if (status.waiting_trigger) {
    text.textContent = "等待触发";
    liveTag.textContent = "等待触发";
  } else {
    text.textContent = anyRunning ? (gridRunning ? "当前运行中" : `${runningCount} 个运行中`) : "未运行";
    liveTag.textContent = gridRunning ? "运行中" : "未运行";
  }

  document.getElementById("btn-start").classList.toggle("hidden", gridRunning);
  document.getElementById("btn-stop").classList.toggle("hidden", !gridRunning);
  document.getElementById("status-card").classList.toggle("hidden", !gridRunning);

  if (!gridRunning) {
    document.getElementById("orders-body").innerHTML = '<div class="empty-state">网格未启动</div>';
    renderPendingOrders([], status);
    document.getElementById("filled-body").innerHTML = '<tr><td class="empty-state" colspan="5">暂无成交</td></tr>';
    return;
  }

  document.getElementById("st-symbol").textContent = status.symbol || "--";
  document.getElementById("st-direction").textContent = mapDirection(status.direction);
  document.getElementById("st-mode").textContent = status.grid_mode === "geometric" ? "等比" : "等差";
  document.getElementById("st-grid-profit-pct").textContent = status.grid_profit_pct ? `${Number(status.grid_profit_pct).toFixed(3)}%` : "--";
  document.getElementById("st-completed-pairs").textContent = String(status.completed_pairs ?? 0);

  const profit = Number(status.total_profit ?? 0);
  const profitEl = document.getElementById("st-profit");
  profitEl.textContent = `${fmtNum(profit)} USDT`;
  profitEl.className = profit >= 0 ? "positive" : "negative";

  const initialSide = status.initial_side === "Buy" ? "买入" : status.initial_side === "Sell" ? "卖出" : "--";
  document.getElementById("st-initial").textContent = status.initial_qty ? `${initialSide} ${Number(status.initial_qty).toFixed(6)}` : (status.trigger_message || "--");

  renderOrders(status.active_orders || []);
  renderPendingOrders(status.active_orders || [], status);
  renderFilled(status.filled_orders || []);
}

function renderRunningGrids(statuses) {
  const body = document.getElementById("running-grids-body");
  const runningStatuses = statuses
    .filter((status) => status.running)
    .sort((a, b) => String(a.symbol).localeCompare(String(b.symbol)));

  if (!runningStatuses.length) {
    body.innerHTML = '<div class="empty-state">暂无运行中的策略</div>';
    return;
  }

  body.innerHTML = runningStatuses.map((status) => {
    const profit = Number(status.total_profit || 0);
    const completedPairs = Number(status.completed_pairs || 0);
    return `
      <button class="grid-summary-item${status.symbol === getSymbol() ? " active" : ""}" type="button" onclick="selectGridSymbol('${status.symbol}')">
        <span>
          <strong>${status.symbol}</strong>
          <small>${mapDirection(status.direction)} · ${status.grid_mode === "geometric" ? "等比" : "等差"}</small>
        </span>
        <span>
          <strong class="${profit >= 0 ? "positive" : "negative"}">${fmtNum(profit)}</strong>
          <small>完成 ${completedPairs} 次</small>
        </span>
      </button>
    `;
  }).join("");
}

async function selectGridSymbol(symbol) {
  document.getElementById("symbol-input").value = symbol;
  await fetchPrice();
  await pollGridStatus();
}

function renderPendingOrders(orders, status = {}) {
  const body = document.getElementById("pending-orders-body");
  const buyCountEl = document.getElementById("pending-buy-count");
  const sellCountEl = document.getElementById("pending-sell-count");
  const buyBar = document.getElementById("pending-buy-bar");
  const sellBar = document.getElementById("pending-sell-bar");
  const qtyEl = document.getElementById("pending-qty");
  const lastPriceEl = document.getElementById("pending-last-price");

  const currentPrice = Number(status.current_price || latestPrice);
  const buyOrders = orders
    .filter((order) => order.side === "Buy")
    .sort((a, b) => Number(b.price) - Number(a.price));
  const sellOrders = orders
    .filter((order) => order.side === "Sell")
    .sort((a, b) => Number(a.price) - Number(b.price));

  buyCountEl.textContent = String(buyOrders.length);
  sellCountEl.textContent = String(sellOrders.length);
  qtyEl.textContent = formatOrderQty(status.config?.qty_per_grid || orders[0]?.qty);
  lastPriceEl.textContent = Number.isFinite(currentPrice) ? `${fmtMarket(currentPrice)} USDT` : "--";

  const total = buyOrders.length + sellOrders.length;
  const buyPct = total ? (buyOrders.length / total) * 100 : 50;
  const sellPct = total ? (sellOrders.length / total) * 100 : 50;
  buyBar.style.width = `${buyPct}%`;
  sellBar.style.width = `${sellPct}%`;

  if (!orders.length) {
    body.innerHTML = '<div class="empty-state">暂无待处理订单</div>';
    return;
  }

  const rows = Math.max(buyOrders.length, sellOrders.length);
  body.innerHTML = Array.from({ length: rows }, (_, index) => {
    const buy = buyOrders[index];
    const sell = sellOrders[index];
    const buyDistance = buy ? formatDistancePercent(buy.price, currentPrice) : "";
    const sellDistance = sell ? formatDistancePercent(sell.price, currentPrice) : "";

    return `
      <div class="pending-row">
        <span class="pending-distance">${buyDistance}</span>
        <div class="pending-price-pair">
          <span class="pending-buy-price">${buy ? fmtMarket(buy.price) : ""}</span>
          <span class="pending-index">${index + 1}</span>
          <span class="pending-sell-price">${sell ? fmtMarket(sell.price) : ""}</span>
        </div>
        <span class="pending-distance">${sellDistance}</span>
      </div>
    `;
  }).join("");
}

function renderOrders(orders) {
  const el = document.getElementById("orders-body");
  if (!orders.length) {
    el.innerHTML = '<div class="empty-state">暂无挂单</div>';
    return;
  }

  const sorted = [...orders].sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
  el.innerHTML = sorted.map((order) => `
    <div class="order-item">
      <div>
        <div class="${order.side === "Buy" ? "side-buy" : "side-sell"}">${order.side === "Buy" ? "买入" : "卖出"}</div>
        <div class="muted">${order.reduce_only ? "止盈/平仓" : "开仓/补仓"}</div>
      </div>
      <div class="order-price">${fmtMarket(order.price)}</div>
      <div class="muted">${order.qty}</div>
    </div>
  `).join("");
}

function renderFilled(filledOrders) {
  const tbody = document.getElementById("filled-body");
  if (!filledOrders.length) {
    tbody.innerHTML = '<tr><td class="empty-state" colspan="5">暂无成交</td></tr>';
    return;
  }

  tbody.innerHTML = [...filledOrders]
    .reverse()
    .slice(0, 30)
    .map((item) => {
      const profit = Number(item.profit || 0);
      return `
        <tr>
          <td class="${item.side === "Buy" ? "side-buy" : "side-sell"}">${item.side}</td>
          <td>${fmtMarket(item.price)}</td>
          <td>${item.qty}</td>
          <td class="${profit >= 0 ? "positive" : "negative"}">${item.reduce_only ? `${profit >= 0 ? "+" : ""}${profit.toFixed(4)}` : "--"}</td>
          <td>${new Date(item.time * 1000).toLocaleTimeString()}</td>
        </tr>
      `;
    })
    .join("");
}

async function fetchPositions() {
  const symbol = getSymbol();
  try {
    const data = await api(`/api/positions/${symbol}`);
    const el = document.getElementById("positions-body");
    if (!data.positions.length) {
      el.innerHTML = '<div class="empty-state">暂无持仓</div>';
      return;
    }

    el.innerHTML = data.positions.map((position) => {
      const pnl = Number(position.unrealised_pnl || 0);
      const direction = position.side === "Buy" ? "多仓" : "空仓";
      const sideClass = position.side === "Buy" ? "side-buy" : "side-sell";
      return `
        <div class="position-card">
          <div class="position-head">
            <strong class="${sideClass}">${direction}</strong>
            <span class="muted">${position.leverage}x</span>
          </div>
          <div class="position-grid">
            <div><span>数量</span><strong>${position.size}</strong></div>
            <div><span>开仓均价</span><strong>${fmtMarket(position.entry_price)}</strong></div>
            <div><span>标记价</span><strong>${fmtMarket(position.mark_price)}</strong></div>
            <div><span>未实现盈亏</span><strong class="${pnl >= 0 ? "positive" : "negative"}">${fmtNum(position.unrealised_pnl)}</strong></div>
            <div><span>强平价</span><strong>${position.liq_price || "--"}</strong></div>
          </div>
        </div>
      `;
    }).join("");
  } catch (_) {
    // Keep polling quiet.
  }
}

function clearPositions() {
  document.getElementById("positions-body").innerHTML = '<div class="empty-state">暂无持仓</div>';
}

function openApiModal() {
  document.getElementById("api-modal").classList.remove("hidden");
  document.getElementById("cfg-error").classList.add("hidden");
}

function closeApiModal() {
  document.getElementById("api-modal").classList.add("hidden");
}

function closeModalOutside(event) {
  if (event.target.id === "api-modal") {
    closeApiModal();
  }
}

async function saveApiConfig() {
  const apiKey = document.getElementById("cfg-api-key").value.trim();
  const apiSecret = document.getElementById("cfg-api-secret").value.trim();
  const testnet = document.getElementById("cfg-testnet").checked;
  const errorEl = document.getElementById("cfg-error");

  if (!apiKey || !apiSecret) {
    errorEl.textContent = "请完整填写 API Key 和 API Secret";
    errorEl.classList.remove("hidden");
    return;
  }

  try {
    const result = await api("/api/config", "POST", { api_key: apiKey, api_secret: apiSecret, testnet });
    errorEl.classList.add("hidden");
    document.getElementById("cfg-api-key").value = "";
    document.getElementById("cfg-api-secret").value = "";
    await loadConfig();
    closeApiModal();
    showToast(result.message, "success");
    await fetchBalance();
  } catch (error) {
    errorEl.textContent = error.message;
    errorEl.classList.remove("hidden");
  }
}

async function api(url, method = "GET", body = null, optionsOverride = {}) {
  const options = { method, headers: { "Content-Type": "application/json" } };
  if (body !== null) options.body = JSON.stringify(body);

  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (response.status === 401 && authRequired && !optionsOverride.skipAuthRedirect) {
    showAuthModal({ configured: true });
    throw new Error("请先登录");
  }
  if (!response.ok) throw new Error(data.detail || "请求失败");
  return data;
}

function parseOptionalNumber(id) {
  const value = document.getElementById(id).value.trim();
  return value === "" ? null : Number(value);
}

function mapDirection(direction) {
  if (direction === "long") return "做多";
  if (direction === "short") return "做空";
  if (direction === "neutral") return "中性";
  return "--";
}

function getSymbol() {
  return document.getElementById("symbol-input").value.trim().toUpperCase();
}

function findStatusForSymbol(symbol) {
  return allGridStatuses.find((status) => String(status.symbol).toUpperCase() === symbol);
}

function isSymbolRunning(symbol) {
  const status = findStatusForSymbol(symbol);
  return Boolean(status && status.running);
}

function fmtNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(4) : "--";
}

function fmtMarket(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toLocaleString() : "--";
}

function formatOrderQty(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  return Number.isInteger(num) ? num.toLocaleString() : num.toFixed(6).replace(/0+$/, "").replace(/\.$/, "");
}

function formatDistancePercent(price, currentPrice) {
  const orderPrice = Number(price);
  const last = Number(currentPrice);
  if (!Number.isFinite(orderPrice) || !Number.isFinite(last) || last <= 0) return "--";
  const pct = ((orderPrice - last) / last) * 100;
  return `${pct >= 0 ? "" : ""}${pct.toFixed(2)}%`;
}

function formatVolume(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
  if (num >= 1e3) return `${(num / 1e3).toFixed(2)}K`;
  return num.toFixed(2);
}

function showToast(message, type = "info") {
  let toast = document.getElementById("toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "toast";
    document.body.appendChild(toast);
  }

  toast.className = `toast ${type}`;
  toast.textContent = message;
  toast.classList.add("show");

  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => {
    toast.classList.remove("show");
  }, 3000);
}
