let gridRunning = false;
let allGridStatuses = [];
let currentDirection = "long";
let toastTimer = null;
let latestPrice = NaN;
let authRequired = false;
let activeDetailPanel = "positions";
let activeExchange = "bybit";
let currentRiskSymbol = "";
let previewRequestSeq = 0;
let exchangeOpenOrders = [];
let exchangeTrades = [];
let exchangeConfigs = {};

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

  [
    "upper-price",
    "lower-price",
    "grid-count",
    "total-investment",
    "grid-order-qty",
    "maker-fee-rate",
    "taker-fee-rate",
    "grid-mode",
    "initial-order-type",
    "initial-order-price",
    "trigger-price",
    "stop-loss-price",
    "take-profit-price",
  ].forEach((id) => {
    document.getElementById(id).addEventListener("input", updatePreview);
    document.getElementById(id).addEventListener("change", updatePreview);
  });
  document.getElementById("position-sizing-mode").addEventListener("change", updateSizingModeVisibility);
  updateSizingModeVisibility(false);

  document.getElementById("symbol-input").addEventListener("change", async () => {
    await fetchPrice();
    updatePreview();
    if (!isSymbolRunning(getSymbol())) {
      clearPositions();
    }
    await pollGridStatus();
  });

  document.getElementById("active-exchange-select").addEventListener("change", async () => {
    activeExchange = document.getElementById("active-exchange-select").value;
    syncExchangeInputs();
    await fetchPrice();
    await fetchBalance();
    await pollGridStatus();
    updatePreview();
  });

  document.getElementById("cfg-exchange").addEventListener("change", renderConfigDraftHint);
}

function updateSizingModeVisibility(shouldPreview = true) {
  const mode = document.getElementById("position-sizing-mode").value;
  const fixedQtyMode = mode === "fixed_grid_qty";
  document.getElementById("total-investment-group").classList.toggle("hidden", fixedQtyMode);
  document.getElementById("grid-order-qty-group").classList.toggle("hidden", !fixedQtyMode);
  if (shouldPreview) updatePreview();
}

function exchangeDisplayName(exchange) {
  if (exchange === "binance") return "Binance";
  if (exchange === "aster") return "AsterDEX";
  return "Bybit";
}

function exchangeQuery(exchange = activeExchange) {
  return `exchange=${encodeURIComponent(exchange || "bybit")}`;
}

function withExchange(url, exchange = activeExchange) {
  return `${url}${url.includes("?") ? "&" : "?"}${exchangeQuery(exchange)}`;
}

function syncExchangeInputs() {
  const activeSelect = document.getElementById("active-exchange-select");
  const configSelect = document.getElementById("cfg-exchange");
  const hint = document.getElementById("active-exchange-hint");
  if (activeSelect) activeSelect.value = activeExchange;
  if (configSelect && !configSelect.value) configSelect.value = activeExchange;
  if (hint) {
    hint.textContent = `当前工作区：${exchangeDisplayName(activeExchange)}。行情、余额、网格、挂单和风控都会使用该交易所。`;
  }
}

async function loadConfig() {
  try {
    const config = await api("/api/config");
    exchangeConfigs = config.configs || {};
    activeExchange = activeExchange || config.active_exchange || config.exchange || "bybit";
    if (!config.configs?.[activeExchange]?.configured) {
      activeExchange = config.active_exchange || config.exchange || activeExchange;
    }
    document.getElementById("cfg-exchange").value = activeExchange;
    document.getElementById("cfg-testnet").checked = Boolean(config.configs?.[activeExchange]?.testnet ?? config.testnet);
    syncExchangeInputs();
    renderConfigStatus(config);
    renderConfigDraftHint();
    if (config.configs?.[activeExchange]?.configured || config.configured) {
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
    const configured = Object.values(config.configs || {}).filter((item) => item.configured);
    statusEl.textContent = configured.length
      ? `已配置：${configured.map((item) => exchangeDisplayName(item.exchange)).join(" / ")}`
      : "当前未保存 API 配置";
    statusEl.className = "config-status";
    return;
  }

  const configs = config.configs || {};
  const configured = Object.values(configs).filter((item) => item.configured);
  const summary = configured.map((item) => {
    const source = item.source === "env" ? "环境变量" : "本地加密文件";
    return `${exchangeDisplayName(item.exchange)} · ${item.api_key} · ${item.testnet ? "Testnet" : "Mainnet"} · ${source}`;
  }).join(" | ");
  statusEl.textContent = summary || "当前未保存 API 配置";
  statusEl.className = "config-status configured";
}

function renderConfigDraftHint() {
  const hintEl = document.getElementById("cfg-draft-hint");
  if (!hintEl) return;

  const selected = document.getElementById("cfg-exchange").value;
  const selectedName = exchangeDisplayName(selected);
  hintEl.textContent = `正在编辑 ${selectedName} 配置。保存后只更新该交易所，不会影响其他交易所正在运行的网格。`;
}

function setDirection(direction) {
  currentDirection = direction;
  document.getElementById("direction").value = direction;
  document.getElementById("btn-long").className = `dir-btn${direction === "long" ? " active long" : ""}`;
  document.getElementById("btn-short").className = `dir-btn${direction === "short" ? " active short" : ""}`;
  document.getElementById("btn-neutral").className = `dir-btn${direction === "neutral" ? " active" : ""}`;
}

async function updatePreview() {
  const upper = parseFloat(document.getElementById("upper-price").value);
  const lower = parseFloat(document.getElementById("lower-price").value);
  const count = parseInt(document.getElementById("grid-count").value, 10);
  const investment = parseFloat(document.getElementById("total-investment").value);
  const sizingMode = document.getElementById("position-sizing-mode").value;
  const gridOrderQty = parseFloat(document.getElementById("grid-order-qty").value);
  const leverage = parseInt(document.getElementById("leverage").value, 10);
  const makerFeeRate = parsePercentRate("maker-fee-rate");
  const takerFeeRate = parsePercentRate("taker-fee-rate");
  const gridMode = document.getElementById("grid-mode").value;
  const box = document.getElementById("grid-preview");
  const symbol = getSymbol();

  const hasSizingInput = sizingMode === "fixed_grid_qty"
    ? gridOrderQty > 0
    : investment > 0;
  if (!symbol || !upper || !lower || !count || !hasSizingInput || upper <= lower) {
    box.classList.add("hidden");
    return;
  }

  const requestSeq = ++previewRequestSeq;
  try {
    const preview = await api("/api/grid/preview", "POST", {
      exchange: activeExchange,
      symbol,
      direction: currentDirection,
      grid_mode: gridMode,
      upper_price: upper,
      lower_price: lower,
      grid_count: count,
      total_investment: sizingMode === "fixed_grid_qty" ? 0 : investment,
      leverage,
      position_sizing_mode: sizingMode,
      grid_order_qty: Number.isFinite(gridOrderQty) ? gridOrderQty : null,
      fee_rate: takerFeeRate,
      maker_fee_rate: makerFeeRate,
      taker_fee_rate: takerFeeRate,
      initial_order_type: document.getElementById("initial-order-type").value,
      initial_order_price: parseOptionalNumber("initial-order-price"),
      grid_order_post_only: document.getElementById("grid-order-post-only").checked,
      trigger_price: parseOptionalNumber("trigger-price"),
      stop_loss_price: parseOptionalNumber("stop-loss-price"),
      take_profit_price: parseOptionalNumber("take-profit-price"),
    });
    if (requestSeq !== previewRequestSeq) return;

    const minQty = Number(preview.qty_per_grid_min || 0);
    const maxQty = Number(preview.qty_per_grid_max || 0);
    const qtyText = Math.abs(maxQty - minQty) > 0
      ? `${formatOrderQty(minQty)} - ${formatOrderQty(maxQty)}`
      : formatOrderQty(preview.qty_per_grid_avg);

    document.getElementById("prev-step").textContent = gridMode === "geometric"
      ? `${Number(preview.grid_step).toFixed(6)} / ${Number(preview.grid_profit_pct).toFixed(3)}%`
      : Number(preview.grid_step).toFixed(6);
    document.getElementById("prev-profit-pct").textContent = `${Number(preview.grid_profit_pct).toFixed(3)}%`;
    document.getElementById("prev-gross-profit").textContent = Number(preview.per_grid_gross_profit).toFixed(4);
    document.getElementById("prev-fee").textContent = `${Number(preview.per_grid_fee).toFixed(4)}（开 ${Number(preview.per_grid_open_fee || 0).toFixed(4)} / 平 ${Number(preview.per_grid_close_fee || 0).toFixed(4)}）`;
    document.getElementById("prev-profit").textContent = Number(preview.per_grid_net_profit).toFixed(4);
    document.getElementById("prev-active-count").textContent = `${preview.active_grid_count} / ${preview.grid_count}`;
    document.getElementById("prev-qty").textContent = qtyText;
    document.getElementById("prev-total-qty").textContent = formatOrderQty(preview.total_qty);
    box.classList.remove("hidden");
  } catch (_) {
    box.classList.add("hidden");
  }
}

async function fetchPrice() {
  const symbol = getSymbol();
  if (!symbol) return;

  try {
    const data = await api(withExchange(`/api/price/${symbol}`));
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
  if (exchangeConfigs[activeExchange] && !exchangeConfigs[activeExchange].configured) {
    document.getElementById("balance-avail").textContent = "--";
    document.getElementById("balance-equity").textContent = "--";
    document.getElementById("balance-upnl").textContent = "--";
    return;
  }
  try {
    const data = await api(withExchange("/api/balance"));
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
  const sizingMode = document.getElementById("position-sizing-mode").value;
  const payload = {
    exchange: activeExchange,
    symbol: getSymbol(),
    direction: currentDirection,
    grid_mode: document.getElementById("grid-mode").value,
    upper_price: parseFloat(document.getElementById("upper-price").value),
    lower_price: parseFloat(document.getElementById("lower-price").value),
    grid_count: parseInt(document.getElementById("grid-count").value, 10),
    total_investment: sizingMode === "fixed_grid_qty" ? 0 : parseFloat(document.getElementById("total-investment").value),
    leverage: parseInt(document.getElementById("leverage").value, 10),
    position_sizing_mode: sizingMode,
    grid_order_qty: parseOptionalNumber("grid-order-qty"),
    fee_rate: parsePercentRate("taker-fee-rate"),
    maker_fee_rate: parsePercentRate("maker-fee-rate"),
    taker_fee_rate: parsePercentRate("taker-fee-rate"),
    initial_order_type: document.getElementById("initial-order-type").value,
    initial_order_price: parseOptionalNumber("initial-order-price"),
    grid_order_post_only: document.getElementById("grid-order-post-only").checked,
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
  if (
    Number.isFinite(latestPrice)
    && !(payload.lower_price < latestPrice && latestPrice < payload.upper_price)
  ) {
    showToast(
      `当前价 ${fmtMarket(latestPrice)} 不在区间 ${payload.lower_price} - ${payload.upper_price} 内`,
      "error",
    );
    return;
  }
  if (!payload.grid_count || payload.grid_count < 2) {
    showToast("网格数量至少为 2", "error");
    return;
  }
  if (payload.position_sizing_mode === "fixed_grid_qty") {
    if (!payload.grid_order_qty || payload.grid_order_qty <= 0) {
      showToast("每格开仓数量必须大于 0", "error");
      return;
    }
  } else if (!payload.total_investment || payload.total_investment <= 0) {
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
    const result = await api(withExchange(`/api/grid/stop/${symbol}`), "POST");
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
    const currentStatus = findStatusForSymbol(getSymbol(), activeExchange) || { running: false, symbol: getSymbol(), exchange: activeExchange };
    renderRunningGrids(allGridStatuses);
    if (currentStatus.running) {
      await fetchExchangeOrderData(getSymbol());
    } else {
      exchangeOpenOrders = [];
      exchangeTrades = [];
    }
    renderStatus(currentStatus, summary);
    await fetchRiskSnapshot(getSymbol());
    if (currentStatus.running && currentStatus.grid_ready) {
      await fetchPositions();
    } else {
      clearPositions();
    }
  } catch (_) {
    // Keep UI quiet during polling.
  }
}

async function fetchExchangeOrderData(symbol) {
  if (!symbol) return;
  if (exchangeConfigs[activeExchange] && !exchangeConfigs[activeExchange].configured) return;
  try {
    const [openData, tradeData] = await Promise.all([
      api(withExchange(`/api/orders/open/${symbol}`)),
      api(withExchange(`/api/trades/${symbol}?limit=100`)),
    ]);
    exchangeOpenOrders = Array.isArray(openData.orders) ? openData.orders : [];
    exchangeTrades = Array.isArray(tradeData.trades) ? tradeData.trades : [];
  } catch (_) {
    exchangeOpenOrders = [];
    exchangeTrades = [];
  }
}

async function fetchRiskSnapshot(symbol) {
  if (!symbol) return;
  if (exchangeConfigs[activeExchange] && !exchangeConfigs[activeExchange].configured) {
    renderRiskSnapshot({ has_risk: false });
    return;
  }
  try {
    const risk = await api(withExchange(`/api/risk/${symbol}`));
    renderRiskSnapshot(risk);
  } catch (_) {
    renderRiskSnapshot({ has_risk: false });
  }
}

function renderRiskSnapshot(risk) {
  const card = document.getElementById("risk-card");
  const body = document.getElementById("risk-body");
  const cancelBtn = document.getElementById("btn-cancel-orphans");
  if (!card || !body || !cancelBtn) return;

  const hasRisk = Boolean(risk.has_risk);
  currentRiskSymbol = risk.symbol || getSymbol();
  card.classList.toggle("hidden", !hasRisk);
  if (!hasRisk) {
    body.innerHTML = "";
    cancelBtn.classList.add("hidden");
    return;
  }

  const orphanCount = Number(risk.orphan_order_count || 0);
  const positionText = (risk.positions || [])
    .map((position) => `${position.side === "Buy" ? "多仓" : "空仓"} ${position.size}，均价 ${fmtMarket(position.entry_price)}`)
    .join("；");
  const messages = [];
  const reduceProtection = risk.reduce_protection || {};
  if (reduceProtection.has_risk) {
    const missing = reduceProtection.missing_by_level || [];
    const missingText = missing
      .slice(0, 6)
      .map((item) => `L${item.level}@${item.price}: ${fmtQty(item.missing_qty)}`)
      .join("，");
    const reason = reduceProtection.ledger_ok === false
      ? `平仓保护账本异常：${reduceProtection.ledger_reason || "无法确认每格保护数量"}`
      : "平仓保护挂单价位不完整";
    messages.push(`<div>${reason}${missingText ? `，缺口 ${missingText}` : ""}。</div>`);
  }
  if (orphanCount > 0) {
    messages.push(`<div><strong>${currentRiskSymbol}</strong> 有 ${orphanCount} 个程序历史挂单未被当前网格托管。</div>`);
  }
  if (risk.unmanaged_position && positionText) {
    messages.push(`<div>当前还有未托管持仓：${positionText}。</div>`);
  }
  messages.push("<div>建议先撤销孤儿挂单，再决定是否手动保留或平掉持仓。</div>");
  body.innerHTML = messages.join("");
  cancelBtn.classList.toggle("hidden", orphanCount <= 0);
}

async function cancelOrphanOrders() {
  const symbol = currentRiskSymbol || getSymbol();
  if (!symbol) return;
  if (!window.confirm(`确认撤销 ${symbol} 的未托管网格挂单？这不会平掉现有持仓。`)) return;

  try {
    const result = await api(withExchange(`/api/risk/cancel-orphans/${symbol}`), "POST");
    showToast(`已撤销 ${result.cancelled.length} 个孤儿挂单`, "success");
    await pollGridStatus();
  } catch (error) {
    showToast(`撤销孤儿挂单失败：${error.message}`, "error");
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

  if (status.waiting_initial_order) {
    text.textContent = "等待限价开仓";
    liveTag.textContent = "等待开仓";
  } else if (status.waiting_trigger) {
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
    document.getElementById("filled-body").innerHTML = '<tr><td class="empty-state" colspan="8">暂无成交</td></tr>';
    return;
  }

  document.getElementById("st-symbol").textContent = status.symbol || "--";
  document.getElementById("st-direction").textContent = mapDirection(status.direction);
  document.getElementById("st-mode").textContent = status.grid_mode === "geometric" ? "等比" : "等差";
  document.getElementById("st-grid-profit-pct").textContent = status.grid_profit_pct ? `${Number(status.grid_profit_pct).toFixed(3)}%` : "--";
  document.getElementById("st-completed-pairs").textContent = String(status.completed_pairs ?? 0);

  const profit = Number(status.total_equity_profit ?? status.total_profit ?? 0);
  const realizedNet = Number(status.realized_net_profit ?? status.total_profit ?? 0);
  const unrealized = Number(status.unrealised_pnl ?? 0);
  const profitEl = document.getElementById("st-profit");
  profitEl.textContent = `${fmtNum(profit)} USDT`;
  profitEl.className = profit >= 0 ? "positive" : "negative";
  document.getElementById("st-gross-profit").textContent = `${fmtNum(status.gross_profit ?? 0)} USDT`;
  document.getElementById("st-fee").textContent = `${fmtNum(status.total_fee ?? 0)} USDT`;
  const realizedEl = document.getElementById("st-realized-net");
  realizedEl.textContent = `${fmtNum(realizedNet)} USDT`;
  realizedEl.className = realizedNet >= 0 ? "positive" : "negative";
  const unrealizedEl = document.getElementById("st-unrealized");
  unrealizedEl.textContent = `${fmtNum(unrealized)} USDT`;
  unrealizedEl.className = unrealized >= 0 ? "positive" : "negative";
  document.getElementById("st-volume").textContent = `${fmtNum(status.total_volume ?? 0)} USDT`;

  const initialSide = status.initial_side === "Buy" ? "买入" : status.initial_side === "Sell" ? "卖出" : "--";
  document.getElementById("st-initial").textContent = status.initial_qty ? `${initialSide} ${Number(status.initial_qty).toFixed(6)}` : (status.trigger_message || "--");
  const baseline = status.baseline_position || {};
  const baselineSide = baseline.side === "Buy" ? "多仓" : baseline.side === "Sell" ? "空仓" : "--";
  document.getElementById("st-baseline-position").textContent = Number(baseline.qty || 0) > 0
    ? `${baselineSide} ${formatOrderQty(baseline.qty)}`
    : "--";
  const gridNetQty = Number(status.grid_position_net_qty || 0);
  const gridSide = gridNetQty > 0 ? "多仓" : gridNetQty < 0 ? "空仓" : "--";
  document.getElementById("st-grid-position").textContent = Math.abs(gridNetQty) > 0
    ? `${gridSide} ${formatOrderQty(Math.abs(gridNetQty))}`
    : "--";

  const openOrders = exchangeOpenOrders.length ? exchangeOpenOrders : (status.active_orders || []);
  renderOrders(openOrders);
  renderPendingOrders(openOrders, status);
  renderFilled(exchangeTrades.length ? exchangeTrades : (status.filled_orders || []));
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
    const profit = Number(status.total_equity_profit ?? status.total_profit ?? 0);
    const completedPairs = Number(status.completed_pairs || 0);
    return `
      <button class="grid-summary-item${status.symbol === getSymbol() && status.exchange === activeExchange ? " active" : ""}" type="button" onclick="selectGridSymbol('${status.exchange || activeExchange}', '${status.symbol}')">
        <span>
          <strong>${status.symbol}</strong>
          <small>${exchangeDisplayName(status.exchange)} · ${mapDirection(status.direction)} · ${status.grid_mode === "geometric" ? "等比" : "等差"}</small>
        </span>
        <span>
          <strong class="${profit >= 0 ? "positive" : "negative"}">${fmtNum(profit)}</strong>
          <small>完成 ${completedPairs} 次</small>
        </span>
      </button>
    `;
  }).join("");
}

async function selectGridSymbol(exchange, symbol) {
  activeExchange = exchange || activeExchange;
  syncExchangeInputs();
  document.getElementById("symbol-input").value = symbol;
  showToast(`已切换到 ${exchangeDisplayName(activeExchange)} · ${symbol}`, "success");
  await fetchPrice();
  await fetchBalance();
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
  qtyEl.textContent = formatOrderQty(orders[0]?.qty || status.config?.qty_per_grid);
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
        <div class="muted">${order.reduce_only ? "止盈/平仓" : "开仓/补仓"} · ${order.status || "open"}</div>
      </div>
      <div class="order-price">${fmtMarket(order.price)}</div>
      <div class="muted">${formatOrderQty(order.qty)}</div>
    </div>
  `).join("");
}

function renderFilled(filledOrders) {
  const tbody = document.getElementById("filled-body");
  if (!filledOrders.length) {
    tbody.innerHTML = '<tr><td class="empty-state" colspan="8">暂无成交</td></tr>';
    return;
  }

  tbody.innerHTML = [...filledOrders]
    .reverse()
    .slice(0, 30)
    .map((item) => {
      const profit = Number(item.realized_pnl ?? item.profit ?? 0);
      const fee = Number(item.fee_usdt || item.fee || 0);
      const volume = Number(item.volume || 0);
      const feeAsset = item.fee_asset ? ` (${item.fee_asset})` : "";
      const liquidity = item.is_maker === true ? "挂单" : item.is_maker === false ? "吃单" : mapLiquidity(item.liquidity);
      const timeValue = formatTradeTime(item.time);
      return `
        <tr>
          <td class="${item.side === "Buy" ? "side-buy" : "side-sell"}">${item.side}</td>
          <td>${fmtMarket(item.price)}</td>
          <td>${formatOrderQty(item.qty)}</td>
          <td>${fmtPrecise(volume, 8)}</td>
          <td>${fmtPrecise(fee, 10)}${feeAsset}</td>
          <td>${liquidity}</td>
          <td class="${profit >= 0 ? "positive" : "negative"}">${profit >= 0 ? "+" : ""}${fmtPrecise(profit, 8)}</td>
          <td>${timeValue}</td>
        </tr>
      `;
    })
    .join("");
}

function openDetailModal(panel) {
  activeDetailPanel = panel;
  const titles = {
    positions: ["仓位监控", "当前持仓"],
    orders: ["挂单追踪", "网格挂单"],
    fills: ["成交记录", "最近成交"],
    history: ["策略复盘", "开仓历史"],
  };
  const [eyebrow, title] = titles[panel] || titles.positions;

  document.getElementById("detail-modal-eyebrow").textContent = eyebrow;
  document.getElementById("detail-modal-title").textContent = title;
  ["positions", "orders", "fills", "history"].forEach((name) => {
    document.getElementById(`detail-${name}-panel`).classList.toggle("hidden", name !== panel);
  });
  document.getElementById("detail-modal").classList.remove("hidden");
  if (panel === "history") {
    fetchGridHistory();
  }
}

function closeDetailModal() {
  document.getElementById("detail-modal").classList.add("hidden");
}

function closeDetailModalOutside(event) {
  if (event.target.id === "detail-modal") {
    closeDetailModal();
  }
}

async function fetchGridHistory() {
  try {
    const data = await api("/api/grid/history?limit=100");
    renderGridHistory(data.runs || []);
  } catch (error) {
    document.getElementById("history-body").innerHTML = `<tr><td class="empty-state" colspan="10">读取历史失败：${error.message}</td></tr>`;
  }
}

function renderGridHistory(runs) {
  const tbody = document.getElementById("history-body");
  if (!runs.length) {
    tbody.innerHTML = '<tr><td class="empty-state" colspan="10">暂无历史</td></tr>';
    return;
  }

  tbody.innerHTML = runs.map((run) => {
    const profit = Number(run.net_profit || 0);
    const initialType = run.initial_order_type === "post_only"
      ? "Post Only"
      : run.initial_order_type === "limit"
        ? "限价"
        : "市价";
    const sizingText = run.position_sizing_mode === "fixed_grid_qty"
      ? `每格 ${formatOrderQty(run.grid_order_qty || 0)}`
      : `投入 ${fmtNum(run.total_investment || 0)}U`;
    const limitText = run.initial_order_price ? ` @ ${fmtMarket(run.initial_order_price)}` : "";
    return `
      <tr>
        <td>${formatTime(run.started_at)}</td>
        <td>${run.symbol || "--"}</td>
        <td>${mapDirection(run.direction)}</td>
        <td>${run.grid_mode === "geometric" ? "等比" : "等差"}</td>
        <td>${initialType}${limitText}<br><span class="muted">${sizingText}</span></td>
        <td>${mapRunStatus(run.status)}</td>
        <td class="${profit >= 0 ? "positive" : "negative"}">${fmtNum(profit)}</td>
        <td>${fmtNum(run.total_fee || 0)}</td>
        <td>${fmtNum(run.total_volume || 0)}</td>
        <td>${run.completed_pairs ?? 0}</td>
      </tr>
    `;
  }).join("");
}

async function fetchPositions() {
  const symbol = getSymbol();
  if (exchangeConfigs[activeExchange] && !exchangeConfigs[activeExchange].configured) {
    clearPositions();
    return;
  }
  try {
    const data = await api(withExchange(`/api/positions/${symbol}`));
    const el = document.getElementById("positions-body");
    if (!data.positions.length) {
      el.innerHTML = '<div class="empty-state">暂无持仓</div>';
      return;
    }

    el.innerHTML = data.positions.map((position) => {
      const pnl = Number(position.unrealised_pnl || 0);
      const direction = position.side === "Buy" ? "多仓" : "空仓";
      const sideClass = position.side === "Buy" ? "side-buy" : "side-sell";
      const leverage = position.leverage ? `${position.leverage}x` : "--";
      return `
        <div class="position-card">
          <div class="position-head">
            <strong class="${sideClass}">${direction}</strong>
            <span class="muted">${leverage}</span>
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
  const exchange = document.getElementById("cfg-exchange").value;
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
    const result = await api("/api/config", "POST", { exchange, api_key: apiKey, api_secret: apiSecret, testnet });
    errorEl.classList.add("hidden");
    document.getElementById("cfg-api-key").value = "";
    document.getElementById("cfg-api-secret").value = "";
    activeExchange = exchange;
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

function parsePercentRate(id) {
  const value = Number(document.getElementById(id).value);
  return Number.isFinite(value) && value > 0 ? value / 100 : 0;
}

function mapDirection(direction) {
  if (direction === "long") return "做多";
  if (direction === "short") return "做空";
  if (direction === "neutral") return "中性";
  return "--";
}

function mapLiquidity(liquidity) {
  if (liquidity === "maker") return "挂单";
  if (liquidity === "taker") return "吃单";
  if (liquidity === "mixed") return "混合";
  return "--";
}

function mapRunStatus(status) {
  if (status === "running") return "运行中";
  if (status === "stopped") return "已停止";
  if (status === "closed") return "已关闭";
  if (status === "saved") return "已保存";
  return status || "--";
}

function getSymbol() {
  return document.getElementById("symbol-input").value.trim().toUpperCase();
}

function findStatusForSymbol(symbol, exchange = activeExchange) {
  return allGridStatuses.find((status) =>
    String(status.symbol).toUpperCase() === symbol
    && String(status.exchange || activeExchange) === String(exchange || activeExchange)
  );
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
  return fmtPrecise(value, 10);
}

function formatOrderQty(value) {
  return fmtPrecise(value, 8);
}

function fmtPrecise(value, maxDecimals = 8) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: maxDecimals,
  });
}

function formatTradeTime(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return "--";
  const ms = num > 1e12 ? num : num * 1000;
  return new Date(ms).toLocaleString();
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

function formatTime(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return "--";
  return new Date(num * 1000).toLocaleString();
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
