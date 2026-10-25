// Synthetic, read-only UI fixtures. Never connects to an exchange or accepts writes.
const http = require("node:http");
const exchange = "trade_xyz";
const symbol = "PONSUSDC";
const run = {
  exchange,
  symbol,
  run_id: "ui-demo-only",
  running: true,
  lifecycle: "active",
  direction: "short",
  grid_mode: "arithmetic",
  lower_price: "0.7",
  upper_price: "0.8",
  grid_count: 35,
  active_grid_count: 19,
  participating_level_count: 35,
  leverage: 3,
  position_sizing_mode: "fixed_grid_qty",
  grid_order_qty: "100",
  initial_order_type: "limit",
  initial_order_price: "0.755",
  planned_total_qty: "1900",
  opening_filled_qty: "1900",
  baseline_position: { signed_qty: "0" },
  grid_position_net_qty: "-1700",
  expected_position_net_qty: "-1700",
  realized_net_profit: "86.4271",
  total_fee: "11.3902",
  total_volume: "81147.83",
  completed_pairs: 412,
};
const orders = Array.from({ length: 37 }, (_, i) => ({
  order_id: `demo-order-${i}`,
  order_link_id: `demo_grid_${i}_read_only`,
  side: i < 11 ? "Buy" : "Sell",
  price: (0.7 + i * 0.00285).toFixed(5),
  qty: i === 8 ? "70" : "100",
  original_qty: "100",
  remaining_qty: i === 8 ? "70" : "100",
  executed_qty: i === 8 ? "30" : "0",
  status: i === 8 ? "PARTIALLY_FILLED" : "NEW",
  reduce_only: i < 11,
  created_time: 1788830100000 + i * 4000,
})).reverse();
const trades = Array.from({ length: 27 }, (_, i) => ({
  order_id: `demo-fill-order-${i}`,
  trade_id: `demo-trade-${i}`,
  side: i % 2 ? "Buy" : "Sell",
  price: i % 2 ? "0.72565" : "0.72850",
  qty: i === 2 ? "13" : "100",
  volume: ((i % 2 ? 0.72565 : 0.7285) * (i === 2 ? 13 : 100)).toFixed(5),
  is_maker: i !== 2 && i !== 9,
  fee_usdt: "0.01049",
  fee_quote_asset: "USDC",
  realized_pnl: i % 2 ? "0.285" : "0",
  time: 1788830000000 - i * 35000,
}));
const history = Array.from({ length: 12 }, (_, i) => ({
  exchange,
  symbol,
  run_id: `demo-history-${i}`,
  status: "stopped",
  started_at: 1788800000000 - i * 3600000,
  direction: i % 2 ? "long" : "short",
  grid_mode: "arithmetic",
  grid_count: 35,
  net_profit: i % 3 ? "14.9281" : "-2.4175",
  total_fee: "0.6932",
  total_volume: "7241.65",
  completed_pairs: 32,
  grid_order_qty: "100",
  initial_order_price: "0.755",
}));
http
  .createServer((request, response) => {
    const url = new URL(request.url, "http://127.0.0.1");
    const send = (value, status = 200) => {
      response.writeHead(status, { "Content-Type": "application/json" });
      response.end(JSON.stringify(value));
    };
    if (request.method !== "GET")
      return send({ detail: "UI demo is read-only; trading is disabled" }, 405);
    if (url.pathname === "/api/auth/status")
      return send({ required: false, configured: true, authenticated: true });
    if (url.pathname === "/api/config")
      return send({
        configured: true,
        active_exchange: exchange,
        configs: {
          trade_xyz: { exchange, configured: true, source: "UI fixture" },
        },
      });
    if (url.pathname === "/api/grid/status")
      return send({
        running: true,
        count: 1,
        trading_enabled: false,
        grids: [run],
      });
    if (url.pathname.startsWith("/api/price/"))
      return send({
        last_price: "0.72915",
        mark_price: "0.72912",
        price_24h_pcnt: "0.0264",
        volume_24h: "2847312.4",
      });
    if (url.pathname === "/api/balance")
      return send({
        available: "2230.9933",
        equity: "2973.6434",
        unit: "USDC",
        unrealised_pnl: "-1.7842",
      });
    if (url.pathname.startsWith("/api/fees/"))
      return send({ maker_fee_rate: "0.000144", taker_fee_rate: "0.000432" });
    if (url.pathname.startsWith("/api/risk/"))
      return send({
        ...run,
        has_risk: false,
        actual_position_net_qty: "-1700",
        unmanaged_delta_qty: "0",
        profit_scope: "strategy_owned_inventory",
        gross_profit: "97.8173",
        trading_net_equity_profit: "84.6429",
        funding_status: "verified",
        funding_net_income: "-1.25",
        funding_payment_count: 1,
        funding_observed_through_ms: Date.now(),
        total_equity_profit: "83.3929",
        grid_unrealised_pnl: "-1.7842",
      });
    if (url.pathname.startsWith("/api/positions/"))
      return send({
        positions: [
          {
            side: "Sell",
            size: "1700",
            entry_price: "0.72807",
            mark_price: "0.72912",
            unrealised_pnl: "-1.7842",
            leverage: 3,
            liq_price: "1.05083",
          },
        ],
      });
    if (url.pathname.startsWith("/api/orders/open/")) return send({ orders });
    if (url.pathname.startsWith("/api/trades/")) return send({ trades });
    if (url.pathname === "/api/grid/history") return send({ runs: history });
    return send({ detail: "UI fixture route not found" }, 404);
  })
  .listen(8000, "127.0.0.1", () =>
    console.log(
      "Synthetic bot detail preview API on 127.0.0.1:8000. No trading or external API.",
    ),
  );
