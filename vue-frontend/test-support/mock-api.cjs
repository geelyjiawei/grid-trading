const http = require("node:http");

function send(response, body, status = 200) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
}

http
  .createServer((request, response) => {
    const url = new URL(request.url, "http://127.0.0.1");
    if (url.pathname === "/api/auth/status") {
      return send(response, { required: false, configured: true, authenticated: true });
    }
    if (url.pathname === "/api/config") {
      return send(response, {
        configured: true,
        active_exchange: "aster",
        configs: {
          aster: {
            exchange: "aster",
            configured: true,
            api_key: "0x12...89",
            testnet: false,
            source: "file",
          },
          binance: {
            exchange: "binance",
            configured: true,
            api_key: "BIN...123",
            testnet: false,
            source: "file",
          },
          bybit: { exchange: "bybit", configured: false },
        },
      });
    }
    if (url.pathname === "/api/grid/status") {
      return send(response, {
        running: true,
        count: 2,
        running_count: 2,
        grids: [
          {
            exchange: "aster",
            symbol: "ANSEMUSDT",
            running: true,
            direction: "long",
            grid_mode: "arithmetic",
            total_equity_profit: 12.9112,
            total_profit: 11.204,
            total_fee: 1.7072,
            realized_net_profit: 10.9821,
            unrealised_pnl: 1.9291,
            total_volume: 28342.11,
            grid_position_net_qty: 3000,
            completed_pairs: 1408,
            current_price: 0.381,
          },
          {
            exchange: "binance",
            symbol: "MUUSDT",
            running: true,
            direction: "short",
            grid_mode: "arithmetic",
            total_equity_profit: -0.4182,
            total_profit: 1.12,
            total_fee: 0.38,
            realized_net_profit: 0.74,
            unrealised_pnl: -1.1582,
            total_volume: 8120.5,
            grid_position_net_qty: -1.4,
            completed_pairs: 83,
            current_price: 1012.4,
          },
        ],
      });
    }
    if (url.pathname.startsWith("/api/price/")) {
      return send(response, {
        last_price: "0.3810000",
        mark_price: "0.3809200",
        price_24h_pcnt: "0.0271",
        volume_24h: "4938192.2",
      });
    }
    if (url.pathname === "/api/balance") {
      return send(response, {
        available_balance: "1842.5521",
        equity: "2018.4409",
        unrealised_pnl: "1.9291",
      });
    }
    if (url.pathname.startsWith("/api/fees/")) {
      return send(response, {
        maker_fee_rate: 0.0002,
        taker_fee_rate: 0.0005,
        source: "exchange",
      });
    }
    if (url.pathname.startsWith("/api/risk/")) {
      return send(response, {
        has_risk: false,
        unmanaged_position: false,
        unmanaged_delta_qty: 0,
      });
    }
    return send(response, { detail: "mock endpoint not found" }, 404);
  })
  .listen(8000, "127.0.0.1", () => {
    console.log("Vue visual QA mock API: http://127.0.0.1:8000");
  });
