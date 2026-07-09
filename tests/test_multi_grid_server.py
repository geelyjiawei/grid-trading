import os
import sys
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
if str(ROOT_DIR / "tests") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "tests"))

import main  # noqa: E402
import pyotp  # noqa: E402
from aster_client import AsterFuturesClient  # noqa: E402
from binance_client import BinanceFuturesClient  # noqa: E402
from bybit_client import BybitClient  # noqa: E402
from auth import hash_password  # noqa: E402
from test_grid_engine import FakeClient  # noqa: E402


class FakeConfigClient:
    def __init__(self, api_key, api_secret, testnet=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

    def get_balance(self):
        return {"retCode": 0, "result": {"list": [{"coin": []}]}}


class FakeBinanceConfigClient(FakeConfigClient):
    pass


class FakeBybitConfigClient(FakeConfigClient):
    pass


class FakeAsterConfigClient(FakeConfigClient):
    pass


class MultiGridServerTests(unittest.TestCase):
    def setUp(self):
        self._original_state_file = main.GRID_STATE_FILE
        self._original_history_file = main.GRID_HISTORY_FILE
        self._original_api_config_file = main.API_CONFIG_FILE
        self._original_api_configs = main._api_configs
        self._original_clients = main._clients
        self._original_active_exchange = main._active_exchange
        self._original_api_config = main._api_config
        self._original_client = main._client
        self._state_tmp = tempfile.TemporaryDirectory()
        main.GRID_STATE_FILE = str(Path(self._state_tmp.name) / "grid_state.json")
        main.GRID_HISTORY_FILE = str(Path(self._state_tmp.name) / "grid_history.json")
        main.API_CONFIG_FILE = str(Path(self._state_tmp.name) / "api_config.json")
        main._engines.clear()
        fake_client = FakeClient("100")
        main._api_configs = {
            "binance": {
                "exchange": "binance",
                "api_key": "test-api-key",
                "api_secret": "test-api-secret",
                "testnet": False,
                "source": "test",
            }
        }
        main._clients = {"binance": fake_client}
        main._active_exchange = "binance"
        main._api_config = main._api_configs["binance"]
        main._client = fake_client
        self.client = TestClient(main.app)

    def tearDown(self):
        main._engines.clear()
        main.GRID_STATE_FILE = self._original_state_file
        main.GRID_HISTORY_FILE = self._original_history_file
        main.API_CONFIG_FILE = self._original_api_config_file
        main._api_configs = self._original_api_configs
        main._clients = self._original_clients
        main._active_exchange = self._original_active_exchange
        main._api_config = self._original_api_config
        main._client = self._original_client
        self._state_tmp.cleanup()
        for key in (
            "AUTH_REQUIRED",
            "ADMIN_USERNAME",
            "ADMIN_PASSWORD_HASH",
            "TOTP_SECRET",
            "SESSION_SECRET",
            "AUTH_SHOW_TOTP_SETUP",
        ):
            os.environ.pop(key, None)

    def test_grid_status_uses_stable_snapshot_when_registry_changes_during_render(self):
        first = object()
        second = object()
        first_key = main._engine_key("binance", "FIRSTUSDT")
        second_key = main._engine_key("aster", "SECONDUSDT")
        main._engines[first_key] = first
        main._engines[second_key] = second
        original_engine_status = main._engine_status

        def changing_status(engine):
            if engine is first:
                with main._engines_lock:
                    main._engines.pop(second_key, None)
            return {"running": True}

        main._engine_status = changing_status
        try:
            status = main.grid_status()
        finally:
            main._engine_status = original_engine_status

        self.assertEqual(status["engine_count"], 2)
        self.assertEqual(status["running_count"], 2)

    def _payload(self, symbol):
        return {
            "symbol": symbol,
            "direction": "long",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 4,
            "total_investment": 100,
            "leverage": 2,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

    def test_multiple_symbols_can_run_and_stop_independently(self):
        btc_response = self.client.post("/api/grid/start", json=self._payload("BTCUSDT"))
        eth_response = self.client.post("/api/grid/start", json=self._payload("ETHUSDT"))

        self.assertEqual(btc_response.status_code, 200)
        self.assertEqual(eth_response.status_code, 200)

        status = self.client.get("/api/grid/status").json()
        self.assertTrue(status["running"])
        self.assertEqual(status["engine_count"], 2)
        self.assertEqual(status["running_count"], 2)

        stop_response = self.client.post("/api/grid/stop/BTCUSDT")
        self.assertEqual(stop_response.status_code, 200)

        status = self.client.get("/api/grid/status").json()
        by_symbol = {item["symbol"]: item for item in status["engines"]}
        self.assertNotIn("BTCUSDT", by_symbol)
        self.assertTrue(by_symbol["ETHUSDT"]["running"])

    def test_same_symbol_can_run_on_different_exchanges_independently(self):
        old_configs = main._api_configs
        old_clients = main._clients
        old_active = main._active_exchange
        old_api_config = main._api_config
        old_client = main._client
        try:
            binance_client = FakeClient("100")
            aster_client = FakeClient("100")
            main._api_configs = {
                "binance": {
                    "exchange": "binance",
                    "api_key": "binance-key",
                    "api_secret": "binance-secret",
                    "testnet": False,
                    "source": "file",
                },
                "aster": {
                    "exchange": "aster",
                    "api_key": "0x0000000000000000000000000000000000000abc",
                    "api_secret": "0x" + "1" * 64,
                    "testnet": False,
                    "source": "file",
                },
            }
            main._clients = {"binance": binance_client, "aster": aster_client}
            main._active_exchange = "binance"
            main._api_config = main._api_configs["binance"]
            main._client = binance_client

            binance_payload = self._payload("MUUSDT")
            binance_payload["exchange"] = "binance"
            aster_payload = self._payload("MUUSDT")
            aster_payload["exchange"] = "aster"
            aster_payload["direction"] = "short"

            binance_response = self.client.post("/api/grid/start", json=binance_payload)
            aster_response = self.client.post("/api/grid/start", json=aster_payload)

            self.assertEqual(binance_response.status_code, 200)
            self.assertEqual(aster_response.status_code, 200)
            self.assertIn(main._engine_key("binance", "MUUSDT"), main._engines)
            self.assertIn(main._engine_key("aster", "MUUSDT"), main._engines)

            status = self.client.get("/api/grid/status").json()
            running = {(item["exchange"], item["symbol"]) for item in status["engines"] if item["running"]}
            self.assertEqual(status["running_count"], 2)
            self.assertIn(("binance", "MUUSDT"), running)
            self.assertIn(("aster", "MUUSDT"), running)

            stop_binance = self.client.post("/api/grid/stop/MUUSDT?exchange=binance")
            self.assertEqual(stop_binance.status_code, 200)
            status = self.client.get("/api/grid/status").json()
            running = {(item["exchange"], item["symbol"]) for item in status["engines"] if item["running"]}
            self.assertEqual(status["running_count"], 1)
            self.assertNotIn(("binance", "MUUSDT"), running)
            self.assertIn(("aster", "MUUSDT"), running)
        finally:
            for engine in list(main._engines.values()):
                engine.running = False
            main._engines.clear()
            main._api_configs = old_configs
            main._clients = old_clients
            main._active_exchange = old_active
            main._api_config = old_api_config
            main._client = old_client

    def test_same_symbol_cannot_start_twice(self):
        first_response = self.client.post("/api/grid/start", json=self._payload("BTCUSDT"))
        second_response = self.client.post("/api/grid/start", json=self._payload("BTCUSDT"))

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 400)

    def test_grid_history_records_running_and_stopped_strategy_summary(self):
        start = self.client.post("/api/grid/start", json=self._payload("BILLUSDT"))
        running_history = self.client.get("/api/grid/history").json()

        self.assertEqual(start.status_code, 200)
        self.assertEqual(len(running_history["runs"]), 1)
        self.assertEqual(running_history["runs"][0]["symbol"], "BILLUSDT")
        self.assertEqual(running_history["runs"][0]["direction"], "long")
        self.assertEqual(running_history["runs"][0]["status"], "running")
        self.assertIn("net_profit", running_history["runs"][0])
        self.assertIn("total_fee", running_history["runs"][0])
        self.assertIn("total_volume", running_history["runs"][0])

        stop = self.client.post("/api/grid/stop/BILLUSDT")
        stopped_history = self.client.get("/api/grid/history").json()

        self.assertEqual(stop.status_code, 200)
        self.assertEqual(stopped_history["runs"][0]["status"], "stopped")
        self.assertIsNotNone(stopped_history["runs"][0]["stopped_at"])

    def test_grid_status_reports_total_profit_with_unrealised_pnl(self):
        main._client.positions = [{"side": "Buy", "size": "1", "unrealisedPnl": "2.5"}]

        start = self.client.post("/api/grid/start", json=self._payload("BILLUSDT"))
        status = self.client.get("/api/grid/status/BILLUSDT").json()

        self.assertEqual(start.status_code, 200)
        self.assertIn("realized_net_profit", status)
        self.assertEqual(status["account_unrealised_pnl"], 2.5)
        self.assertEqual(status["unrealised_pnl"], 0.0)
        self.assertAlmostEqual(status["total_equity_profit"], status["realized_net_profit"])

    def test_grid_preview_uses_active_grid_count_and_exchange_qty_step(self):
        main._client = FakeClient("100")
        payload = {
            "symbol": "BILLUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 103,
            "lower_price": 99,
            "grid_count": 40,
            "total_investment": 100,
            "leverage": 10,
            "fee_rate": 0.0005,
            "maker_fee_rate": 0.0002,
            "taker_fee_rate": 0.0005,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["active_grid_count"], 10)
        self.assertEqual(data["grid_count"], 40)
        self.assertAlmostEqual(data["total_qty"], 10.0)
        self.assertAlmostEqual(data["qty_per_grid_min"], 1.0)
        self.assertAlmostEqual(data["qty_per_grid_max"], 1.0)
        self.assertAlmostEqual(data["per_grid_open_fee"], 0.05)
        self.assertAlmostEqual(data["per_grid_close_fee"], 0.02)
        self.assertAlmostEqual(data["per_grid_fee"], 0.07)

    def test_grid_preview_uses_post_only_initial_price_as_reference(self):
        main._client = FakeClient("1010", tick_size="0.1", qty_step="0.01", min_qty="0.01")
        payload = {
            "symbol": "MUUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 1020,
            "lower_price": 1000,
            "grid_count": 20,
            "total_investment": 500,
            "leverage": 5,
            "fee_rate": 0.0005,
            "maker_fee_rate": 0.0002,
            "taker_fee_rate": 0.0005,
            "initial_order_type": "post_only",
            "initial_order_price": 1011,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["current_price"], 1010)
        self.assertEqual(data["reference_price"], 1011)
        self.assertEqual(data["active_grid_count"], 11)
        self.assertAlmostEqual(data["total_qty"], 2.47)
        self.assertAlmostEqual(data["qty_per_grid_min"], 0.22)
        self.assertAlmostEqual(data["qty_per_grid_max"], 0.23)

    def test_grid_preview_regular_limit_honors_marketable_user_price(self):
        main._client = FakeClient("1012", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        payload = {
            "symbol": "MUUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 1020,
            "lower_price": 1000,
            "grid_count": 20,
            "total_investment": 0,
            "leverage": 5,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 0.2,
            "fee_rate": 0.0005,
            "maker_fee_rate": 0.0002,
            "taker_fee_rate": 0.0005,
            "initial_order_type": "limit",
            "initial_order_price": 1008,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["reference_price"], 1008)
        self.assertEqual(data["active_grid_count"], 8)
        self.assertAlmostEqual(data["total_qty"], 1.6)
        self.assertAlmostEqual(data["qty_per_grid_min"], 0.2)
        self.assertAlmostEqual(data["qty_per_grid_max"], 0.2)

    def test_grid_preview_post_only_crossing_price_uses_maker_safe_reference(self):
        main._client = FakeClient("1014", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        payload = {
            "symbol": "MUUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 1020,
            "lower_price": 1000,
            "grid_count": 20,
            "total_investment": 0,
            "leverage": 5,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 0.2,
            "fee_rate": 0.0005,
            "maker_fee_rate": 0.0002,
            "taker_fee_rate": 0.0005,
            "initial_order_type": "post_only",
            "initial_order_price": 1012,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertAlmostEqual(data["reference_price"], 1014.1)
        self.assertEqual(data["active_grid_count"], 15)
        self.assertAlmostEqual(data["total_qty"], 3.0)

    def test_start_limit_open_allows_current_outside_range_when_limit_is_inside(self):
        main._client = FakeClient("990", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        payload = {
            "symbol": "MUUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 1020,
            "lower_price": 1000,
            "grid_count": 20,
            "total_investment": 0,
            "leverage": 5,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 0.2,
            "fee_rate": 0.0005,
            "maker_fee_rate": 0.0002,
            "taker_fee_rate": 0.0005,
            "initial_order_type": "limit",
            "initial_order_price": 1014,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

        response = self.client.post("/api/grid/start", json=payload)
        opening_order = next(
            order for order in main._client.orders if order.get("order_link_id", "").startswith("open_")
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(opening_order["price"], "1014.0")
        self.assertEqual(opening_order["qty"], "2.8")

    def test_grid_preview_supports_fixed_grid_order_qty(self):
        main._client = FakeClient("100", tick_size="0.1", qty_step="0.01", min_qty="0.01")
        payload = {
            "symbol": "MUUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 103,
            "lower_price": 99,
            "grid_count": 40,
            "total_investment": 0,
            "leverage": 5,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 4,
            "fee_rate": 0.0005,
            "maker_fee_rate": 0.0002,
            "taker_fee_rate": 0.0005,
            "initial_order_type": "limit",
            "initial_order_price": 101,
            "trigger_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
        }

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["position_sizing_mode"], "fixed_grid_qty")
        self.assertEqual(data["active_grid_count"], 20)
        self.assertAlmostEqual(data["total_qty"], 80.0)
        self.assertAlmostEqual(data["qty_per_grid_min"], 4.0)
        self.assertAlmostEqual(data["qty_per_grid_max"], 4.0)

    def test_grid_history_includes_opening_details(self):
        payload = self._payload("BILLUSDT")
        payload.update(
            {
                "initial_order_type": "limit",
                "initial_order_price": 101,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "total_investment": 0,
            }
        )

        start = self.client.post("/api/grid/start", json=payload)
        history = self.client.get("/api/grid/history").json()

        self.assertEqual(start.status_code, 200)
        self.assertEqual(history["runs"][0]["initial_order_type"], "limit")
        self.assertEqual(history["runs"][0]["initial_order_price"], 101)
        self.assertEqual(history["runs"][0]["position_sizing_mode"], "fixed_grid_qty")
        self.assertEqual(history["runs"][0]["grid_order_qty"], 1)

    def test_restore_keeps_reduce_orders_without_event_loop_restart(self):
        main._client = FakeClient("15.95")
        main._client.positions = [{"side": "Buy", "size": "200", "avgPrice": "16.18"}]
        placed = main._client.place_order(
            symbol="NOKUSDT",
            side="Sell",
            qty="3.1",
            price="15.9",
            order_type="Limit",
            reduce_only=True,
            order_link_id="g_14_S_reduce",
        )
        order_id = placed["result"]["orderId"]
        state = {
            "version": 1,
            "exchange": main._normalize_exchange(main._api_config.get("exchange")),
            "testnet": bool(main._api_config.get("testnet", False)),
            "grids": {
                "NOKUSDT": {
                    "config": {
                        "symbol": "NOKUSDT",
                        "direction": "long",
                        "grid_mode": "arithmetic",
                        "upper_price": 16.3,
                        "lower_price": 15.6,
                        "grid_count": 30,
                        "total_investment": 40,
                        "leverage": 20,
                    },
                    "running": True,
                    "grid_ready": True,
                    "grid_levels": [15.6, 15.9, 16.3],
                    "active_orders": {
                        "g_14_S_reduce": {
                            "link_id": "g_14_S_reduce",
                            "order_id": order_id,
                            "level_idx": 14,
                            "side": "Sell",
                            "price": "15.9",
                            "qty": "3.1",
                            "status": "open",
                            "order_type": "Limit",
                            "time_in_force": "GTC",
                            "reduce_only": True,
                            "entry_price": 15.93,
                        }
                    },
                    "baseline_position_side": "Buy",
                    "baseline_position_qty": 200.0,
                    "baseline_position_entry_price": 16.18,
                    "grid_position_net_qty": 0.0,
                    "tick_size": "0.1",
                    "qty_step": "0.1",
                    "min_qty": 0.1,
                }
            },
        }
        main._write_grid_state_file(state)

        main._restore_saved_engines()

        state_key = main._engine_key(state["exchange"], "NOKUSDT")
        engine = main._engines[state_key]
        saved_grids = main._load_grid_state_file()["grids"]
        saved = saved_grids[state_key]
        self.assertFalse(engine.running)
        self.assertTrue(engine.grid_ready)
        self.assertFalse(saved["running"])
        self.assertNotIn("NOKUSDT", saved_grids)
        open_orders = main._client.get_open_orders("NOKUSDT")["result"]["list"]
        self.assertEqual(len(open_orders), 1)
        self.assertEqual(open_orders[0]["orderId"], order_id)
        self.assertEqual(open_orders[0]["qty"], "3.1")

    def test_risk_endpoint_detects_and_cancels_orphan_grid_orders(self):
        main._client.place_order(
            symbol="BILLUSDT",
            side="Sell",
            qty="625",
            price="0.17243",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_1_S_orphan",
        )
        main._client.positions = [{"side": "Sell", "size": "1250", "avgPrice": "0.1727"}]

        snapshot = self.client.get("/api/risk/BILLUSDT")

        self.assertEqual(snapshot.status_code, 200)
        self.assertTrue(snapshot.json()["has_risk"])
        self.assertEqual(snapshot.json()["orphan_order_count"], 1)
        self.assertTrue(snapshot.json()["unmanaged_position"])

        cancelled = self.client.post("/api/risk/cancel-orphans/BILLUSDT")
        after = self.client.get("/api/risk/BILLUSDT")

        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(len(cancelled.json()["cancelled"]), 1)
        self.assertEqual(after.json()["orphan_order_count"], 0)

    def test_risk_endpoint_flags_per_level_grid_coverage_mismatch(self):
        main._client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        main._client.positions = [{"side": "Sell", "size": "0.3", "avgPrice": "100"}]
        exchange = main._active_exchange
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "MUUSDT",
                "exchange": exchange,
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.running = True
        engine.grid_ready = True
        engine.grid_levels = [90, 100, 110]
        engine.target_qty_by_level = {"0": 0.2, "1": 0.2}
        engine.grid_position_net_qty = -0.3
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 0.1, "entry_value": 10.0},
            "1": {"qty": 0.2, "entry_value": 20.0},
        }
        engine.active_orders = {
            "g_0_B_reduce": {
                "link_id": "g_0_B_reduce",
                "order_id": "1",
                "level_idx": 0,
                "side": "Buy",
                "price": "90",
                "qty": "0.1",
                "status": "NEW",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 100,
                "processed_fill_qty": 0.0,
            },
            "g_1_B_reduce": {
                "link_id": "g_1_B_reduce",
                "order_id": "2",
                "level_idx": 1,
                "side": "Buy",
                "price": "100",
                "qty": "0.2",
                "status": "NEW",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 110,
                "processed_fill_qty": 0.0,
            },
        }
        main._engines[main._engine_key(exchange, "MUUSDT")] = engine

        response = self.client.get(f"/api/risk/MUUSDT?exchange={exchange}")

        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertTrue(snapshot["has_risk"])
        self.assertFalse(snapshot["unmanaged_position"])
        self.assertEqual(snapshot["orphan_order_count"], 0)
        self.assertTrue(snapshot["grid_coverage"]["has_risk"])
        self.assertEqual(snapshot["grid_coverage"]["missing_by_level"][0]["level"], 0)
        self.assertAlmostEqual(
            snapshot["grid_coverage"]["missing_by_level"][0]["missing_qty"],
            0.1,
        )

    def test_api_config_change_is_blocked_while_grid_is_running(self):
        original_binance = main.BinanceFuturesClient
        original_bybit = main.BybitClient
        try:
            main.BinanceFuturesClient = FakeBinanceConfigClient
            main.BybitClient = FakeBybitConfigClient
            payload = self._payload("BTCUSDT")
            payload["exchange"] = "binance"
            start = self.client.post("/api/grid/start", json=payload)
            response = self.client.post(
                "/api/config",
                json={
                    "exchange": "binance",
                    "api_key": "binance-api-key",
                    "api_secret": "binance-api-secret",
                    "testnet": False,
                },
            )

            self.assertEqual(start.status_code, 200)
            self.assertEqual(response.status_code, 400)
            self.assertIn("Stop running", response.json()["detail"])
        finally:
            main.BinanceFuturesClient = original_binance
            main.BybitClient = original_bybit

    def test_api_config_can_be_saved_and_loaded_from_disk(self):
        original_path = main.API_CONFIG_FILE
        with tempfile.TemporaryDirectory() as tmpdir:
            main.API_CONFIG_FILE = str(Path(tmpdir) / "api_config.json")
            config = {
                "exchange": "binance",
                "api_key": "abcd1234efgh",
                "api_secret": "super-private-token",
                "testnet": True,
            }

            main._save_api_config(config)
            loaded = main._load_api_config()
            saved_text = Path(main.API_CONFIG_FILE).read_text(encoding="utf-8")

            self.assertEqual(loaded["api_key"], config["api_key"])
            self.assertEqual(loaded["api_secret"], config["api_secret"])
            self.assertEqual(loaded["exchange"], "binance")
            self.assertEqual(loaded["testnet"], config["testnet"])
            self.assertEqual(loaded["source"], "file")
            self.assertEqual(main._mask_api_key(loaded["api_key"]), "abcd...efgh")
            self.assertIn('"encrypted": true', saved_text)
            self.assertNotIn(config["api_key"], saved_text)
            self.assertNotIn(config["api_secret"], saved_text)

        main.API_CONFIG_FILE = original_path

    def test_api_config_can_be_loaded_from_environment(self):
        original_path = main.API_CONFIG_FILE
        old_values = {
            "GRID_EXCHANGE": os.environ.get("GRID_EXCHANGE"),
            "BYBIT_API_KEY": os.environ.get("BYBIT_API_KEY"),
            "BYBIT_API_SECRET": os.environ.get("BYBIT_API_SECRET"),
            "BYBIT_TESTNET": os.environ.get("BYBIT_TESTNET"),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                main.API_CONFIG_FILE = str(Path(tmpdir) / "missing_api_config.json")
                os.environ["GRID_EXCHANGE"] = "bybit"
                os.environ["BYBIT_API_KEY"] = "env-api-key"
                os.environ["BYBIT_API_SECRET"] = "env-api-secret"
                os.environ["BYBIT_TESTNET"] = "true"

                loaded = main._load_api_config()

                self.assertEqual(loaded["api_key"], "env-api-key")
                self.assertEqual(loaded["api_secret"], "env-api-secret")
                self.assertEqual(loaded["exchange"], "bybit")
                self.assertTrue(loaded["testnet"])
                self.assertEqual(loaded["source"], "env")
            finally:
                main.API_CONFIG_FILE = original_path
                for key, value in old_values.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_binance_api_config_can_be_loaded_from_environment(self):
        old_values = {
            "GRID_EXCHANGE": os.environ.get("GRID_EXCHANGE"),
            "BINANCE_API_KEY": os.environ.get("BINANCE_API_KEY"),
            "BINANCE_API_SECRET": os.environ.get("BINANCE_API_SECRET"),
            "BINANCE_TESTNET": os.environ.get("BINANCE_TESTNET"),
        }
        try:
            os.environ["GRID_EXCHANGE"] = "binance"
            os.environ["BINANCE_API_KEY"] = "binance-key"
            os.environ["BINANCE_API_SECRET"] = "binance-secret"
            os.environ["BINANCE_TESTNET"] = "true"

            loaded = main._load_env_api_config()

            self.assertEqual(loaded["exchange"], "binance")
            self.assertEqual(loaded["api_key"], "binance-key")
            self.assertEqual(loaded["api_secret"], "binance-secret")
            self.assertTrue(loaded["testnet"])
            self.assertEqual(loaded["source"], "env")
            self.assertIsInstance(
                main._build_client_from_config(loaded),
                BinanceFuturesClient,
            )
        finally:
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_aster_api_config_can_be_loaded_from_environment(self):
        old_values = {
            "GRID_EXCHANGE": os.environ.get("GRID_EXCHANGE"),
            "ASTER_USER_ADDRESS": os.environ.get("ASTER_USER_ADDRESS"),
            "ASTER_SIGNER_PRIVATE_KEY": os.environ.get("ASTER_SIGNER_PRIVATE_KEY"),
            "ASTER_TESTNET": os.environ.get("ASTER_TESTNET"),
        }
        try:
            os.environ["GRID_EXCHANGE"] = "aster"
            os.environ["ASTER_USER_ADDRESS"] = "0x0000000000000000000000000000000000000abc"
            os.environ["ASTER_SIGNER_PRIVATE_KEY"] = "0x" + "1" * 64
            os.environ["ASTER_TESTNET"] = "false"

            loaded = main._load_env_api_config()

            self.assertEqual(loaded["exchange"], "aster")
            self.assertEqual(loaded["api_key"], "0x0000000000000000000000000000000000000abc")
            self.assertEqual(loaded["api_secret"], "0x" + "1" * 64)
            self.assertFalse(loaded["testnet"])
            self.assertEqual(loaded["source"], "env")
            self.assertIsInstance(main._build_client_from_config(loaded), AsterFuturesClient)
        finally:
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_saved_file_config_takes_priority_over_environment(self):
        original_path = main.API_CONFIG_FILE
        old_values = {
            "GRID_EXCHANGE": os.environ.get("GRID_EXCHANGE"),
            "BYBIT_API_KEY": os.environ.get("BYBIT_API_KEY"),
            "BYBIT_API_SECRET": os.environ.get("BYBIT_API_SECRET"),
            "BYBIT_TESTNET": os.environ.get("BYBIT_TESTNET"),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                main.API_CONFIG_FILE = str(Path(tmpdir) / "api_config.json")
                os.environ["GRID_EXCHANGE"] = "bybit"
                os.environ["BYBIT_API_KEY"] = "env-bybit-key"
                os.environ["BYBIT_API_SECRET"] = "env-bybit-secret"
                os.environ["BYBIT_TESTNET"] = "false"

                main._save_api_config(
                    {
                        "exchange": "binance",
                        "api_key": "file-binance-key",
                        "api_secret": "file-binance-secret",
                        "testnet": True,
                    }
                )
                loaded = main._load_api_config()

                self.assertEqual(loaded["source"], "file")
                self.assertEqual(loaded["exchange"], "binance")
                self.assertEqual(loaded["api_key"], "file-binance-key")
                self.assertTrue(loaded["testnet"])
                self.assertIsInstance(main._build_client_from_config(loaded), BinanceFuturesClient)
            finally:
                main.API_CONFIG_FILE = original_path
                for key, value in old_values.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_api_config_endpoint_saves_binance_and_uses_binance_client(self):
        original_path = main.API_CONFIG_FILE
        original_binance = main.BinanceFuturesClient
        original_bybit = main.BybitClient
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                main.API_CONFIG_FILE = str(Path(tmpdir) / "api_config.json")
                main.BinanceFuturesClient = FakeBinanceConfigClient
                main.BybitClient = FakeBybitConfigClient

                response = self.client.post(
                    "/api/config",
                    json={
                        "exchange": "binance",
                        "api_key": "binance-api-key",
                        "api_secret": "binance-api-secret",
                        "testnet": True,
                    },
                )
                config_response = self.client.get("/api/config")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["message"], "Binance API config saved")
                self.assertIsInstance(main._client, FakeBinanceConfigClient)
                self.assertEqual(main._api_config["exchange"], "binance")
                self.assertEqual(config_response.json()["exchange"], "binance")
                self.assertEqual(config_response.json()["api_key"], "bina...-key")
            finally:
                main.API_CONFIG_FILE = original_path
                main.BinanceFuturesClient = original_binance
                main.BybitClient = original_bybit

    def test_api_config_endpoint_saves_bybit_and_uses_bybit_client(self):
        original_path = main.API_CONFIG_FILE
        original_binance = main.BinanceFuturesClient
        original_bybit = main.BybitClient
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                main.API_CONFIG_FILE = str(Path(tmpdir) / "api_config.json")
                main.BinanceFuturesClient = FakeBinanceConfigClient
                main.BybitClient = FakeBybitConfigClient

                response = self.client.post(
                    "/api/config",
                    json={
                        "exchange": "bybit",
                        "api_key": "bybit-api-key",
                        "api_secret": "bybit-api-secret",
                        "testnet": False,
                    },
                )
                config_response = self.client.get("/api/config")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["message"], "Bybit API config saved")
                self.assertIsInstance(main._client, FakeBybitConfigClient)
                self.assertEqual(main._api_config["exchange"], "bybit")
                self.assertEqual(config_response.json()["exchange"], "bybit")
                self.assertEqual(config_response.json()["api_key"], "bybi...-key")
            finally:
                main.API_CONFIG_FILE = original_path
                main.BinanceFuturesClient = original_binance
                main.BybitClient = original_bybit

    def test_api_config_endpoint_saves_aster_and_uses_aster_client(self):
        original_path = main.API_CONFIG_FILE
        original_aster = main.AsterFuturesClient
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                main.API_CONFIG_FILE = str(Path(tmpdir) / "api_config.json")
                main.AsterFuturesClient = FakeAsterConfigClient

                response = self.client.post(
                    "/api/config",
                    json={
                        "exchange": "aster",
                        "api_key": "0x0000000000000000000000000000000000000abc",
                        "api_secret": "0x" + "1" * 64,
                        "testnet": False,
                    },
                )
                config_response = self.client.get("/api/config")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["message"], "Aster API config saved")
                self.assertIsInstance(main._client, FakeAsterConfigClient)
                self.assertEqual(main._api_config["exchange"], "aster")
                self.assertEqual(config_response.json()["exchange"], "aster")
            finally:
                main.API_CONFIG_FILE = original_path
                main.AsterFuturesClient = original_aster

    def test_binance_order_and_position_shapes_match_grid_engine_contract(self):
        client = BinanceFuturesClient("", "", True)

        order = client._normalize_order(
            {
                "orderId": 123,
                "clientOrderId": "g_1_B_abcdef",
                "side": "BUY",
                "price": "100.5",
                "origQty": "0.01",
                "status": "NEW",
                "reduceOnly": False,
                "time": 1714012800000,
            }
        )
        position = client._normalize_position(
            {
                "positionAmt": "-0.25",
                "entryPrice": "105",
                "markPrice": "100",
                "unRealizedProfit": "1.25",
                "leverage": "3",
                "liquidationPrice": "140",
            }
        )
        client._asset_price_cache["BNBUSDT"] = Decimal("600")
        trade = client._normalize_trade(
            {
                "orderId": 123,
                "id": 456,
                "side": "BUY",
                "price": "100",
                "qty": "0.5",
                "quoteQty": "50",
                "commission": "0.001",
                "commissionAsset": "BNB",
                "maker": True,
            }
        )

        self.assertEqual(order["orderId"], "123")
        self.assertEqual(order["orderLinkId"], "g_1_B_abcdef")
        self.assertEqual(order["side"], "Buy")
        self.assertEqual(position["side"], "Sell")
        self.assertEqual(position["size"], "0.25")
        self.assertEqual(position["avgPrice"], "105")
        self.assertEqual(trade["feeAsset"], "BNB")
        self.assertEqual(trade["feeUsdt"], "0.600")
        self.assertTrue(trade["isMaker"])

    def test_binance_fee_asset_price_cache_expires(self):
        client = BinanceFuturesClient("", "", True)
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return {"price": "700"}

        client._request = fake_request
        client._asset_price_cache["BNBUSDT"] = (Decimal("600"), time.time() - 61)

        fee_usdt = client._fee_to_usdt(Decimal("0.001"), "BNB")

        self.assertEqual(fee_usdt, Decimal("0.700"))
        self.assertEqual(len(calls), 1)

    def test_private_state_files_are_chmod_600_on_unix(self):
        main._write_grid_state_file({"version": 1, "grids": {}})
        main._write_grid_history_file({"version": 1, "runs": []})

        if os.name != "nt":
            self.assertEqual(oct(Path(main.GRID_STATE_FILE).stat().st_mode & 0o777), "0o600")
            self.assertEqual(oct(Path(main.GRID_HISTORY_FILE).stat().st_mode & 0o777), "0o600")

    def test_cors_origins_default_to_same_origin_only(self):
        os.environ.pop("CORS_ALLOWED_ORIGINS", None)
        self.assertEqual(main._cors_allowed_origins(), [])

        os.environ["CORS_ALLOWED_ORIGINS"] = "https://example.com, http://127.0.0.1:8000"
        self.assertEqual(
            main._cors_allowed_origins(),
            ["https://example.com", "http://127.0.0.1:8000"],
        )

    def test_auth_required_blocks_api_until_totp_login(self):
        secret = pyotp.random_base32()
        os.environ["AUTH_REQUIRED"] = "true"
        os.environ["ADMIN_USERNAME"] = "admin"
        os.environ["ADMIN_PASSWORD_HASH"] = hash_password("correct horse battery staple")
        os.environ["TOTP_SECRET"] = secret
        os.environ["SESSION_SECRET"] = "test-session-secret"

        unauthenticated = self.client.get("/api/grid/status")
        self.assertEqual(unauthenticated.status_code, 401)

        login = self.client.post(
            "/api/auth/login",
            json={
                "username": "admin",
                "password": "correct horse battery staple",
                "code": pyotp.TOTP(secret).now(),
            },
        )
        self.assertEqual(login.status_code, 200)

        authenticated = self.client.get("/api/grid/status")
        self.assertEqual(authenticated.status_code, 200)


if __name__ == "__main__":
    unittest.main()
