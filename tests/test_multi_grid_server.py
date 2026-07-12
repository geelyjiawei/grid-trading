import asyncio
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

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
from exchange_errors import ExchangeRateLimitError, ExchangeRequestUncertainError  # noqa: E402
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
        self._original_grid_config_key = os.environ.get("GRID_CONFIG_KEY")
        # A non-secret, test-only Fernet key makes secure-storage tests portable
        # across Windows and the Linux production container.
        os.environ["GRID_CONFIG_KEY"] = "sIhr5IiGxypCvGJCNqSWKTujXBi7mPYx68efWDYmPhs="
        self._original_state_file = main.GRID_STATE_FILE
        self._original_history_file = main.GRID_HISTORY_FILE
        self._original_api_config_file = main.API_CONFIG_FILE
        self._original_api_configs = main._api_configs
        self._original_clients = main._clients
        self._original_active_exchange = main._active_exchange
        self._original_api_config = main._api_config
        self._original_client = main._client
        self._original_grid_state_integrity_error = main._grid_state_integrity_error
        self._original_grid_history_integrity_error = main._grid_history_integrity_error
        self._original_api_config_integrity_error = main._api_config_integrity_error
        self._original_api_config_read_error = main._api_config_read_error
        self._original_api_config_write_error = main._api_config_write_error
        self._original_api_config_tracked_path = main._api_config_tracked_path
        self._original_api_config_file_was_present = main._api_config_file_was_present
        self._state_tmp = tempfile.TemporaryDirectory()
        main.GRID_STATE_FILE = str(Path(self._state_tmp.name) / "grid_state.json")
        main.GRID_HISTORY_FILE = str(Path(self._state_tmp.name) / "grid_history.json")
        main.API_CONFIG_FILE = str(Path(self._state_tmp.name) / "api_config.json")
        main._engines.clear()
        main._starting_engine_keys.clear()
        main._stopping_engine_keys.clear()
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
        main._grid_state_integrity_error = ""
        main._grid_history_integrity_error = ""
        main._api_config_integrity_error = ""
        main._api_config_read_error = ""
        main._api_config_write_error = ""
        main._api_config_tracked_path = ""
        main._api_config_file_was_present = False
        self.client = TestClient(main.app)

    def tearDown(self):
        main._engines.clear()
        main._starting_engine_keys.clear()
        main._stopping_engine_keys.clear()
        main.GRID_STATE_FILE = self._original_state_file
        main.GRID_HISTORY_FILE = self._original_history_file
        main.API_CONFIG_FILE = self._original_api_config_file
        main._api_configs = self._original_api_configs
        main._clients = self._original_clients
        main._active_exchange = self._original_active_exchange
        main._api_config = self._original_api_config
        main._client = self._original_client
        main._grid_state_integrity_error = self._original_grid_state_integrity_error
        main._grid_history_integrity_error = self._original_grid_history_integrity_error
        main._api_config_integrity_error = self._original_api_config_integrity_error
        main._api_config_read_error = self._original_api_config_read_error
        main._api_config_write_error = self._original_api_config_write_error
        main._api_config_tracked_path = self._original_api_config_tracked_path
        main._api_config_file_was_present = self._original_api_config_file_was_present
        self._state_tmp.cleanup()
        if self._original_grid_config_key is None:
            os.environ.pop("GRID_CONFIG_KEY", None)
        else:
            os.environ["GRID_CONFIG_KEY"] = self._original_grid_config_key
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

    def test_same_symbol_concurrent_start_is_reserved_before_exchange_calls_finish(self):
        class BlockingLeverageClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.entered = threading.Event()
                self.release = threading.Event()

            def set_leverage(self, symbol, leverage):
                self.entered.set()
                if not self.release.wait(timeout=5):
                    raise TimeoutError("test leverage barrier timed out")
                return {"retCode": 0}

        blocking_client = BlockingLeverageClient("100")
        main._clients["binance"] = blocking_client
        main._client = blocking_client

        with ThreadPoolExecutor(max_workers=2) as pool:
            first_future = pool.submit(
                TestClient(main.app).post,
                "/api/grid/start",
                json=self._payload("RACEUSDT"),
            )
            self.assertTrue(blocking_client.entered.wait(timeout=2))
            second_response = TestClient(main.app).post(
                "/api/grid/start", json=self._payload("RACEUSDT")
            )
            blocking_client.release.set()
            first_response = first_future.result(timeout=10)

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 400)
        self.assertEqual(
            len(
                [
                    order
                    for order in blocking_client.orders
                    if order.get("order_type") == "Market"
                ]
            ),
            1,
        )

    def test_same_grid_concurrent_stop_is_reserved_before_cancellation(self):
        class BlockingCancelClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.entered = threading.Event()
                self.release = threading.Event()
                self.cancel_calls = 0
                self.cancel_lock = threading.Lock()

            def cancel_order(self, symbol, order_id):
                with self.cancel_lock:
                    self.cancel_calls += 1
                    first_call = self.cancel_calls == 1
                if first_call:
                    self.entered.set()
                    if not self.release.wait(timeout=5):
                        raise TimeoutError("test cancellation barrier timed out")
                return super().cancel_order(symbol, order_id)

        blocking_client = BlockingCancelClient("100")
        main._clients["binance"] = blocking_client
        main._client = blocking_client
        start = TestClient(main.app).post(
            "/api/grid/start",
            json=self._payload("STOPRACEUSDT"),
        )
        self.assertEqual(start.status_code, 200, start.text)
        expected_cancel_calls = len(blocking_client.open_limit_order_ids)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first_future = pool.submit(
                TestClient(main.app).post,
                "/api/grid/stop/STOPRACEUSDT?exchange=binance",
            )
            self.assertTrue(blocking_client.entered.wait(timeout=2))
            second_response = TestClient(main.app).post(
                "/api/grid/stop/STOPRACEUSDT?exchange=binance"
            )
            blocking_client.release.set()
            first_response = first_future.result(timeout=10)

        self.assertEqual(first_response.status_code, 200, first_response.text)
        self.assertEqual(second_response.status_code, 409)
        self.assertIn("already stopping", second_response.json()["detail"].lower())
        self.assertEqual(blocking_client.cancel_calls, expected_cancel_calls)

    def test_stop_all_continues_after_one_grid_fails(self):
        first_start = self.client.post(
            "/api/grid/start", json=self._payload("STOPALLAUSDT")
        )
        second_start = self.client.post(
            "/api/grid/start", json=self._payload("STOPALLBUSDT")
        )
        self.assertEqual(first_start.status_code, 200, first_start.text)
        self.assertEqual(second_start.status_code, 200, second_start.text)
        first_key = main._engine_key("binance", "STOPALLAUSDT")
        second_key = main._engine_key("binance", "STOPALLBUSDT")
        first_engine = main._engines[first_key]
        second_order_ids = {
            str(order["order_id"])
            for order in main._engines[second_key].active_orders.values()
        }

        async def fail_first_stop():
            raise RuntimeError("injected first-grid stop failure")

        with patch.object(first_engine, "stop", new=fail_first_stop):
            response = self.client.post("/api/grid/stop-all")

        self.assertEqual(response.status_code, 503, response.text)
        detail = response.json()["detail"]
        self.assertIn("STOPALLAUSDT", detail)
        self.assertIn("STOPALLBUSDT", detail)
        self.assertIn(first_key, main._engines)
        self.assertNotIn(second_key, main._engines)
        self.assertTrue(
            second_order_ids.isdisjoint(main._client.open_limit_order_ids)
        )

        retry = self.client.post(
            "/api/grid/stop/STOPALLAUSDT?exchange=binance"
        )
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertNotIn(first_key, main._engines)

    def test_stop_all_stops_and_archives_every_grid(self):
        symbols = ("STOPALLCUSDT", "STOPALLDUSDT")
        run_ids = {}
        for symbol in symbols:
            start = self.client.post(
                "/api/grid/start", json=self._payload(symbol)
            )
            self.assertEqual(start.status_code, 200, start.text)
            key = main._engine_key("binance", symbol)
            run_ids[symbol] = main._engines[key].config["run_id"]

        response = self.client.post("/api/grid/stop-all")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(main._engines, {})
        self.assertEqual(main._load_grid_state_file()["grids"], {})
        self.assertEqual(main._client.open_limit_order_ids, set())
        history_by_run = {
            item["run_id"]: item
            for item in main._load_grid_history_file()["runs"]
        }
        for symbol in symbols:
            record = history_by_run[run_ids[symbol]]
            self.assertEqual(record["symbol"], symbol)
            self.assertEqual(record["status"], "stopped")
            self.assertIsNotNone(record["stopped_at"])

    def test_stale_stop_snapshot_cannot_overwrite_new_grid_instance(self):
        first_start = self.client.post(
            "/api/grid/start", json=self._payload("STALESTOPUSDT")
        )
        self.assertEqual(first_start.status_code, 200, first_start.text)
        key = main._engine_key("binance", "STALESTOPUSDT")
        stale_engine = main._engines[key]

        first_stop = self.client.post(
            "/api/grid/stop/STALESTOPUSDT?exchange=binance"
        )
        self.assertEqual(first_stop.status_code, 200, first_stop.text)
        second_start = self.client.post(
            "/api/grid/start", json=self._payload("STALESTOPUSDT")
        )
        self.assertEqual(second_start.status_code, 200, second_start.text)
        current_engine = main._engines[key]
        current_run_id = current_engine.config["run_id"]
        open_before = set(main._client.open_limit_order_ids)

        with self.assertRaises(main.HTTPException) as error:
            asyncio.run(main._stop_engine_once(stale_engine))

        self.assertEqual(error.exception.status_code, 409)
        self.assertIs(main._engines[key], current_engine)
        self.assertTrue(current_engine.running)
        self.assertEqual(main._client.open_limit_order_ids, open_before)
        saved = main._load_grid_state_file()["grids"][key]
        self.assertEqual(saved["config"]["run_id"], current_run_id)

    def test_completed_stale_stop_is_idempotent_with_retained_position(self):
        start = self.client.post(
            "/api/grid/start", json=self._payload("STALEDONEUSDT")
        )
        self.assertEqual(start.status_code, 200, start.text)
        key = main._engine_key("binance", "STALEDONEUSDT")
        stale_engine = main._engines[key]
        self.assertGreater(abs(stale_engine._grid_position_qty()), 0)

        stop = self.client.post(
            "/api/grid/stop/STALEDONEUSDT?exchange=binance"
        )
        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])

        result = asyncio.run(main._stop_engine_once(stale_engine))

        self.assertEqual(result, ("STALEDONEUSDT", "binance"))
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])

    def test_unregistered_incomplete_stop_snapshot_is_rejected_without_writes(self):
        start = self.client.post(
            "/api/grid/start", json=self._payload("ORPHANSTOPUSDT")
        )
        self.assertEqual(start.status_code, 200, start.text)
        key = main._engine_key("binance", "ORPHANSTOPUSDT")
        engine = main._engines.pop(key)
        open_before = set(main._client.open_limit_order_ids)
        saved_before = main._load_grid_state_file()["grids"][key]

        with self.assertRaises(main.HTTPException) as error:
            asyncio.run(main._stop_engine_once(engine))

        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(main._client.open_limit_order_ids, open_before)
        saved_after = main._load_grid_state_file()["grids"][key]
        self.assertEqual(saved_after, saved_before)

    def test_concurrent_engine_state_saves_preserve_both_grids(self):
        original_load = main._load_grid_state_file

        def slow_load():
            state = original_load()
            time.sleep(0.05)
            return state

        def fake_engine(symbol):
            engine = Mock()
            engine.config = {"symbol": symbol, "exchange": "binance"}
            engine.running = True
            engine.to_state.return_value = {
                "config": dict(engine.config),
                "running": True,
            }
            return engine

        first = fake_engine("FIRSTUSDT")
        second = fake_engine("SECONDUSDT")

        with (
            patch.object(main, "_load_grid_state_file", side_effect=slow_load),
            patch.object(main, "_upsert_grid_history"),
            ThreadPoolExecutor(max_workers=2) as pool,
        ):
            futures = [
                pool.submit(main._save_engine_state, first),
                pool.submit(main._save_engine_state, second),
            ]
            for future in futures:
                future.result(timeout=5)

        grids = original_load()["grids"]
        self.assertEqual(
            set(grids),
            {
                main._engine_key("binance", "FIRSTUSDT"),
                main._engine_key("binance", "SECONDUSDT"),
            },
        )

    def test_concurrent_history_upserts_preserve_both_runs(self):
        original_load = main._load_grid_history_file

        def slow_load():
            history = original_load()
            time.sleep(0.05)
            return history

        def history_record(engine, status):
            run_id = engine.config["run_id"]
            return {
                "run_id": run_id,
                "started_at": 1 if run_id == "run-first" else 2,
                "status": status,
            }

        first = Mock()
        first.config = {"run_id": "run-first"}
        second = Mock()
        second.config = {"run_id": "run-second"}

        with (
            patch.object(main, "_load_grid_history_file", side_effect=slow_load),
            patch.object(main, "_history_record_from_engine", side_effect=history_record),
            ThreadPoolExecutor(max_workers=2) as pool,
        ):
            futures = [
                pool.submit(main._upsert_grid_history, first),
                pool.submit(main._upsert_grid_history, second),
            ]
            for future in futures:
                future.result(timeout=5)

        history = original_load()
        self.assertEqual(
            {record["run_id"] for record in history["runs"]},
            {"run-first", "run-second"},
        )

    def test_corrupt_history_is_preserved_without_blocking_state_persistence(self):
        corrupt_bytes = b'{"version": 1, "runs": ['
        Path(main.GRID_HISTORY_FILE).write_bytes(corrupt_bytes)
        engine = Mock()
        engine.config = {
            "run_id": "history-corrupt-run",
            "symbol": "HISTORYUSDT",
            "exchange": "binance",
        }
        engine.running = True
        engine.to_state.return_value = {
            "running": True,
            "config": dict(engine.config),
        }

        main._save_engine_state(engine)

        saved_state = main._load_grid_state_file()
        self.assertIn(
            main._engine_key("binance", "HISTORYUSDT"),
            saved_state["grids"],
        )
        self.assertEqual(Path(main.GRID_HISTORY_FILE).read_bytes(), corrupt_bytes)
        self.assertTrue(main._grid_history_integrity_error)
        response = self.client.get("/api/grid/history")
        self.assertEqual(response.status_code, 503)
        self.assertIn("preserved", response.json()["detail"])
        risk = self.client.get("/api/risk/HISTORYUSDT?exchange=binance").json()
        self.assertTrue(risk["has_risk"])
        self.assertEqual(
            risk["history_store_error"],
            main._grid_history_integrity_error,
        )

    def test_repaired_history_resumes_updates_and_clears_integrity_error(self):
        Path(main.GRID_HISTORY_FILE).write_text("not-json", encoding="utf-8")
        with self.assertRaises(main.GridHistoryIntegrityError):
            main._load_grid_history_file()

        main._write_grid_history_file({"version": 1, "runs": []})
        engine = Mock()
        engine.config = {"run_id": "history-repaired-run"}
        with patch.object(
            main,
            "_history_record_from_engine",
            return_value={
                "run_id": "history-repaired-run",
                "started_at": 1,
                "status": "running",
            },
        ):
            saved = main._upsert_grid_history(engine, "running")

        self.assertTrue(saved)
        self.assertEqual(main._grid_history_integrity_error, "")
        self.assertEqual(
            main._load_grid_history_file()["runs"][0]["run_id"],
            "history-repaired-run",
        )

    def test_history_write_failure_does_not_break_trading_state_save(self):
        engine = Mock()
        engine.config = {
            "run_id": "history-write-failure",
            "symbol": "WRITEFAILUSDT",
            "exchange": "binance",
        }
        engine.running = True
        engine.to_state.return_value = {
            "running": True,
            "config": dict(engine.config),
        }

        with (
            patch.object(
                main,
                "_history_record_from_engine",
                return_value={
                    "run_id": "history-write-failure",
                    "started_at": 1,
                    "status": "running",
                },
            ),
            patch.object(
                main,
                "_write_grid_history_file",
                side_effect=PermissionError("history is read-only"),
            ),
        ):
            main._save_engine_state(engine)

        state = main._load_grid_state_file()
        self.assertIn(
            main._engine_key("binance", "WRITEFAILUSDT"),
            state["grids"],
        )
        self.assertIn("cannot be written", main._grid_history_integrity_error)

    def test_state_saves_throttle_history_but_persist_status_transitions(self):
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "THROTTLEUSDT",
                "exchange": "binance",
                "direction": "short",
                "run_id": "throttle-run",
            },
        )

        with patch.object(main, "_upsert_grid_history") as upsert:
            main._save_engine_state(engine)
            main._save_engine_state(engine)
            main._save_engine_state(engine)
            self.assertEqual(upsert.call_count, 1)
            self.assertEqual(upsert.call_args.args[1], "saved")

            engine.running = True
            main._save_engine_state(engine)
            main._save_engine_state(engine)

        self.assertEqual(upsert.call_count, 2)
        self.assertEqual(upsert.call_args.args[1], "running")

    def test_state_file_is_flushed_before_atomic_replace(self):
        events = []
        real_replace = os.replace

        def record_fsync(_descriptor):
            events.append("fsync")

        def record_replace(source, destination):
            events.append("replace")
            return real_replace(source, destination)

        with (
            patch.object(main.os, "fsync", side_effect=record_fsync),
            patch.object(main.os, "replace", side_effect=record_replace),
        ):
            main._write_grid_state_file({"version": 1, "grids": {}})

        self.assertGreaterEqual(len(events), 2)
        self.assertEqual(events[:2], ["fsync", "replace"])
        self.assertTrue(Path(main.GRID_STATE_FILE).exists())

    def test_corrupt_state_file_blocks_new_grid_without_overwriting_ledger(self):
        corrupt_payload = '{"version": 1, "grids": '
        Path(main.GRID_STATE_FILE).write_text(corrupt_payload, encoding="utf-8")

        response = self.client.post(
            "/api/grid/start", json=self._payload("CORRUPTUSDT")
        )

        self.assertEqual(response.status_code, 503, response.text)
        self.assertIn("durable grid state file", response.json()["detail"])
        self.assertEqual(main._client.orders, [])
        self.assertEqual(
            Path(main.GRID_STATE_FILE).read_text(encoding="utf-8"),
            corrupt_payload,
        )

        risk = self.client.get(
            "/api/risk/CORRUPTUSDT?exchange=binance"
        ).json()
        self.assertTrue(risk["has_risk"])
        self.assertIn("durable grid state file", risk["state_store_error"])

    def test_initialization_failure_after_market_fill_is_retained_and_blocks_retry(self):
        original_save = main._save_engine_state
        save_calls = 0

        def fail_after_exchange_submission(engine):
            nonlocal save_calls
            save_calls += 1
            if save_calls >= 3:
                raise OSError("simulated state persistence failure")
            return original_save(engine)

        with patch.object(main, "_save_engine_state", side_effect=fail_after_exchange_submission):
            response = self.client.post(
                "/api/grid/start", json=self._payload("INITFAILUSDT")
            )

        self.assertEqual(response.status_code, 409, response.text)
        key = main._engine_key("binance", "INITFAILUSDT")
        engine = main._engines[key]
        retry = self.client.post("/api/grid/start", json=self._payload("INITFAILUSDT"))

        self.assertTrue(engine.initialization_failed)
        self.assertEqual(retry.status_code, 400)
        self.assertEqual(
            len(
                [
                    order
                    for order in main._client.orders
                    if order.get("order_type") == "Market"
                ]
            ),
            1,
        )
        self.assertEqual(float(main._client.positions[0]["size"]), 2.0)

        engine.state_callback = original_save
        stop = self.client.post("/api/grid/stop/INITFAILUSDT?exchange=binance")

        self.assertEqual(stop.status_code, 200)
        self.assertNotIn(key, main._engines)
        self.assertEqual(float(main._client.positions[0]["size"]), 2.0)

    def test_pre_exchange_start_failure_is_archived_without_saved_state(self):
        class RejectInitialGridClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.limit_attempts = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Limit":
                    self.limit_attempts += 1
                    return {
                        "retCode": -2010,
                        "retMsg": "definitive test rejection",
                        "result": {},
                    }
                return super().place_order(**kwargs)

        client = RejectInitialGridClient("100")
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("STARTREJECTUSDT")
        payload.update(
            {
                "direction": "neutral",
                "grid_order_post_only": True,
            }
        )

        response = self.client.post("/api/grid/start", json=payload)

        self.assertEqual(response.status_code, 400, response.text)
        key = main._engine_key("binance", "STARTREJECTUSDT")
        self.assertEqual(client.limit_attempts, 4)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["symbol"] == "STARTREJECTUSDT"
        )
        self.assertEqual(record["status"], "failed")
        self.assertIsNone(record["stopped_at"])

        retry_client = FakeClient("100")
        main._client = retry_client
        main._clients["binance"] = retry_client
        retry = self.client.post("/api/grid/start", json=payload)
        self.assertEqual(retry.status_code, 200, retry.text)
        retry_engine = main._engines[key]
        self.assertNotEqual(retry_engine.config["run_id"], record["run_id"])
        retry_stop = self.client.post(
            "/api/grid/stop/STARTREJECTUSDT?exchange=binance"
        )
        self.assertEqual(retry_stop.status_code, 200, retry_stop.text)

    def test_failed_start_history_write_retains_state_until_restart_retry(self):
        class RejectInitialGridClient(FakeClient):
            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Limit":
                    return {
                        "retCode": -2010,
                        "retMsg": "definitive test rejection",
                        "result": {},
                    }
                return super().place_order(**kwargs)

        client = RejectInitialGridClient("100")
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("FAILHISTORYUSDT")
        payload.update(
            {
                "direction": "neutral",
                "grid_order_post_only": True,
            }
        )
        original_upsert = main._upsert_grid_history

        def fail_failed_history(engine, status="running"):
            if status == "failed":
                return False
            return original_upsert(engine, status)

        with patch.object(
            main,
            "_upsert_grid_history",
            side_effect=fail_failed_history,
        ):
            response = self.client.post("/api/grid/start", json=payload)

        self.assertEqual(response.status_code, 503, response.text)
        key = main._engine_key("binance", "FAILHISTORYUSDT")
        retained = main._engines[key]
        self.assertFalse(retained.running)
        self.assertTrue(retained.initialization_failed)
        self.assertTrue(retained.stop_finalize_pending)
        self.assertIn(key, main._load_grid_state_file()["grids"])
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])

        main._engines.clear()
        main._restore_saved_engines()

        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["symbol"] == "FAILHISTORYUSDT"
        )
        self.assertEqual(record["status"], "failed")

    def test_initial_deployment_ledger_alone_blocks_failed_start_cleanup(self):
        async def fail_after_durable_deployment_intent(engine):
            engine.initialization_in_progress = True
            engine.initial_grid_deployment_pending = True
            engine.initial_grid_deployment_ledger = {
                "Sell|0|110.0|1.0|0": {
                    "link_id": "g_0_S_durableintent",
                    "side": "Sell",
                    "level_idx": 0,
                    "price": "110.0",
                    "qty": "1.0",
                    "reduce_only": False,
                }
            }
            engine._persist_state()
            raise RuntimeError("exchange acknowledgement interrupted")

        with patch.object(
            main.GridEngine,
            "initialize",
            new=fail_after_durable_deployment_intent,
        ):
            response = self.client.post(
                "/api/grid/start",
                json=self._payload("LEDGERONLYUSDT"),
            )

        self.assertEqual(response.status_code, 409, response.text)
        key = main._engine_key("binance", "LEDGERONLYUSDT")
        retained = main._engines[key]
        self.assertTrue(retained.initialization_failed)
        self.assertTrue(retained.initial_grid_deployment_ledger)
        saved = main._load_grid_state_file()["grids"][key]
        self.assertTrue(saved["initial_grid_deployment_ledger"])

        stop_started_at = time.monotonic()
        stop = self.client.post(
            "/api/grid/stop/LEDGERONLYUSDT?exchange=binance"
        )
        stop_elapsed = time.monotonic() - stop_started_at
        self.assertEqual(stop.status_code, 503, stop.text)
        self.assertLess(stop_elapsed, 5)
        self.assertIn("initial deployment ledger", stop.json()["detail"].lower())
        self.assertIn(key, main._engines)
        self.assertTrue(retained.initial_grid_deployment_ledger)
        saved_after_stop = main._load_grid_state_file()["grids"][key]
        self.assertTrue(saved_after_stop["initial_grid_deployment_ledger"])

    def test_background_initialization_cleanup_archives_after_terminal_cancel(self):
        class PartiallyAcceptedInitialGridClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.limit_attempts = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Limit":
                    self.limit_attempts += 1
                    if self.limit_attempts > 1:
                        return {
                            "retCode": -2010,
                            "retMsg": "definitive deployment rejection",
                            "result": {},
                        }
                return super().place_order(**kwargs)

        client = PartiallyAcceptedInitialGridClient("100")
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("BACKGROUNDCLEANUSDT")
        payload.update(
            {
                "direction": "neutral",
                "grid_order_post_only": True,
            }
        )

        response = self.client.post("/api/grid/start", json=payload)

        self.assertEqual(response.status_code, 409, response.text)
        key = main._engine_key("binance", "BACKGROUNDCLEANUSDT")
        deadline = time.time() + 5
        while key in main._engines and time.time() < deadline:
            time.sleep(0.05)

        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        self.assertEqual(client.open_limit_order_ids, set())
        self.assertEqual(len(client.cancelled_orders), 1)
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["symbol"] == "BACKGROUNDCLEANUSDT"
        )
        self.assertEqual(record["status"], "failed")

    def test_ledger_only_open_order_is_recovered_and_cancelled_on_stop(self):
        config = self._payload("RECOVERLEDGERUSDT")
        config.update(
            {
                "exchange": "binance",
                "run_id": "recover-ledger-run",
            }
        )
        placed = main._client.place_order(
            symbol="RECOVERLEDGERUSDT",
            side="Sell",
            qty="1",
            price="110",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_0_S_recoverledger",
        )
        order_id = str(placed["result"]["orderId"])
        engine = main.GridEngine(main._client, config, state_callback=main._save_engine_state)
        engine._fetch_precision()
        engine.running = False
        engine.grid_ready = False
        engine.initialization_failed = True
        engine.initial_grid_deployment_ledger = {
            "Sell|0|110.0|1.0|0": {
                "link_id": "g_0_S_recoverledger",
                "side": "Sell",
                "level_idx": 0,
                "price": "110.0",
                "qty": "1.0",
                "reduce_only": False,
            }
        }
        key = main._engine_key("binance", "RECOVERLEDGERUSDT")
        main._engines[key] = engine
        main._save_engine_state(engine)

        stop = self.client.post(
            "/api/grid/stop/RECOVERLEDGERUSDT?exchange=binance"
        )

        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertIn(order_id, main._client.cancelled_orders)
        self.assertNotIn(order_id, main._client.open_limit_order_ids)
        self.assertEqual(engine.initial_grid_deployment_ledger, {})
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["run_id"] == "recover-ledger-run"
        )
        self.assertEqual(record["status"], "stopped")

    def test_unfilled_opening_terminal_failure_is_archived_as_failed(self):
        payload = self._payload("ASYNCOPENFAILUSDT")
        payload.update(
            {
                "initial_order_type": "limit",
                "initial_order_price": 99,
            }
        )
        start = self.client.post("/api/grid/start", json=payload)
        self.assertEqual(start.status_code, 200, start.text)
        key = main._engine_key("binance", "ASYNCOPENFAILUSDT")
        engine = main._engines[key]
        run_id = engine.config["run_id"]
        opening_order_id = str(engine.opening_order["order_id"])
        main._client.cancel_order("ASYNCOPENFAILUSDT", opening_order_id)
        main._client.ticker_price = 120

        asyncio.run(engine._check_initial_order())

        self.assertEqual(main._client.positions, [])
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["run_id"] == run_id
        )
        self.assertEqual(record["status"], "failed")
        self.assertIsNone(record["stopped_at"])

    def test_completed_risk_shutdown_is_archived_as_closed(self):
        class FilledRiskClient(FakeClient):
            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                order = next(
                    item
                    for item in self.orders
                    if str(item["orderId"]) == str(order_id)
                )
                if order.get("order_type") == "Market":
                    qty = float(order["qty"])
                    response["result"].update(
                        {
                            "orderLinkId": order.get("order_link_id", ""),
                            "orderStatus": "FILLED",
                            "executedQty": str(qty),
                            "cumQuote": str(qty * 100),
                            "avgPrice": "100",
                        }
                    )
                return response

            def get_order_trades(self, symbol, order_id):
                order = next(
                    item
                    for item in self.orders
                    if str(item["orderId"]) == str(order_id)
                )
                qty = float(order["qty"])
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "qty": str(qty),
                                "price": "100",
                                "volume": str(qty * 100),
                                "feeUsdt": "0.05",
                                "feeAsset": "USDT",
                                "isMaker": False,
                            }
                        ]
                    },
                }

        client = FilledRiskClient("100")
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("RISKCOMPLETEUSDT")
        payload["take_profit_price"] = 101
        start = self.client.post("/api/grid/start", json=payload)
        self.assertEqual(start.status_code, 200, start.text)
        key = main._engine_key("binance", "RISKCOMPLETEUSDT")
        engine = main._engines[key]
        run_id = engine.config["run_id"]
        self.assertTrue(main._client.positions)

        completed = asyncio.run(engine._shutdown_with_close())

        self.assertTrue(completed)
        self.assertEqual(main._client.positions, [])
        self.assertEqual(main._client.open_limit_order_ids, set())
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["run_id"] == run_id
        )
        self.assertEqual(record["status"], "closed")
        self.assertIsNotNone(record["stopped_at"])

    def test_completed_baseline_breach_is_archived_as_failed(self):
        main._client.positions = [
            {"side": "Sell", "size": "3", "avgPrice": "100"}
        ]
        payload = self._payload("BASELINEFAILUSDT")
        payload["direction"] = "short"
        start = self.client.post("/api/grid/start", json=payload)
        self.assertEqual(start.status_code, 200, start.text)
        key = main._engine_key("binance", "BASELINEFAILUSDT")
        engine = main._engines[key]
        run_id = engine.config["run_id"]
        self.assertEqual(engine.baseline_position_qty, 3.0)
        main._client.positions = [
            {"side": "Sell", "size": "2.9", "avgPrice": "100"}
        ]

        halted = engine._halt_if_baseline_breached()

        self.assertTrue(halted)
        self.assertEqual(main._client.open_limit_order_ids, set())
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["run_id"] == run_id
        )
        self.assertEqual(record["status"], "failed")
        self.assertIsNone(record["stopped_at"])

    def test_restore_preserves_closed_terminal_history_status(self):
        config = self._payload("RESTORECLOSEDUSDT")
        config.update(
            {
                "exchange": "binance",
                "run_id": "restore-closed-run",
            }
        )
        engine = main.GridEngine(main._client, config)
        engine.running = False
        engine.grid_ready = False
        engine.stop_finalize_pending = True
        engine.finalize_history_status = "closed"
        key = main._engine_key("binance", "RESTORECLOSEDUSDT")
        main._write_grid_state_file(
            {"version": 1, "grids": {key: engine.to_state()}}
        )
        self.assertTrue(main._upsert_grid_history(engine, "saved"))

        main._restore_saved_engines()

        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["run_id"] == "restore-closed-run"
        )
        self.assertEqual(record["status"], "closed")
        self.assertIsNotNone(record["stopped_at"])

    def test_restore_never_finalizes_nonempty_initial_deployment_ledger(self):
        config = self._payload("RESTORELEDGERUSDT")
        config.update(
            {
                "exchange": "binance",
                "run_id": "restore-ledger-run",
            }
        )
        engine = main.GridEngine(main._client, config)
        engine.running = False
        engine.grid_ready = False
        engine.stop_finalize_pending = True
        engine.finalize_history_status = "stopped"
        engine.initialization_in_progress = True
        engine.initial_grid_deployment_pending = True
        engine.initial_grid_deployment_ledger = {
            "Sell|0|110.0|1.0|0": {
                "link_id": "g_0_S_restoreledger",
                "side": "Sell",
                "level_idx": 0,
                "price": "110.0",
                "qty": "1.0",
                "reduce_only": False,
            }
        }
        key = main._engine_key("binance", "RESTORELEDGERUSDT")
        main._write_grid_state_file(
            {"version": 1, "grids": {key: engine.to_state()}}
        )

        main._restore_saved_engines()

        self.assertIn(key, main._engines)
        retained = main._engines[key]
        self.assertFalse(retained.running)
        self.assertTrue(retained.manual_stop_pending)
        self.assertTrue(retained.stop_finalize_pending)
        self.assertTrue(retained.initial_grid_deployment_ledger)
        self.assertIn(key, main._load_grid_state_file()["grids"])

    def test_rate_limited_initial_opening_starts_in_recoverable_wait_state(self):
        class RateLimitedStartClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.market_calls = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Market" and not kwargs.get("reduce_only"):
                    self.market_calls += 1
                    raise ExchangeRateLimitError(
                        "Too many requests while opening",
                        retry_after=60,
                    )
                return super().place_order(**kwargs)

            def get_order_history(self, symbol, limit=50):
                return {"retCode": 0, "result": {"list": []}}

        client = RateLimitedStartClient("100")
        main._client = client
        main._clients["binance"] = client

        response = self.client.post(
            "/api/grid/start",
            json=self._payload("LIMITWAITUSDT"),
        )

        self.assertEqual(response.status_code, 200, response.text)
        engine = main._get_engine("binance", "LIMITWAITUSDT")
        self.assertIsNotNone(engine)
        self.assertTrue(engine.running)
        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertFalse(engine.initialization_failed)
        self.assertFalse(engine.manual_stop_pending)
        self.assertTrue(engine.opening_order.get("submission_pending"))
        self.assertTrue(engine.opening_order.get("submission_retry_safe"))
        self.assertGreater(engine._rate_limit_remaining(), 59)
        self.assertEqual(client.market_calls, 1)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])

        saved = main._load_grid_state_file()["grids"][
            main._engine_key("binance", "LIMITWAITUSDT")
        ]
        self.assertTrue(saved["running"])
        self.assertTrue(saved["opening_order"]["submission_retry_safe"])

    def test_rate_limited_initial_grid_deployment_returns_running_recoverable_state(self):
        class RateLimitedGridDeploymentClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.batch_attempts = 0
                self.batch_calls = []

            def place_orders(self, orders):
                self.batch_attempts += 1
                self.batch_calls.append([dict(order) for order in orders])
                if self.batch_attempts == 2:
                    raise ExchangeRateLimitError(
                        "Too many requests during initial grid deployment",
                        retry_after=60,
                    )
                return {
                    "retCode": 0,
                    "result": {
                        "list": [self.place_order(**order) for order in orders],
                    },
                }

        client = RateLimitedGridDeploymentClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("GRIDLIMITUSDT")
        payload.update(
            {
                "direction": "short",
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            }
        )

        response = self.client.post("/api/grid/start", json=payload)

        self.assertEqual(response.status_code, 200, response.text)
        engine = main._get_engine("binance", "GRIDLIMITUSDT")
        self.assertIsNotNone(engine)
        self.assertTrue(engine.running)
        self.assertTrue(engine.initial_grid_deployment_pending)
        self.assertTrue(engine.initialization_in_progress)
        self.assertFalse(engine.initialization_failed)
        self.assertFalse(engine.manual_stop_pending)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(engine.active_orders), 5)
        self.assertEqual(len(engine.initial_grid_deployment_ledger), 5)
        self.assertEqual(float(client.positions[0]["size"]), 10.0)
        self.assertEqual(
            len([order for order in client.orders if order.get("order_type") == "Market"]),
            1,
        )
        self.assertEqual(
            len([order for order in client.orders if order.get("order_type") == "Limit"]),
            5,
        )

        saved = main._load_grid_state_file()["grids"][
            main._engine_key("binance", "GRIDLIMITUSDT")
        ]
        self.assertTrue(saved["running"])
        self.assertTrue(saved["initial_grid_deployment_pending"])
        self.assertEqual(len(saved["initial_grid_deployment_ledger"]), 5)

        risk = self.client.get(
            "/api/risk/GRIDLIMITUSDT?exchange=binance"
        ).json()
        self.assertTrue(risk["initial_grid_deployment_pending"])
        self.assertEqual(risk["initial_grid_deployment_submitted_count"], 5)
        self.assertEqual(risk["initial_grid_deployment_total_count"], 20)
        self.assertTrue(risk["has_risk"])
        history = self.client.get(
            "/api/grid/history?exchange=binance"
        ).json()["runs"]
        run = next(item for item in history if item["symbol"] == "GRIDLIMITUSDT")
        self.assertEqual(run["status"], "running")

        stop = self.client.post(
            "/api/grid/stop/GRIDLIMITUSDT?exchange=binance"
        )
        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertIsNone(main._get_engine("binance", "GRIDLIMITUSDT"))
        self.assertEqual(float(client.positions[0]["size"]), 10.0)
        self.assertEqual(client.open_limit_order_ids, set())

    def test_nonrunning_engine_with_unconfirmed_orders_blocks_restart_and_can_be_stopped(self):
        config = self._payload("MUUSDT")
        config["exchange"] = "binance"
        config["direction"] = "short"
        engine = main.GridEngine(main._client, config, state_callback=main._save_engine_state)
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine._place("Sell", 110, 0, reduce_only=False, qty_override=1)
        engine.running = False
        main._engines[main._engine_key("binance", "MUUSDT")] = engine

        restart = self.client.post("/api/grid/start", json=self._payload("MUUSDT"))
        stop = self.client.post("/api/grid/stop/MUUSDT?exchange=binance")

        self.assertEqual(restart.status_code, 400)
        self.assertIn("unconfirmed exchange work", restart.json()["detail"])
        self.assertEqual(stop.status_code, 200)
        self.assertNotIn(main._engine_key("binance", "MUUSDT"), main._engines)

    def test_nonrunning_engine_with_queued_replacement_blocks_restart(self):
        config = self._payload("MUUSDT")
        config["exchange"] = "binance"
        engine = main.GridEngine(main._client, config, state_callback=main._save_engine_state)
        engine.running = False
        engine.grid_levels = [90, 110]
        engine.paused_replacements = [
            {
                "link_id": "g_0_B_filled",
                "order_id": "old",
                "level_idx": 0,
                "side": "Buy",
                "price": "90",
                "qty": "1",
                "fill_price": 90.0,
                "reduce_only": False,
            }
        ]
        key = main._engine_key("binance", "MUUSDT")
        main._engines[key] = engine

        restart = self.client.post("/api/grid/start", json=self._payload("MUUSDT"))
        stop = self.client.post("/api/grid/stop/MUUSDT?exchange=binance")

        self.assertEqual(restart.status_code, 400)
        self.assertIn("unconfirmed exchange work", restart.json()["detail"])
        self.assertEqual(stop.status_code, 200)
        self.assertNotIn(key, main._engines)

    def test_nonrunning_engine_with_retained_grid_position_blocks_restart(self):
        config = self._payload("MUUSDT")
        config["exchange"] = "binance"
        config["direction"] = "short"
        engine = main.GridEngine(main._client, config, state_callback=main._save_engine_state)
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.grid_position_net_qty = -1.0
        engine.running = False
        main._client.positions = [{"side": "Sell", "size": "1", "avgPrice": "100"}]
        key = main._engine_key("binance", "MUUSDT")
        main._engines[key] = engine

        restart = self.client.post("/api/grid/start", json=self._payload("MUUSDT"))
        stop = self.client.post("/api/grid/stop/MUUSDT?exchange=binance")

        self.assertEqual(restart.status_code, 400)
        self.assertIn("unconfirmed exchange work", restart.json()["detail"])
        self.assertEqual(stop.status_code, 200)
        self.assertNotIn(key, main._engines)
        self.assertEqual(main._client.positions[0]["size"], "1")

    def test_start_is_blocked_by_existing_grid_tagged_exchange_order(self):
        existing = main._client.place_order(
            symbol="ORPHANUSDT",
            side="Sell",
            qty="1",
            price="105",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_1_S_oldrun",
        )
        order_count = len(main._client.orders)

        response = self.client.post(
            "/api/grid/start", json=self._payload("ORPHANUSDT")
        )

        self.assertEqual(response.status_code, 409)
        self.assertIn("existing grid-tagged", response.json()["detail"])
        self.assertEqual(len(main._client.orders), order_count)
        self.assertIn(str(existing["result"]["orderId"]), main._client.open_limit_order_ids)
        self.assertNotIn(main._engine_key("binance", "ORPHANUSDT"), main._engines)

    def test_risk_classifies_opening_and_repair_client_ids_as_grid_orders(self):
        self.assertTrue(main._is_grid_order({"orderLinkId": "init_S_abcdef"}))
        self.assertTrue(main._is_grid_order({"orderLinkId": "repair_B_abcdef"}))
        self.assertFalse(main._is_grid_order({"orderLinkId": "manual_order"}))

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

    def test_stop_history_failure_retains_durable_engine_until_retry(self):
        start = self.client.post("/api/grid/start", json=self._payload("BILLUSDT"))
        self.assertEqual(start.status_code, 200)
        key = main._engine_key("binance", "BILLUSDT")
        original_upsert = main._upsert_grid_history

        def fail_final_history(engine, status="running"):
            if status == "stopped":
                return False
            return original_upsert(engine, status)

        with patch.object(main, "_upsert_grid_history", side_effect=fail_final_history):
            stop = self.client.post("/api/grid/stop/BILLUSDT?exchange=binance")

        self.assertEqual(stop.status_code, 503)
        self.assertIn("history", stop.json()["detail"].lower())
        self.assertIn(key, main._engines)
        retained = main._engines[key]
        self.assertFalse(retained.running)
        self.assertTrue(retained.stop_finalize_pending)
        self.assertIn(key, main._load_grid_state_file()["grids"])
        self.assertEqual(main._client.open_limit_order_ids, set())

        retry = self.client.post("/api/grid/stop/BILLUSDT?exchange=binance")
        self.assertEqual(retry.status_code, 200)
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        history = main._load_grid_history_file()["runs"]
        record = next(item for item in history if item["symbol"] == "BILLUSDT")
        self.assertEqual(record["status"], "stopped")
        self.assertIsNotNone(record["stopped_at"])

    def test_stop_assigns_run_id_before_archiving_legacy_engine(self):
        config = self._payload("LEGACYUSDT")
        config.update({"exchange": "binance", "direction": "short"})
        engine = main.GridEngine(
            main._client,
            config,
            state_callback=main._save_engine_state,
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine._place("Sell", 105, 0, reduce_only=False, qty_override=1)
        engine.running = True
        engine.grid_ready = True
        engine.start_time = 1234.5
        key = main._engine_key("binance", "LEGACYUSDT")
        main._engines[key] = engine
        main._save_engine_state(engine)
        self.assertNotIn("run_id", engine.config)

        stop = self.client.post(
            "/api/grid/stop/LEGACYUSDT?exchange=binance"
        )

        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["symbol"] == "LEGACYUSDT"
        )
        self.assertTrue(record["run_id"])
        self.assertEqual(record["status"], "stopped")
        self.assertIsNotNone(record["stopped_at"])

    def test_legacy_stop_run_id_rolls_back_when_intent_write_fails(self):
        config = self._payload("LEGACYFAILUSDT")
        config.update({"exchange": "binance", "direction": "short"})
        engine = main.GridEngine(
            main._client,
            config,
            state_callback=main._save_engine_state,
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine._place("Sell", 105, 0, reduce_only=False, qty_override=1)
        engine.running = True
        engine.grid_ready = True
        key = main._engine_key("binance", "LEGACYFAILUSDT")
        main._engines[key] = engine
        main._save_engine_state(engine)
        open_before = set(main._client.open_limit_order_ids)

        with patch.object(
            main,
            "_write_grid_state_file",
            side_effect=PermissionError("state is read-only"),
        ):
            stop = self.client.post(
                "/api/grid/stop/LEGACYFAILUSDT?exchange=binance"
            )

        self.assertEqual(stop.status_code, 503)
        self.assertNotIn("run_id", engine.config)
        self.assertTrue(engine.running)
        self.assertFalse(engine.manual_stop_pending)
        self.assertFalse(engine.stop_finalize_pending)
        self.assertEqual(main._client.open_limit_order_ids, open_before)

    def test_stop_intent_survives_restart_without_resuming_grid_orders(self):
        config = self._payload("CRASHSTOPUSDT")
        config.update(
            {
                "exchange": "binance",
                "direction": "short",
                "run_id": "crash-stop-before-cleanup",
            }
        )
        engine = main.GridEngine(
            main._client,
            config,
            state_callback=main._save_engine_state,
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        link_id = engine._place(
            "Sell",
            105,
            0,
            reduce_only=False,
            qty_override=1,
        )
        order_id = str(engine.active_orders[link_id]["order_id"])
        engine.running = True
        engine.grid_ready = True
        engine.stop_finalize_pending = True
        engine.manual_stop_pending = True
        key = main._engine_key("binance", "CRASHSTOPUSDT")
        main._engines[key] = engine
        main._save_engine_state(engine)
        order_count = len(main._client.orders)

        main._engines.clear()
        main._restore_saved_engines()

        self.assertIn(key, main._engines)
        restored = main._engines[key]
        self.assertFalse(restored.running)
        self.assertTrue(restored.manual_stop_pending)
        self.assertTrue(restored.stop_finalize_pending)
        self.assertIn(link_id, restored.active_orders)
        self.assertEqual(len(main._client.orders), order_count)
        self.assertIn(order_id, main._client.open_limit_order_ids)

        stop = self.client.post(
            "/api/grid/stop/CRASHSTOPUSDT?exchange=binance"
        )

        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        self.assertNotIn(order_id, main._client.open_limit_order_ids)
        self.assertEqual(len(main._client.orders), order_count)

    def test_restore_auto_finalizes_cleanup_completed_before_crash(self):
        config = self._payload("ARCHIVEUSDT")
        config.update(
            {
                "exchange": "binance",
                "direction": "short",
                "run_id": "crash-stop-after-cleanup",
            }
        )
        engine = main.GridEngine(main._client, config)
        engine.running = False
        engine.grid_ready = False
        engine.stop_finalize_pending = True
        key = main._engine_key("binance", "ARCHIVEUSDT")
        main._write_grid_state_file(
            {"version": 1, "grids": {key: engine.to_state()}}
        )
        self.assertTrue(main._upsert_grid_history(engine, "saved"))

        main._restore_saved_engines()

        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["run_id"] == "crash-stop-after-cleanup"
        )
        self.assertEqual(record["status"], "stopped")
        self.assertIsNotNone(record["stopped_at"])

    def test_stop_does_not_cancel_when_durable_intent_write_fails(self):
        start = self.client.post("/api/grid/start", json=self._payload("INTENTUSDT"))
        self.assertEqual(start.status_code, 200)
        key = main._engine_key("binance", "INTENTUSDT")
        engine = main._engines[key]
        open_before = set(main._client.open_limit_order_ids)

        with patch.object(
            main,
            "_write_grid_state_file",
            side_effect=PermissionError("state is read-only"),
        ):
            stop = self.client.post(
                "/api/grid/stop/INTENTUSDT?exchange=binance"
            )

        self.assertEqual(stop.status_code, 503)
        self.assertIn("persist", stop.json()["detail"].lower())
        self.assertIn(key, main._engines)
        self.assertIs(main._engines[key], engine)
        self.assertTrue(engine.running)
        self.assertFalse(engine.manual_stop_pending)
        self.assertFalse(engine.stop_finalize_pending)
        self.assertEqual(main._client.open_limit_order_ids, open_before)

        retry = self.client.post(
            "/api/grid/stop/INTENTUSDT?exchange=binance"
        )
        self.assertEqual(retry.status_code, 200, retry.text)

    def test_stop_does_not_cancel_when_failed_intent_cannot_be_read_back(self):
        start = self.client.post("/api/grid/start", json=self._payload("READBACKUSDT"))
        self.assertEqual(start.status_code, 200)
        key = main._engine_key("binance", "READBACKUSDT")
        engine = main._engines[key]
        open_before = set(main._client.open_limit_order_ids)
        original_load = main._load_grid_state_file
        loads = 0

        def load_once_then_fail():
            nonlocal loads
            loads += 1
            if loads == 1:
                return original_load()
            raise main.GridStateIntegrityError("state cannot be read back")

        with (
            patch.object(
                main,
                "_load_grid_state_file",
                side_effect=load_once_then_fail,
            ),
            patch.object(
                main,
                "_write_grid_state_file",
                side_effect=PermissionError("state write outcome is unknown"),
            ),
        ):
            stop = self.client.post(
                "/api/grid/stop/READBACKUSDT?exchange=binance"
            )

        self.assertEqual(stop.status_code, 503)
        self.assertIn(key, main._engines)
        self.assertIs(main._engines[key], engine)
        self.assertTrue(engine.running)
        self.assertFalse(engine.manual_stop_pending)
        self.assertFalse(engine.stop_finalize_pending)
        self.assertEqual(main._client.open_limit_order_ids, open_before)

    def test_stop_continues_when_write_error_occurs_after_intent_is_durable(self):
        start = self.client.post("/api/grid/start", json=self._payload("FSYNCUSDT"))
        self.assertEqual(start.status_code, 200)
        key = main._engine_key("binance", "FSYNCUSDT")
        original_write = main._write_grid_state_file
        writes = 0

        def persist_then_fail_once(state):
            nonlocal writes
            writes += 1
            original_write(state)
            if writes == 1:
                raise OSError("directory fsync failed after atomic replace")

        with patch.object(
            main,
            "_write_grid_state_file",
            side_effect=persist_then_fail_once,
        ):
            stop = self.client.post(
                "/api/grid/stop/FSYNCUSDT?exchange=binance"
            )

        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])
        self.assertEqual(main._client.open_limit_order_ids, set())
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["symbol"] == "FSYNCUSDT"
        )
        self.assertEqual(record["status"], "stopped")

    def test_stop_does_not_trust_durable_intent_from_another_run(self):
        start = self.client.post("/api/grid/start", json=self._payload("STALERUNUSDT"))
        self.assertEqual(start.status_code, 200)
        key = main._engine_key("binance", "STALERUNUSDT")
        engine = main._engines[key]
        open_before = set(main._client.open_limit_order_ids)

        state = main._load_grid_state_file()
        stale = state["grids"][key]
        stale["config"]["run_id"] = "stale-previous-run"
        stale["manual_stop_pending"] = True
        stale["stop_finalize_pending"] = True
        main._write_grid_state_file(state)

        with patch.object(
            main,
            "_write_grid_state_file",
            side_effect=PermissionError("state is read-only"),
        ):
            stop = self.client.post(
                "/api/grid/stop/STALERUNUSDT?exchange=binance"
            )

        self.assertEqual(stop.status_code, 503)
        self.assertIn(key, main._engines)
        self.assertIs(main._engines[key], engine)
        self.assertTrue(engine.running)
        self.assertFalse(engine.manual_stop_pending)
        self.assertFalse(engine.stop_finalize_pending)
        self.assertEqual(main._client.open_limit_order_ids, open_before)

    def test_stop_state_delete_failure_retains_finalization_for_retry(self):
        start = self.client.post("/api/grid/start", json=self._payload("DELETEUSDT"))
        self.assertEqual(start.status_code, 200)
        key = main._engine_key("binance", "DELETEUSDT")
        original_write = main._write_grid_state_file

        def fail_only_final_delete(state):
            if key not in state.get("grids", {}):
                raise PermissionError("cannot delete final grid state")
            return original_write(state)

        with patch.object(
            main,
            "_write_grid_state_file",
            side_effect=fail_only_final_delete,
        ):
            stop = self.client.post(
                "/api/grid/stop/DELETEUSDT?exchange=binance"
            )

        self.assertEqual(stop.status_code, 503)
        self.assertIn("final", stop.json()["detail"].lower())
        self.assertIn(key, main._engines)
        retained = main._engines[key]
        self.assertFalse(retained.running)
        self.assertTrue(retained.stop_finalize_pending)
        self.assertIn(key, main._load_grid_state_file()["grids"])
        self.assertEqual(main._client.open_limit_order_ids, set())
        record = next(
            item
            for item in main._load_grid_history_file()["runs"]
            if item["symbol"] == "DELETEUSDT"
        )
        self.assertEqual(record["status"], "stopped")

        retry = self.client.post(
            "/api/grid/stop/DELETEUSDT?exchange=binance"
        )
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertNotIn(key, main._engines)
        self.assertNotIn(key, main._load_grid_state_file()["grids"])

    def test_grid_status_reports_total_profit_with_unrealised_pnl(self):
        main._client.positions = [{"side": "Buy", "size": "1", "unrealisedPnl": "2.5"}]

        start = self.client.post("/api/grid/start", json=self._payload("BILLUSDT"))
        status = self.client.get("/api/grid/status/BILLUSDT").json()

        self.assertEqual(start.status_code, 200)
        self.assertIn("realized_net_profit", status)
        self.assertEqual(status["account_unrealised_pnl"], 2.5)
        self.assertEqual(status["unrealised_pnl"], 0.0)
        self.assertAlmostEqual(status["total_equity_profit"], status["realized_net_profit"])

    def test_fee_rates_endpoint_returns_account_values(self):
        main._client.maker_fee_rate = "0"
        main._client.taker_fee_rate = "0.0004"

        response = self.client.get("/api/fees/ANSEMUSDT?exchange=binance")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "exchange": "binance",
                "symbol": "ANSEMUSDT",
                "maker_fee_rate": "0",
                "taker_fee_rate": "0.0004",
                "source": "exchange",
                "fetched_at": 1714012800000,
            },
        )
        self.assertEqual(main._client.fee_rate_calls, ["ANSEMUSDT"])

    def test_grid_preview_uses_account_rates_not_submitted_rates(self):
        main._client = FakeClient("100")
        main._client.maker_fee_rate = "0"
        main._client.taker_fee_rate = "0.0004"
        payload = self._payload("FEEUSDT")
        payload.update(
            {
                "fee_rate": 0.009,
                "maker_fee_rate": 0.009,
                "taker_fee_rate": 0.009,
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["maker_fee_rate"], 0.0)
        self.assertEqual(data["taker_fee_rate"], 0.0004)
        self.assertEqual(data["fee_rate_source"], "exchange")
        self.assertAlmostEqual(data["per_grid_close_fee"], 0.0)
        self.assertAlmostEqual(data["per_grid_open_fee"], 0.04)

    def test_grid_start_persists_account_rates_not_submitted_rates(self):
        main._client = FakeClient("100")
        main._client.maker_fee_rate = "0.0001"
        main._client.taker_fee_rate = "0.0006"
        payload = self._payload("FEEUSDT")
        payload.update({"maker_fee_rate": 0, "taker_fee_rate": 0, "fee_rate": 0})

        response = self.client.post("/api/grid/start", json=payload)
        engine = main._get_engine("binance", "FEEUSDT")

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(engine)
        self.assertEqual(engine.config["maker_fee_rate"], 0.0001)
        self.assertEqual(engine.config["taker_fee_rate"], 0.0006)
        self.assertEqual(engine.config["fee_rate"], 0.0006)
        self.assertEqual(engine.config["fee_rate_source"], "exchange")
        status = self.client.get("/api/grid/status/FEEUSDT?exchange=binance").json()
        history = self.client.get("/api/grid/history?exchange=binance").json()["runs"]
        record = next(item for item in history if item["symbol"] == "FEEUSDT")
        self.assertEqual(status["maker_fee_rate"], 0.0001)
        self.assertEqual(status["taker_fee_rate"], 0.0006)
        self.assertEqual(status["fee_rate_source"], "exchange")
        self.assertEqual(record["maker_fee_rate"], 0.0001)
        self.assertEqual(record["taker_fee_rate"], 0.0006)
        self.assertEqual(record["fee_rate_source"], "exchange")

    def test_grid_start_fails_before_orders_when_fee_rate_lookup_fails(self):
        main._client = FakeClient("100")

        def fail_fee_lookup(symbol):
            raise RuntimeError("fee endpoint unavailable")

        main._client.get_fee_rates = fail_fee_lookup

        response = self.client.post("/api/grid/start", json=self._payload("FEEFAILUSDT"))

        self.assertEqual(response.status_code, 502)
        self.assertIn("fee endpoint unavailable", response.json()["detail"])
        self.assertEqual(main._client.orders, [])
        self.assertIsNone(main._get_engine("binance", "FEEFAILUSDT"))

    def test_grid_start_rejects_malformed_exchange_fee_rates_before_orders(self):
        main._client = FakeClient("100")
        main._client.maker_fee_rate = "NaN"

        response = self.client.post("/api/grid/start", json=self._payload("BADFEEUSDT"))

        self.assertEqual(response.status_code, 502)
        self.assertIn("invalid maker fee rate", response.json()["detail"])
        self.assertEqual(main._client.orders, [])
        self.assertIsNone(main._get_engine("binance", "BADFEEUSDT"))

    def test_fee_endpoint_rejects_wrong_symbol_response(self):
        main._client = FakeClient("100")
        main._clients["binance"] = main._client
        main._client.get_fee_rates = lambda symbol: {
            "retCode": 0,
            "result": {
                "symbol": "OTHERUSDT",
                "makerFeeRate": "0.0002",
                "takerFeeRate": "0.0005",
                "source": "exchange",
                "fetchedAt": 1714012800000,
            },
        }

        response = self.client.get("/api/fees/TESTUSDT?exchange=binance")

        self.assertEqual(response.status_code, 502)
        self.assertIn("fee rate", response.json()["detail"])
        self.assertIn("symbol", response.json()["detail"])

    def test_balance_endpoint_rejects_malformed_snapshots(self):
        valid_coin = {
            "coin": "USDT",
            "availableToWithdraw": "10",
            "walletBalance": "12",
            "equity": "11",
            "unrealisedPnl": "-1",
        }

        def response_with_coin(coin):
            return {
                "retCode": 0,
                "result": {"list": [{"coin": [coin]}]},
            }

        cases = {
            "top-level list": [],
            "missing result": {"retCode": 0},
            "invalid result": {"retCode": 0, "result": []},
            "missing account list": {"retCode": 0, "result": {}},
            "invalid account list": {"retCode": 0, "result": {"list": {}}},
            "empty account list": {"retCode": 0, "result": {"list": []}},
            "multiple account rows": {
                "retCode": 0,
                "result": {"list": [{"coin": []}, {"coin": []}]},
            },
            "invalid account row": {
                "retCode": 0,
                "result": {"list": [None]},
            },
            "invalid coin list": {
                "retCode": 0,
                "result": {"list": [{"coin": {}}]},
            },
            "invalid coin row": {
                "retCode": 0,
                "result": {"list": [{"coin": [None]}]},
            },
            "duplicate usdt": {
                "retCode": 0,
                "result": {"list": [{"coin": [valid_coin, valid_coin]}]},
            },
            "missing available balance": response_with_coin(
                {key: value for key, value in valid_coin.items() if key != "availableToWithdraw"}
                | {"walletBalance": ""}
            ),
            "nan available balance": response_with_coin(
                {**valid_coin, "availableToWithdraw": "NaN"}
            ),
            "infinite equity": response_with_coin(
                {**valid_coin, "equity": "Infinity"}
            ),
            "boolean pnl": response_with_coin(
                {**valid_coin, "unrealisedPnl": True}
            ),
        }

        for label, snapshot in cases.items():
            with self.subTest(label=label):
                client = FakeClient("100")
                client.get_balance = lambda snapshot=snapshot: snapshot
                main._client = client
                main._clients["binance"] = client

                response = self.client.get("/api/balance?exchange=binance")

                self.assertEqual(response.status_code, 502)
                self.assertIn("balance snapshot", response.json()["detail"])

    def test_balance_endpoint_allows_valid_account_without_usdt(self):
        client = FakeClient("100")
        client.get_balance = lambda: {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "BTC",
                                "availableToWithdraw": "1",
                                "walletBalance": "1",
                                "equity": "1",
                                "unrealisedPnl": "0",
                            }
                        ]
                    }
                ]
            },
        }
        main._client = client
        main._clients["binance"] = client

        response = self.client.get("/api/balance?exchange=binance")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"available": "0", "equity": "0", "unrealised_pnl": "0"},
        )

    def test_price_and_preview_reject_wrong_symbol_ticker(self):
        client = FakeClient("100")
        client.get_ticker = lambda symbol: {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "OTHERUSDT",
                        "lastPrice": "1",
                        "markPrice": "1",
                    }
                ]
            },
        }
        main._client = client
        main._clients["binance"] = client

        price_response = self.client.get("/api/price/TESTUSDT?exchange=binance")
        preview_response = self.client.post(
            "/api/grid/preview",
            json=self._payload("TESTUSDT"),
        )

        self.assertEqual(price_response.status_code, 502)
        self.assertIn("ticker snapshot", price_response.json()["detail"])
        self.assertEqual(preview_response.status_code, 502)
        self.assertIn("ticker snapshot", preview_response.json()["detail"])
        self.assertEqual(client.orders, [])

    def test_preview_rejects_wrong_symbol_instrument_snapshot(self):
        client = FakeClient("100")
        original_get_instrument_info = client.get_instrument_info

        def wrong_symbol_instrument(symbol):
            response = original_get_instrument_info(symbol)
            response["result"]["list"][0]["symbol"] = "OTHERUSDT"
            return response

        client.get_instrument_info = wrong_symbol_instrument
        main._client = client
        main._clients["binance"] = client

        response = self.client.post(
            "/api/grid/preview",
            json=self._payload("TESTUSDT"),
        )

        self.assertEqual(response.status_code, 502)
        self.assertIn("instrument snapshot", response.json()["detail"])
        self.assertEqual(client.orders, [])

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

    def test_grid_preview_rejects_per_grid_qty_below_limit_minimum(self):
        main._client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        payload = self._payload("MUUSDT")
        payload.update(
            {
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.05,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("limit-order minimum", response.json()["detail"])
        self.assertEqual(main._client.orders, [])

    def test_grid_start_rejects_per_grid_qty_below_limit_minimum_without_opening(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("MUUSDT")
        payload.update(
            {
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.05,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/start", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("below exchange quantity precision or minimum", response.json()["detail"])
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])
        self.assertNotIn(main._engine_key("binance", "MUUSDT"), main._engines)

    def test_grid_preview_rejects_levels_that_round_to_duplicate_exchange_prices(self):
        main._client = FakeClient("1010", tick_size="1", qty_step="0.1", min_qty="0.1")
        payload = self._payload("MUUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 1000,
                "upper_price": 1020,
                "grid_count": 40,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("duplicate exchange prices", response.json()["detail"])
        self.assertEqual(main._client.orders, [])

    def test_grid_preview_rejects_market_max_before_start(self):
        main._client = FakeClient(
            "100",
            tick_size="1",
            qty_step="0.1",
            min_qty="0.1",
            max_market_qty="0.4",
        )
        payload = self._payload("MUUSDT")
        payload.update(
            {
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("single-order maximum", response.json()["detail"])
        self.assertEqual(main._client.orders, [])

    def test_grid_preview_rejects_fixed_qty_market_step_drift(self):
        main._client = FakeClient(
            "100",
            tick_size="1",
            qty_step="0.1",
            min_qty="0.1",
            market_qty_step="0.3",
            market_min_qty="0.3",
        )
        payload = self._payload("MUUSDT")
        payload.update(
            {
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("cannot be represented exactly", response.json()["detail"])
        self.assertEqual(main._client.orders, [])

    def test_grid_preview_investment_mode_uses_market_quantity_rules(self):
        main._client = FakeClient(
            "100",
            tick_size="1",
            qty_step="0.1",
            min_qty="0.1",
            market_qty_step="0.3",
            market_min_qty="0.3",
            max_market_qty="2.0",
        )
        payload = self._payload("MUUSDT")
        payload.update(
            {
                "total_investment": 105,
                "leverage": 1,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)
        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertAlmostEqual(data["total_qty"], 0.9)
        self.assertEqual(data["market_qty_step"], "0.3")
        self.assertEqual(data["market_min_qty"], 0.3)
        self.assertEqual(data["max_market_qty"], 2.0)
        self.assertAlmostEqual(data["qty_per_grid_min"], 0.4)
        self.assertAlmostEqual(data["qty_per_grid_max"], 0.5)

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

    def test_restore_migrates_legacy_total_fill_count_from_history(self):
        symbol = "COUNTUSDT"
        exchange = "binance"
        run_id = "count-run"
        key = main._engine_key(exchange, symbol)
        recent_fills = [
            {"side": "Sell", "price": 100, "qty": 1, "reduce_only": False}
            for _ in range(200)
        ]
        main._write_grid_state_file(
            {
                "version": 1,
                "grids": {
                    key: {
                        "config": {
                            "run_id": run_id,
                            "symbol": symbol,
                            "exchange": exchange,
                            "direction": "short",
                            "grid_mode": "arithmetic",
                            "upper_price": 110,
                            "lower_price": 90,
                            "grid_count": 1,
                            "total_investment": 0,
                            "position_sizing_mode": "fixed_grid_qty",
                            "grid_order_qty": 1,
                            "leverage": 2,
                        },
                        "running": False,
                        "grid_ready": False,
                        "grid_levels": [90, 110],
                        "filled_orders": recent_fills,
                        "tick_size": "1",
                        "qty_step": "0.1",
                        "min_qty": 0.1,
                    }
                },
            }
        )
        main._write_grid_history_file(
            {
                "version": 1,
                "runs": [
                    {
                        "run_id": run_id,
                        "symbol": symbol,
                        "exchange": exchange,
                        "filled_count": 572,
                        "started_at": 1,
                    }
                ],
            }
        )

        main._restore_saved_engines()

        engine = main._engines[key]
        saved = main._load_grid_state_file()["grids"][key]
        history = main._load_grid_history_file()["runs"][0]
        self.assertEqual(engine.filled_count, 572)
        self.assertEqual(saved["filled_count"], 572)
        self.assertEqual(history["filled_count"], 572)

    def test_restore_resumes_incomplete_cleanup_even_when_saved_running_is_false(self):
        symbol = "INTERRUPTUSDT"
        exchange = "binance"
        placed = main._client.place_order(
            symbol=symbol,
            side="Sell",
            qty="1",
            price="101",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_0_S_interrupted",
        )
        order_id = str(placed["result"]["orderId"])
        key = main._engine_key(exchange, symbol)
        main._write_grid_state_file(
            {
                "version": 1,
                "grids": {
                    key: {
                        "config": {
                            "symbol": symbol,
                            "exchange": exchange,
                            "direction": "short",
                            "grid_mode": "arithmetic",
                            "upper_price": 110,
                            "lower_price": 90,
                            "grid_count": 1,
                            "total_investment": 100,
                            "leverage": 2,
                        },
                        "running": False,
                        "grid_ready": False,
                        "grid_levels": [90, 110],
                        "manual_stop_pending": True,
                        "initialization_failed": True,
                        "active_orders": {
                            "g_0_S_interrupted": {
                                "link_id": "g_0_S_interrupted",
                                "order_id": order_id,
                                "level_idx": 0,
                                "side": "Sell",
                                "price": "101",
                                "qty": "1",
                                "status": "open",
                                "order_type": "Limit",
                                "time_in_force": "GTC",
                                "reduce_only": False,
                                "processed_fill_qty": 0.0,
                                "processed_fill_volume": 0.0,
                                "processed_fill_fee": 0.0,
                            }
                        },
                    }
                },
            }
        )

        async def restore_and_wait():
            main._restore_saved_engines()
            engine = main._engines[key]
            for _ in range(100):
                if engine._task and engine._task.done():
                    break
                await asyncio.sleep(0.01)
            if engine._task:
                await engine._task
            return engine

        engine = asyncio.run(restore_and_wait())

        self.assertFalse(engine.running)
        self.assertFalse(engine.manual_stop_pending)
        self.assertEqual(engine.active_orders, {})
        self.assertNotIn(order_id, main._client.open_limit_order_ids)

    def test_restore_registers_rule_refresh_failure_without_normal_order_placement(self):
        class FailingRulesClient(FakeClient):
            def get_instrument_info(self, symbol):
                raise RuntimeError("exchangeInfo unavailable at process startup")

        symbol = "RULEWAITUSDT"
        exchange = "binance"
        key = main._engine_key(exchange, symbol)
        client = FailingRulesClient("100")
        main._client = client
        main._clients[exchange] = client
        main._write_grid_state_file(
            {
                "version": 1,
                "grids": {
                    key: {
                        "config": {
                            "symbol": symbol,
                            "exchange": exchange,
                            "direction": "short",
                            "grid_mode": "arithmetic",
                            "upper_price": 110,
                            "lower_price": 90,
                            "grid_count": 1,
                            "total_investment": 0,
                            "position_sizing_mode": "fixed_grid_qty",
                            "grid_order_qty": 1,
                            "qty_per_grid": 1,
                            "leverage": 2,
                        },
                        "running": True,
                        "grid_ready": True,
                        "grid_levels": [90, 110],
                        "target_qty_by_level": {"0": 1},
                        "tick_size": "1",
                        "qty_step": "1",
                        "min_qty": 1,
                    }
                },
            }
        )

        async def restore_observe_and_stop():
            main._restore_saved_engines()
            engine = main._engines[key]
            await asyncio.sleep(0)
            observed = {
                "running": engine.running,
                "pending": engine.restore_refresh_pending,
                "error": engine.restore_refresh_error,
                "orders": list(client.orders),
            }
            engine.running = False
            await engine.suspend()
            return observed

        observed = asyncio.run(restore_observe_and_stop())

        self.assertTrue(observed["running"])
        self.assertTrue(observed["pending"])
        self.assertIn("exchangeInfo unavailable", observed["error"])
        self.assertEqual(observed["orders"], [])

    def test_stopped_grid_rule_refresh_failure_does_not_reactivate_strategy(self):
        class FailingRulesClient(FakeClient):
            def get_instrument_info(self, symbol):
                raise RuntimeError("exchangeInfo unavailable for stopped grid")

        symbol = "STOPPEDUSDT"
        exchange = "binance"
        key = main._engine_key(exchange, symbol)
        client = FailingRulesClient("100")
        main._client = client
        main._clients[exchange] = client
        main._write_grid_state_file(
            {
                "version": 1,
                "grids": {
                    key: {
                        "config": {
                            "symbol": symbol,
                            "exchange": exchange,
                            "direction": "short",
                            "grid_mode": "arithmetic",
                            "upper_price": 110,
                            "lower_price": 90,
                            "grid_count": 1,
                            "total_investment": 0,
                            "position_sizing_mode": "fixed_grid_qty",
                            "grid_order_qty": 1,
                            "qty_per_grid": 1,
                            "leverage": 2,
                        },
                        "running": False,
                        "grid_ready": False,
                        "grid_levels": [90, 110],
                        "restore_refresh_pending": False,
                        "restore_saved_running": False,
                    }
                },
            }
        )

        main._restore_saved_engines()

        engine = main._engines[key]
        durable = engine.to_state()
        self.assertFalse(engine.running)
        self.assertIsNone(engine._task)
        self.assertTrue(engine.restore_refresh_pending)
        self.assertFalse(engine._restore_saved_running)
        self.assertFalse(durable["restore_saved_running"])
        self.assertEqual(client.orders, [])

    def test_restore_resumes_waiting_opening_order_when_saved_running_is_false(self):
        symbol = "WAITOPENUSDT"
        exchange = "binance"
        link_id = "open_S_waiting"
        placed = main._client.place_order(
            symbol=symbol,
            side="Sell",
            qty="1",
            price="101",
            order_type="Limit",
            reduce_only=False,
            order_link_id=link_id,
        )
        order_id = str(placed["result"]["orderId"])
        key = main._engine_key(exchange, symbol)
        main._write_grid_state_file(
            {
                "version": 1,
                "grids": {
                    key: {
                        "config": {
                            "symbol": symbol,
                            "exchange": exchange,
                            "direction": "short",
                            "grid_mode": "arithmetic",
                            "upper_price": 110,
                            "lower_price": 90,
                            "grid_count": 1,
                            "total_investment": 100,
                            "leverage": 2,
                            "initial_order_type": "limit",
                            "initial_order_price": 101,
                        },
                        "running": False,
                        "grid_ready": False,
                        "grid_levels": [90, 110],
                        "waiting_initial_order": True,
                        "opening_order": {
                            "link_id": link_id,
                            "order_id": order_id,
                            "side": "Sell",
                            "price": "101",
                            "qty": "1",
                            "status": "open",
                            "order_type": "Limit",
                            "time_in_force": "GTC",
                            "reduce_only": False,
                        },
                    }
                },
            }
        )

        async def restore_observe_and_stop():
            main._restore_saved_engines()
            engine = main._engines[key]
            await asyncio.sleep(0.05)
            resumed = bool(engine.running and engine._task and not engine._task.done())
            waiting = engine.waiting_initial_order
            await engine.stop()
            return resumed, waiting

        resumed, waiting = asyncio.run(restore_observe_and_stop())

        self.assertTrue(resumed)
        self.assertTrue(waiting)
        self.assertNotIn(order_id, main._client.open_limit_order_ids)

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

    def test_risk_endpoint_flags_qty_step_position_delta_below_min_qty(self):
        main._client = FakeClient("100", qty_step="0.01", min_qty="0.1")
        main._client.positions = [{"side": "Sell", "size": "0.01", "avgPrice": "100"}]
        exchange = main._active_exchange
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "STEPUSDT",
                "exchange": exchange,
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.running = True
        main._engines[main._engine_key(exchange, "STEPUSDT")] = engine

        response = self.client.get(f"/api/risk/STEPUSDT?exchange={exchange}")

        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertTrue(snapshot["unmanaged_position"])
        self.assertEqual(snapshot["unmanaged_delta_qty"], -0.01)
        self.assertTrue(snapshot["has_risk"])

    def test_position_and_risk_endpoints_reject_invalid_position_snapshot(self):
        main._client.positions = [
            {"side": "Sell", "size": "invalid", "avgPrice": "100"}
        ]

        positions_response = self.client.get("/api/positions/BADUSDT?exchange=binance")
        risk_response = self.client.get("/api/risk/BADUSDT?exchange=binance")

        self.assertEqual(positions_response.status_code, 502)
        self.assertIn("position snapshot", positions_response.json()["detail"])
        self.assertEqual(risk_response.status_code, 502)
        self.assertIn("position snapshot", risk_response.json()["detail"])

    def test_position_endpoint_maps_upstream_failures_without_false_success(self):
        cases = (
            (
                "transport failure",
                Mock(side_effect=TimeoutError("positions offline")),
                502,
                "position snapshot request failed",
            ),
            (
                "exchange rejection",
                Mock(return_value={"retCode": 1001, "retMsg": "position denied"}),
                400,
                "position denied",
            ),
            (
                "malformed response",
                Mock(return_value=[]),
                502,
                "position snapshot",
            ),
        )

        for label, get_positions_mock, expected_status, expected_detail in cases:
            with self.subTest(label=label):
                with patch.object(main._client, "get_positions", get_positions_mock):
                    response = self.client.get(
                        "/api/positions/BADUSDT?exchange=binance"
                    )
                self.assertEqual(response.status_code, expected_status)
                self.assertIn(expected_detail, response.json()["detail"])

    def test_grid_status_exposes_invalid_position_snapshot_instead_of_zero_delta(self):
        main._client.positions = [
            {"side": "Sell", "size": "invalid", "avgPrice": "100"}
        ]
        exchange = main._active_exchange
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "BADUSDT",
                "exchange": exchange,
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
            },
        )
        engine.running = True
        main._engines[main._engine_key(exchange, "BADUSDT")] = engine

        response = self.client.get("/api/grid/status/BADUSDT?exchange=binance")

        self.assertEqual(response.status_code, 200)
        status = response.json()
        self.assertFalse(status["position_snapshot_valid"])
        self.assertIn("position snapshot", status["position_snapshot_error"])
        self.assertIsNone(status["account_position_net_qty"])
        self.assertIsNone(status["position_delta_from_grid"])
        self.assertIsNone(status["account_unrealised_pnl"])

    def test_binance_compatible_position_adapters_preserve_symbol_identity(self):
        raw_position = {
            "symbol": "ANSEMUSDT",
            "positionAmt": "-2",
            "entryPrice": "0.4",
            "markPrice": "0.39",
            "unRealizedProfit": "0.02",
        }

        for client_class in (BinanceFuturesClient, AsterFuturesClient):
            with self.subTest(client=client_class.__name__):
                normalized = client_class._normalize_position(raw_position)
                self.assertEqual(normalized["symbol"], "ANSEMUSDT")
                self.assertEqual(normalized["side"], "Sell")
                self.assertEqual(normalized["size"], "2")

    def test_risk_endpoint_tracks_pending_submission_by_client_order_id(self):
        main._client = FakeClient("100", qty_step="0.1", min_qty="0.1")
        exchange = main._active_exchange
        link_id = "g_0_B_pending"
        main._client.place_order(
            symbol="PENDINGUSDT",
            side="Buy",
            qty="1.0",
            price="90",
            order_type="Limit",
            reduce_only=True,
            order_link_id=link_id,
        )
        main._client.positions = [{"side": "Sell", "size": "1.0", "avgPrice": "100"}]
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "PENDINGUSDT",
                "exchange": exchange,
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.running = True
        engine.grid_ready = True
        engine.grid_levels = [90, 110]
        engine.target_qty_by_level = {"0": 1.0}
        engine.grid_position_net_qty = -1.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"0": {"qty": 1.0, "entry_value": 100.0}}
        engine.active_orders = {
            link_id: engine._pending_limit_order_state(
                link_id=link_id,
                level_idx=0,
                side="Buy",
                price="90",
                qty="1.0",
                reduce_only=True,
                entry_price=100,
                time_in_force="GTC",
            )
        }
        main._engines[main._engine_key(exchange, "PENDINGUSDT")] = engine

        response = self.client.get(f"/api/risk/PENDINGUSDT?exchange={exchange}")

        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertEqual(snapshot["orphan_order_count"], 0)
        self.assertFalse(snapshot["unmanaged_position"])
        self.assertEqual(snapshot["pending_submission_count"], 1)
        self.assertEqual(snapshot["pending_submissions"][0]["order_link_id"], link_id)
        self.assertEqual(snapshot["reduce_protection"]["pending_submission_count"], 1)
        self.assertTrue(snapshot["has_risk"])

    def test_risk_endpoint_flags_persistently_queued_grid_replacement(self):
        exchange = main._active_exchange
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "QUEUEDUSDT",
                "exchange": exchange,
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.running = True
        engine.grid_ready = True
        engine.grid_levels = [90, 100, 110]
        engine.paused_replacements = [
            {
                "replacement_mode": "same_order",
                "replacement_source_link_id": "g_0_B_old",
                "side": "Buy",
                "price": "90",
                "qty": "1",
                "level_idx": 0,
                "reduce_only": False,
                "replacement_retry_attempts": 2,
                "replacement_retry_after": time.time() + 10,
            }
        ]
        main._engines[main._engine_key(exchange, "QUEUEDUSDT")] = engine

        response = self.client.get(f"/api/risk/QUEUEDUSDT?exchange={exchange}")

        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertTrue(snapshot["has_risk"])
        self.assertEqual(snapshot["queued_replacement_count"], 1)
        self.assertEqual(snapshot["queued_replacements"][0]["mode"], "same_order")
        self.assertEqual(snapshot["queued_replacements"][0]["attempts"], 2)

    def test_risk_endpoint_exposes_exchange_accepted_shape_mismatch(self):
        exchange = main._active_exchange
        engine = main.GridEngine(
            main._client,
            {
                "symbol": "SHAPEUSDT",
                "exchange": exchange,
                "direction": "short",
            },
        )
        engine.running = True
        engine.grid_ready = False
        engine.manual_stop_pending = True
        engine.active_orders = {
            "g_0_S_shape": {
                "link_id": "g_0_S_shape",
                "order_id": "accepted-70",
                "level_idx": 0,
                "side": "Sell",
                "price": "0.38",
                "qty": "70",
                "reduce_only": False,
                "accepted_shape_mismatch": "qty expected=100 actual=70",
                "expected_side": "Sell",
                "expected_price": "0.38",
                "expected_qty": "100",
                "expected_reduce_only": False,
                "exchange_accepted_side": "Sell",
                "exchange_accepted_price": "0.38",
                "exchange_accepted_qty": "70",
                "exchange_accepted_reduce_only": False,
            }
        }
        main._engines[main._engine_key(exchange, "SHAPEUSDT")] = engine

        response = self.client.get(f"/api/risk/SHAPEUSDT?exchange={exchange}")

        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertTrue(snapshot["has_risk"])
        self.assertEqual(snapshot["accepted_shape_mismatch_count"], 1)
        mismatch = snapshot["accepted_shape_mismatches"][0]
        self.assertEqual(mismatch["order_id"], "accepted-70")
        self.assertEqual(mismatch["expected_qty"], "100")
        self.assertEqual(mismatch["actual_qty"], "70")
        self.assertIn("expected=100 actual=70", mismatch["reason"])

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
            if os.name != "nt":
                self.assertEqual(Path(main.API_CONFIG_FILE).stat().st_mode & 0o777, 0o600)

        main.API_CONFIG_FILE = original_path

    def test_corrupt_api_config_is_preserved_and_blocks_credential_overwrite(self):
        corrupt_bytes = b'{"version": 2, "configs": {'
        Path(main.API_CONFIG_FILE).write_bytes(corrupt_bytes)
        replacement = {
            "exchange": "aster",
            "api_key": "0x0000000000000000000000000000000000000abc",
            "api_secret": "0x" + "1" * 64,
            "testnet": False,
        }

        with self.assertRaises(main.ApiConfigIntegrityError):
            main._save_api_config(replacement)

        self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), corrupt_bytes)
        response = self.client.post("/api/config", json=replacement)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), corrupt_bytes)
        config_status = self.client.get("/api/config").json()
        self.assertEqual(
            config_status["storage_error"],
            main._api_config_integrity_error,
        )

    def test_api_config_status_detects_corruption_after_successful_load(self):
        original = {
            "binance": {
                "exchange": "binance",
                "api_key": "runtime-binance-key",
                "api_secret": "runtime-binance-secret",
                "testnet": False,
            }
        }
        main._write_api_configs(original)
        self.assertEqual(set(main._load_file_api_configs()), {"binance"})
        corrupt_bytes = b'{"version": 2, "configs": {'
        Path(main.API_CONFIG_FILE).write_bytes(corrupt_bytes)

        response = self.client.get("/api/config")

        self.assertEqual(response.status_code, 200)
        self.assertIn("cannot be read safely", response.json()["storage_error"])
        self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), corrupt_bytes)

    def test_missing_loaded_api_config_blocks_partial_recreation(self):
        original = {
            "binance": {
                "exchange": "binance",
                "api_key": "missing-binance-key",
                "api_secret": "missing-binance-secret",
                "testnet": False,
            }
        }
        main._write_api_configs(original)
        Path(main.API_CONFIG_FILE).unlink()

        with self.assertRaises(main.ApiConfigIntegrityError):
            main._save_api_config(
                {
                    "exchange": "aster",
                    "api_key": "0x0000000000000000000000000000000000000abc",
                    "api_secret": "0x" + "4" * 64,
                    "testnet": False,
                }
            )

        self.assertFalse(Path(main.API_CONFIG_FILE).exists())
        self.assertIn("disappeared after it was loaded", main._api_config_integrity_error)

    def test_malformed_api_config_structures_are_never_overwritten(self):
        malformed_configs = [
            {},
            {"version": 2, "configs": {}},
            {
                "version": 99,
                "configs": {
                    "binance": {
                        "encrypted": False,
                        "exchange": "binance",
                        "api_key": "unsupported-version-key",
                        "api_secret": "unsupported-version-secret",
                        "testnet": False,
                    }
                },
            },
            {
                "version": 2,
                "configs": {
                    "binance": {
                        "encrypted": True,
                        "exchange": "binance",
                        "api_key": "ciphertext-without-secret",
                        "testnet": False,
                    }
                },
            },
            {
                "version": 2,
                "configs": {
                    "binance": {
                        "encrypted": "true",
                        "exchange": "binance",
                        "api_key": "not-valid-ciphertext",
                        "api_secret": "not-valid-ciphertext",
                        "testnet": False,
                    }
                },
            },
            {
                "version": 2,
                "configs": {
                    "binance": {
                        "encrypted": False,
                        "exchange": "aster",
                        "api_key": "mismatched-key",
                        "api_secret": "mismatched-secret",
                        "testnet": False,
                    }
                },
            },
            {
                "version": 2,
                "configs": {
                    "binance": {
                        "encrypted": False,
                        "exchange": "binance",
                        "api_key": "invalid-testnet-key",
                        "api_secret": "invalid-testnet-secret",
                        "testnet": "false",
                    }
                },
            },
        ]

        for malformed in malformed_configs:
            with self.subTest(malformed=malformed):
                original_bytes = json.dumps(malformed).encode("utf-8")
                Path(main.API_CONFIG_FILE).write_bytes(original_bytes)

                with self.assertRaises(main.ApiConfigIntegrityError):
                    main._save_api_config(
                        {
                            "exchange": "aster",
                            "api_key": "0x0000000000000000000000000000000000000abc",
                            "api_secret": "0x" + "5" * 64,
                            "testnet": False,
                        }
                    )

                self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), original_bytes)

    def test_environment_credentials_remain_available_when_file_is_corrupt(self):
        corrupt_bytes = b'{"version": 2, "configs": {'
        Path(main.API_CONFIG_FILE).write_bytes(corrupt_bytes)
        env = {
            "BINANCE_API_KEY": "fallback-binance-key",
            "BINANCE_API_SECRET": "fallback-binance-secret",
            "BINANCE_TESTNET": "false",
        }

        with patch.dict(os.environ, env, clear=False):
            loaded = main._load_api_configs()

        self.assertEqual(loaded["binance"]["api_key"], env["BINANCE_API_KEY"])
        self.assertEqual(loaded["binance"]["source"], "env")
        self.assertIn("cannot be read safely", main._api_config_integrity_error)
        self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), corrupt_bytes)

    def test_api_config_atomic_replace_failure_preserves_previous_credentials(self):
        original = {
            "binance": {
                "exchange": "binance",
                "api_key": "original-binance-key",
                "api_secret": "original-binance-secret",
                "testnet": False,
            }
        }
        main._write_api_configs(original)
        original_bytes = Path(main.API_CONFIG_FILE).read_bytes()
        replacement = {
            **original,
            "aster": {
                "exchange": "aster",
                "api_key": "0x0000000000000000000000000000000000000abc",
                "api_secret": "0x" + "2" * 64,
                "testnet": False,
            },
        }

        with (
            patch.object(
                main.os,
                "replace",
                side_effect=PermissionError("atomic replace denied"),
            ),
            self.assertRaises(PermissionError),
        ):
            main._write_api_configs(replacement)

        self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), original_bytes)
        self.assertIn("previous credential file", main._api_config_integrity_error)
        self.assertEqual(
            list(Path(main.API_CONFIG_FILE).parent.glob(".api_config.json.*.tmp")),
            [],
        )

        config_status = self.client.get("/api/config").json()
        self.assertIn("previous credential file", config_status["storage_error"])

        main._write_api_configs(original)
        self.assertEqual(self.client.get("/api/config").json()["storage_error"], "")

    def test_api_config_encryption_failure_preserves_previous_credentials(self):
        original = {
            "binance": {
                "exchange": "binance",
                "api_key": "encryption-binance-key",
                "api_secret": "encryption-binance-secret",
                "testnet": False,
            }
        }
        main._write_api_configs(original)
        original_bytes = Path(main.API_CONFIG_FILE).read_bytes()

        with (
            patch.object(
                main,
                "encrypt_text",
                side_effect=RuntimeError("encryption backend unavailable"),
            ),
            self.assertRaises(RuntimeError),
        ):
            main._write_api_configs(
                {
                    **original,
                    "aster": {
                        "exchange": "aster",
                        "api_key": "0x0000000000000000000000000000000000000abc",
                        "api_secret": "0x" + "6" * 64,
                        "testnet": False,
                    },
                }
            )

        self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), original_bytes)
        self.assertIn("previous credential file", main._api_config_write_error)
        self.assertEqual(
            list(Path(main.API_CONFIG_FILE).parent.glob(".api_config.json.*.tmp")),
            [],
        )

    def test_plaintext_multi_exchange_config_is_migrated_without_data_loss(self):
        plaintext = {
            "version": 2,
            "configs": {
                "binance": {
                    "exchange": "binance",
                    "api_key": "legacy-binance-key",
                    "api_secret": "legacy-binance-secret",
                    "testnet": False,
                },
                "aster": {
                    "exchange": "aster",
                    "api_key": "0x0000000000000000000000000000000000000abc",
                    "api_secret": "0x" + "7" * 64,
                    "testnet": False,
                },
            },
        }
        Path(main.API_CONFIG_FILE).write_text(
            json.dumps(plaintext),
            encoding="utf-8",
        )

        loaded = main._load_file_api_configs()
        saved_text = Path(main.API_CONFIG_FILE).read_text(encoding="utf-8")

        self.assertEqual(set(loaded), {"binance", "aster"})
        self.assertEqual(loaded["binance"]["api_secret"], "legacy-binance-secret")
        self.assertNotIn("legacy-binance-key", saved_text)
        self.assertNotIn("legacy-binance-secret", saved_text)
        self.assertIn('"encrypted": true', saved_text)

    def test_wrong_config_key_preserves_ciphertext_until_key_is_restored(self):
        original = {
            "binance": {
                "exchange": "binance",
                "api_key": "key-rotation-binance-key",
                "api_secret": "key-rotation-binance-secret",
                "testnet": False,
            }
        }
        main._write_api_configs(original)
        original_bytes = Path(main.API_CONFIG_FILE).read_bytes()
        correct_key = os.environ["GRID_CONFIG_KEY"]
        try:
            os.environ["GRID_CONFIG_KEY"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

            with self.assertRaises(main.ApiConfigIntegrityError):
                main._load_file_api_configs()

            self.assertEqual(Path(main.API_CONFIG_FILE).read_bytes(), original_bytes)
            self.assertIn("cannot be decrypted", main._api_config_read_error)
        finally:
            os.environ["GRID_CONFIG_KEY"] = correct_key

        loaded = main._load_file_api_configs()
        self.assertEqual(loaded["binance"]["api_secret"], "key-rotation-binance-secret")
        self.assertEqual(main._api_config_integrity_error, "")

    def test_concurrent_api_config_saves_preserve_every_exchange(self):
        configs = [
            {
                "exchange": "binance",
                "api_key": "concurrent-binance-key",
                "api_secret": "concurrent-binance-secret",
                "testnet": False,
            },
            {
                "exchange": "aster",
                "api_key": "0x0000000000000000000000000000000000000abc",
                "api_secret": "0x" + "3" * 64,
                "testnet": False,
            },
        ]

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(main._save_api_config, config) for config in configs]
            for future in futures:
                future.result(timeout=5)

        loaded = main._load_file_api_configs()
        self.assertEqual(set(loaded), {"binance", "aster"})
        self.assertEqual(loaded["binance"]["api_secret"], "concurrent-binance-secret")
        self.assertEqual(loaded["aster"]["api_secret"], "0x" + "3" * 64)
        saved_text = Path(main.API_CONFIG_FILE).read_text(encoding="utf-8")
        for config in configs:
            self.assertNotIn(config["api_key"], saved_text)
            self.assertNotIn(config["api_secret"], saved_text)

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

    def test_api_config_rejects_malformed_balance_snapshot_without_saving(self):
        class MalformedBalanceClient(FakeBinanceConfigClient):
            def get_balance(self):
                return {"retCode": 0, "result": {"list": []}}

        original_binance = main.BinanceFuturesClient
        original_client = main._clients["binance"]
        main.BinanceFuturesClient = MalformedBalanceClient
        try:
            response = self.client.post(
                "/api/config",
                json={
                    "exchange": "binance",
                    "api_key": "replacement-key",
                    "api_secret": "replacement-secret",
                    "testnet": True,
                },
            )

            self.assertEqual(response.status_code, 400)
            self.assertIn("balance snapshot", response.json()["detail"])
            self.assertIs(main._clients["binance"], original_client)
            self.assertFalse(Path(main.API_CONFIG_FILE).exists())
        finally:
            main.BinanceFuturesClient = original_binance

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
                "avgPrice": "100.25",
                "executedQty": "0.006",
                "cumQuote": "0.6015",
                "status": "CANCELED",
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
        trade_time = 1714012800000
        client._historical_asset_price_cache[("BNBUSDT", trade_time)] = Decimal(
            "600"
        )
        trade = client._normalize_trade(
            {
                "symbol": "TESTUSDT",
                "orderId": 123,
                "id": 456,
                "side": "BUY",
                "price": "100",
                "qty": "0.5",
                "quoteQty": "50",
                "commission": "0.001",
                "commissionAsset": "BNB",
                "maker": True,
                "time": trade_time,
            }
        )

        self.assertEqual(order["orderId"], "123")
        self.assertEqual(order["orderLinkId"], "g_1_B_abcdef")
        self.assertEqual(order["side"], "Buy")
        self.assertEqual(order["qty"], "0.01")
        self.assertEqual(order["avgPrice"], "100.25")
        self.assertEqual(order["executedQty"], "0.006")
        self.assertEqual(order["cumQuote"], "0.6015")
        self.assertEqual(order["orderStatus"], "CANCELED")
        self.assertEqual(position["side"], "Sell")
        self.assertEqual(position["size"], "0.25")
        self.assertEqual(position["avgPrice"], "105")
        self.assertEqual(trade["feeAsset"], "BNB")
        self.assertEqual(trade["feeUsdt"], "0.600")
        self.assertTrue(trade["isMaker"])

    def test_binance_instrument_info_preserves_market_lot_rules(self):
        client = BinanceFuturesClient("", "", True)

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            return {
                "symbols": [
                    {
                        "symbol": "TESTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {
                                "filterType": "LOT_SIZE",
                                "stepSize": "0.1",
                                "minQty": "0.1",
                                "maxQty": "1000",
                            },
                            {
                                "filterType": "MARKET_LOT_SIZE",
                                "stepSize": "0.1",
                                "minQty": "0.1",
                                "maxQty": "120",
                            },
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }
                ]
            }

        client._request = fake_request

        response = client.get_instrument_info("testusdt")
        info = response["result"]["list"][0]

        self.assertEqual(info["lotSizeFilter"]["maxOrderQty"], "1000")
        self.assertEqual(info["lotSizeFilter"]["minNotionalValue"], "5")
        self.assertEqual(info["marketLotSizeFilter"]["qtyStep"], "0.1")
        self.assertEqual(info["marketLotSizeFilter"]["minOrderQty"], "0.1")
        self.assertEqual(info["marketLotSizeFilter"]["maxOrderQty"], "120")

    def test_binance_missing_required_filters_never_become_default_rules(self):
        client = BinanceFuturesClient("", "", True)
        client._instrument_info_cache["TESTUSDT"] = (
            {
                "retCode": 0,
                "result": {"list": [{"symbol": "TESTUSDT"}]},
            },
            time.time(),
        )
        client._request = Mock(
            return_value={"symbols": [{"symbol": "TESTUSDT", "filters": []}]}
        )

        with self.assertRaisesRegex(RuntimeError, "instrument snapshot"):
            client.get_instrument_info("TESTUSDT")

        self.assertEqual(client._instrument_info_cache, {})

    def test_binance_balance_rejects_invalid_usdt_rows_without_fabricating_zero(self):
        valid = {
            "asset": "USDT",
            "availableBalance": "10",
            "balance": "12",
            "crossUnPnl": "-2",
        }
        cases = {
            "missing available": [
                {key: value for key, value in valid.items() if key != "availableBalance"}
            ],
            "missing wallet balance": [
                {key: value for key, value in valid.items() if key != "balance"}
            ],
            "missing pnl": [
                {key: value for key, value in valid.items() if key != "crossUnPnl"}
            ],
            "non-finite available": [{**valid, "availableBalance": "Infinity"}],
            "duplicate usdt": [valid, valid],
            "invalid row": [None],
        }

        for label, response in cases.items():
            with self.subTest(label=label):
                client = BinanceFuturesClient("", "", True)
                client._request = Mock(return_value=response)

                with self.assertRaisesRegex(RuntimeError, "balance snapshot"):
                    client.get_balance()

        client = BinanceFuturesClient("", "", True)
        client._request = Mock(return_value=[{"asset": "BNB"}])

        response = client.get_balance()

        self.assertEqual(response["result"]["list"][0]["coin"], [])

    def test_bybit_balance_validates_wallet_snapshot_before_returning(self):
        client = BybitClient("", "", True)
        client._request = Mock(
            return_value={"retCode": 0, "result": {"list": []}}
        )

        with self.assertRaisesRegex(RuntimeError, "balance snapshot"):
            client.get_balance()

        valid = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "availableToWithdraw": "",
                                "walletBalance": "12",
                                "equity": "11",
                                "unrealisedPnl": "-1",
                            }
                        ]
                    }
                ]
            },
        }
        client._request = Mock(return_value=valid)

        self.assertIs(client.get_balance(), valid)

    def test_binance_fee_rates_use_signed_endpoint_and_short_cache(self):
        client = BinanceFuturesClient("key", "secret", True)
        calls = []

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            calls.append((method, path, params, auth))
            return {
                "symbol": "BTCUSDT",
                "makerCommissionRate": "0.000200",
                "takerCommissionRate": "0.000500",
            }

        client._request = fake_request

        first = client.get_fee_rates("btcusdt")
        second = client.get_fee_rates("BTCUSDT")

        self.assertEqual(
            calls,
            [("GET", "/fapi/v1/commissionRate", {"symbol": "BTCUSDT"}, True)],
        )
        self.assertEqual(first["result"]["makerFeeRate"], "0.000200")
        self.assertEqual(first["result"]["takerFeeRate"], "0.000500")
        self.assertEqual(second["result"]["source"], "exchange_cache")

    def test_binance_fee_rates_reject_wrong_or_missing_symbol_without_cache(self):
        for label, returned_symbol in (("wrong", "OTHERUSDT"), ("missing", None)):
            with self.subTest(label=label):
                client = BinanceFuturesClient("key", "secret", True)
                response = {
                    "makerCommissionRate": "0.0002",
                    "takerCommissionRate": "0.0005",
                }
                if returned_symbol is not None:
                    response["symbol"] = returned_symbol
                client._request = Mock(return_value=response)

                with self.assertRaisesRegex(RuntimeError, "fee rate.*symbol"):
                    client.get_fee_rates("BTCUSDT")

                self.assertEqual(client._fee_rate_cache, {})

    def test_binance_ticker_rejects_wrong_premium_symbol(self):
        client = BinanceFuturesClient("key", "secret", True)

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            if path == "/fapi/v1/ticker/24hr":
                return {
                    "symbol": "BTCUSDT",
                    "lastPrice": "100",
                    "priceChangePercent": "1",
                }
            return {
                "symbol": "OTHERUSDT",
                "indexPrice": "99",
                "markPrice": "100",
            }

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "ticker.*symbol"):
            client.get_ticker("BTCUSDT")

    def test_binance_fee_rate_cache_expiry_requeries_exchange(self):
        client = BinanceFuturesClient("key", "secret", True)
        client.FEE_RATE_TTL_SECONDS = 0
        client._request = Mock(
            return_value={
                "symbol": "BTCUSDT",
                "makerCommissionRate": "0.0002",
                "takerCommissionRate": "0.0005",
            }
        )

        client.get_fee_rates("BTCUSDT")
        client.get_fee_rates("BTCUSDT")

        self.assertEqual(client._request.call_count, 2)

    def test_binance_get_order_by_link_uses_orig_client_order_id(self):
        client = BinanceFuturesClient("", "", True)
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return {
                "symbol": "TESTUSDT",
                "orderId": 99,
                "clientOrderId": "g_4_S_recover",
                "side": "SELL",
                "price": "101",
                "origQty": "2",
                "avgPrice": "0",
                "executedQty": "0",
                "cumQuote": "0",
                "status": "NEW",
                "reduceOnly": False,
                "timeInForce": "GTC",
                "type": "LIMIT",
                "time": 1714012800000,
            }

        client._request = fake_request

        response = client.get_order_by_link("testusdt", "g_4_S_recover")

        self.assertEqual(
            calls,
            [
                (
                    "GET",
                    "/fapi/v1/order",
                    {"symbol": "TESTUSDT", "origClientOrderId": "g_4_S_recover"},
                    True,
                )
            ],
        )
        self.assertEqual(response["result"]["orderId"], "99")
        self.assertEqual(response["result"]["orderLinkId"], "g_4_S_recover")

    def test_binance_order_trade_query_uses_exchange_maximum_page_size(self):
        client = BinanceFuturesClient("", "", True)
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return []

        client._request = fake_request

        response = client.get_order_trades("TESTUSDT", "99")

        self.assertEqual(response["retCode"], 0)
        self.assertEqual(calls[0][1], "/fapi/v1/userTrades")
        self.assertEqual(calls[0][2]["orderId"], "99")
        self.assertEqual(calls[0][2]["limit"], 1000)

    def test_binance_order_trades_paginate_without_losing_partial_fills(self):
        client = BinanceFuturesClient("", "", True)
        calls = []

        def trade(index):
            return {
                "symbol": "TESTUSDT",
                "orderId": 99,
                "id": index,
                "side": "BUY",
                "price": "100",
                "qty": "0.001",
                "quoteQty": "0.1",
                "commission": "0",
                "commissionAsset": "USDT",
                "realizedPnl": "0",
                "maker": True,
                "time": 1714012800000 + index,
            }

        def fake_request(method, path, *, params=None, auth=False):
            calls.append(dict(params or {}))
            if "fromId" not in (params or {}):
                return [trade(index) for index in range(1, 1001)]
            return [trade(1000), *[trade(index) for index in range(1001, 1031)]]

        client._request = fake_request

        response = client.get_order_trades("TESTUSDT", "99")

        self.assertEqual(len(response["result"]["list"]), 1030)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["fromId"], 1001)
        self.assertEqual(calls[1]["orderId"], "99")

    def test_binance_trade_pagination_fails_closed_when_it_cannot_finish(self):
        full_page = [{"id": index} for index in range(1000)]
        cases = {
            "cursor did not advance": (3, "did not advance"),
            "page cap reached": (1, "pagination exceeded"),
        }

        for label, (max_pages, message) in cases.items():
            with self.subTest(label=label):
                client = BinanceFuturesClient("", "", True)
                client.MAX_TRADE_HISTORY_PAGES = max_pages
                client._request = Mock(return_value=full_page)

                with self.assertRaisesRegex(RuntimeError, message):
                    client.get_order_trades("TESTUSDT", "99")

    def test_binance_order_trades_filters_other_orders_and_duplicate_trade_ids(self):
        client = BinanceFuturesClient("", "", True)
        requested = {
            "symbol": "TESTUSDT",
            "orderId": 99,
            "id": 1,
            "side": "BUY",
            "price": "100",
            "qty": "0.2",
            "quoteQty": "20",
            "commission": "0.01",
            "commissionAsset": "USDT",
            "maker": True,
            "time": 1714012800000,
        }
        client._request = Mock(
            return_value=[
                requested,
                {**requested, "orderId": 100, "id": 2},
                {
                    **requested,
                    "price": "100.0",
                    "qty": "0.20",
                    "quoteQty": "20.0",
                    "commission": "0.010",
                },
            ]
        )

        response = client.get_order_trades("TESTUSDT", "99")

        self.assertEqual(len(response["result"]["list"]), 1)
        self.assertEqual(response["result"]["list"][0]["orderId"], "99")
        self.assertEqual(response["result"]["list"][0]["tradeId"], "1")

    def test_binance_trade_history_rejects_wrong_symbols_and_conflicting_ids(self):
        base = {
            "symbol": "TESTUSDT",
            "orderId": 99,
            "id": 1,
            "side": "BUY",
            "price": "100",
            "qty": "0.2",
            "quoteQty": "20",
            "commission": "0.01",
            "commissionAsset": "USDT",
            "realizedPnl": "0",
            "maker": True,
            "time": 1714012800000,
        }
        cases = {
            "wrong symbol": [{**base, "symbol": "OTHERUSDT"}],
            "conflicting duplicate id": [base, {**base, "qty": "0.3", "quoteQty": "30"}],
        }

        for label, rows in cases.items():
            with self.subTest(label=label):
                client = BinanceFuturesClient("", "", True)
                client._request = Mock(return_value=rows)

                with self.assertRaisesRegex(RuntimeError, "execution snapshot"):
                    client.get_order_trades("TESTUSDT", "99")

    def test_binance_trade_normalizer_rejects_invalid_accounting_rows(self):
        base = {
            "symbol": "TESTUSDT",
            "orderId": 99,
            "id": 1,
            "side": "BUY",
            "price": "100",
            "qty": "0.2",
            "quoteQty": "20",
            "commission": "0.01",
            "commissionAsset": "USDT",
            "realizedPnl": "0",
            "maker": True,
            "time": 1714012800000,
        }
        mutations = {
            "missing symbol": lambda row: row.pop("symbol"),
            "missing order id": lambda row: row.pop("orderId"),
            "missing trade id": lambda row: row.pop("id"),
            "invalid side": lambda row: row.__setitem__("side", "HOLD"),
            "infinite price": lambda row: row.__setitem__("price", "Infinity"),
            "nan quantity": lambda row: row.__setitem__("qty", "NaN"),
            "zero volume": lambda row: row.__setitem__("quoteQty", "0"),
            "infinite fee": lambda row: row.__setitem__("commission", "Infinity"),
            "infinite pnl": lambda row: row.__setitem__("realizedPnl", "Infinity"),
            "missing fee asset": lambda row: row.pop("commissionAsset"),
            "invalid maker flag": lambda row: row.__setitem__("maker", "false"),
            "missing time": lambda row: row.pop("time"),
            "fractional time": lambda row: row.__setitem__("time", "1.5"),
            "invalid time": lambda row: row.__setitem__("time", "not-a-time"),
        }

        for label, mutate in mutations.items():
            with self.subTest(label=label):
                row = dict(base)
                mutate(row)
                client = BinanceFuturesClient("", "", True)

                with self.assertRaisesRegex(RuntimeError, "execution snapshot"):
                    client._normalize_trade(row)

    def test_binance_recent_trades_reject_malformed_rows(self):
        client = BinanceFuturesClient("", "", True)
        client._request = Mock(return_value=[None])

        with self.assertRaisesRegex(RuntimeError, "execution snapshot"):
            client.get_recent_trades("TESTUSDT")

    def test_binance_batch_orders_preserve_each_client_id_and_item_result(self):
        client = BinanceFuturesClient("", "", True)
        calls = []

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            calls.append((method, path, params, auth))
            return [
                {
                    "orderId": 101,
                    "clientOrderId": "g_0_B_batch",
                    "side": "BUY",
                    "price": "90",
                    "origQty": "1",
                    "status": "NEW",
                    "reduceOnly": True,
                },
                {"code": -2010, "msg": "order rejected"},
            ]

        client._request = fake_request
        response = client.place_orders(
            [
                {
                    "symbol": "TESTUSDT",
                    "side": "Buy",
                    "qty": "1",
                    "price": "90",
                    "order_type": "Limit",
                    "reduce_only": True,
                    "order_link_id": "g_0_B_batch",
                    "time_in_force": None,
                },
                {
                    "symbol": "TESTUSDT",
                    "side": "Sell",
                    "qty": "1",
                    "price": "110",
                    "order_type": "Limit",
                    "reduce_only": False,
                    "order_link_id": "g_1_S_batch",
                    "time_in_force": None,
                },
            ]
        )

        payload = json.loads(calls[0][2]["batchOrders"])
        self.assertEqual(calls[0][1], "/fapi/v1/batchOrders")
        self.assertEqual([item["newClientOrderId"] for item in payload], ["g_0_B_batch", "g_1_S_batch"])
        self.assertEqual([item["timeInForce"] for item in payload], ["GTC", "GTC"])
        self.assertEqual([item["reduceOnly"] for item in payload], ["true", "false"])
        self.assertEqual(response["result"]["list"][0]["retCode"], 0)
        self.assertEqual(response["result"]["list"][0]["result"]["orderLinkId"], "g_0_B_batch")
        self.assertEqual(response["result"]["list"][1]["retCode"], -2010)

    def test_binance_http_503_is_an_unknown_request_outcome(self):
        client = BinanceFuturesClient("key", "secret", True)
        response = Mock()
        response.status_code = 503
        response.json.return_value = {
            "code": -1007,
            "msg": "Timeout waiting for backend; execution status unknown",
        }
        response.text = "timeout"
        client.session.request = Mock(return_value=response)

        with self.assertRaises(ExchangeRequestUncertainError):
            client.place_order(
                symbol="TESTUSDT",
                side="Buy",
                qty="1",
                order_type="Market",
                order_link_id="init_B_unknown",
            )

    def test_binance_http_503_with_timestamp_code_is_never_retried(self):
        client = BinanceFuturesClient("key", "secret", True)
        response = Mock()
        response.status_code = 503
        response.json.return_value = {
            "code": -1021,
            "msg": "Timestamp for this request is outside of the recvWindow.",
        }
        response.text = "timestamp rejected after gateway failure"
        response.headers = {}
        client.session.request = Mock(return_value=response)

        with self.assertRaises(ExchangeRequestUncertainError):
            client.place_order(
                symbol="TESTUSDT",
                side="Buy",
                qty="1",
                order_type="Market",
                order_link_id="init_B_503_timestamp",
            )

        self.assertEqual(client.session.request.call_count, 1)

    def test_binance_timestamp_rejection_resigns_once_with_same_client_order_id(self):
        client = BinanceFuturesClient("key", "secret", True)
        calls = []
        signed_attempts = 0

        def response(status_code, data):
            item = Mock()
            item.status_code = status_code
            item.json.return_value = data
            item.text = str(data)
            item.headers = {}
            return item

        def request(method, url, **kwargs):
            nonlocal signed_attempts
            calls.append(
                {
                    "method": method,
                    "url": url,
                    "params": dict(kwargs.get("params") or {}),
                    "headers": kwargs.get("headers"),
                }
            )
            if url.endswith("/fapi/v1/time"):
                return response(
                    200,
                    {"serverTime": int(time.time() * 1000) + 7000},
                )
            signed_attempts += 1
            if signed_attempts == 1:
                return response(
                    400,
                    {
                        "code": -1021,
                        "msg": "Timestamp for this request is outside of the recvWindow.",
                    },
                )
            return response(
                200,
                {
                    "orderId": 987,
                    "clientOrderId": "g_0_B_time_sync",
                    "side": "BUY",
                    "origQty": "1",
                    "price": "90",
                    "status": "NEW",
                    "reduceOnly": False,
                },
            )

        client.session.request = request

        result = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="1",
            price="90",
            order_type="Limit",
            order_link_id="g_0_B_time_sync",
        )

        order_calls = [item for item in calls if item["url"].endswith("/fapi/v1/order")]
        time_calls = [item for item in calls if item["url"].endswith("/fapi/v1/time")]
        self.assertEqual(result["result"]["orderId"], "987")
        self.assertEqual(len(order_calls), 2)
        self.assertEqual(len(time_calls), 1)
        self.assertIsNone(time_calls[0]["headers"])
        self.assertEqual(
            order_calls[0]["params"]["newClientOrderId"],
            order_calls[1]["params"]["newClientOrderId"],
        )
        immutable_first = {
            key: value
            for key, value in order_calls[0]["params"].items()
            if key not in {"timestamp", "signature"}
        }
        immutable_retry = {
            key: value
            for key, value in order_calls[1]["params"].items()
            if key not in {"timestamp", "signature"}
        }
        self.assertEqual(immutable_first, immutable_retry)
        self.assertGreaterEqual(
            order_calls[1]["params"]["timestamp"]
            - order_calls[0]["params"]["timestamp"],
            6000,
        )
        self.assertNotEqual(
            order_calls[0]["params"]["signature"],
            order_calls[1]["params"]["signature"],
        )
        for item in order_calls:
            signed_params = dict(item["params"])
            signature = signed_params.pop("signature")
            self.assertEqual(signature, client._sign(signed_params))

    def test_binance_repeated_timestamp_rejection_does_not_loop(self):
        client = BinanceFuturesClient("key", "secret", True)
        calls = []

        def response(status_code, data):
            item = Mock()
            item.status_code = status_code
            item.json.return_value = data
            item.text = str(data)
            item.headers = {}
            return item

        def request(method, url, **kwargs):
            calls.append((method, url, dict(kwargs.get("params") or {})))
            if url.endswith("/fapi/v1/time"):
                return response(200, {"serverTime": int(time.time() * 1000) + 7000})
            return response(
                400,
                {
                    "code": -1021,
                    "msg": "Timestamp for this request is outside of the recvWindow.",
                },
            )

        client.session.request = request

        with self.assertRaisesRegex(RuntimeError, "outside of the recvWindow"):
            client.get_open_orders("TESTUSDT")

        self.assertEqual(len(calls), 3)
        self.assertEqual(
            len([item for item in calls if item[1].endswith("/fapi/v1/time")]),
            1,
        )

    def test_binance_timestamp_sync_failure_does_not_retry_mutation(self):
        client = BinanceFuturesClient("key", "secret", True)
        calls = []

        def response(status_code, data):
            item = Mock()
            item.status_code = status_code
            item.json.return_value = data
            item.text = str(data)
            item.headers = {}
            return item

        def request(method, url, **kwargs):
            calls.append((method, url, dict(kwargs.get("params") or {})))
            if url.endswith("/fapi/v1/time"):
                return response(500, {"code": -1000, "msg": "time service unavailable"})
            return response(
                400,
                {
                    "code": -1021,
                    "msg": "Timestamp for this request is outside of the recvWindow.",
                },
            )

        client.session.request = request

        with self.assertRaisesRegex(RuntimeError, "outside of the recvWindow"):
            client.place_order(
                symbol="TESTUSDT",
                side="Buy",
                qty="1",
                order_type="Market",
                order_link_id="init_B_no_time_retry",
            )

        self.assertEqual(len(calls), 2)
        self.assertEqual(
            len([item for item in calls if item[1].endswith("/fapi/v1/order")]),
            1,
        )

    def test_binance_concurrent_timestamp_rejections_share_one_time_sync(self):
        client = BinanceFuturesClient("key", "secret", True)
        first_attempt_barrier = threading.Barrier(2)
        local = threading.local()
        calls = []
        calls_lock = threading.Lock()

        def response(status_code, data):
            item = Mock()
            item.status_code = status_code
            item.json.return_value = data
            item.text = str(data)
            item.headers = {}
            return item

        def request(method, url, **kwargs):
            with calls_lock:
                calls.append((method, url, dict(kwargs.get("params") or {})))
            if url.endswith("/fapi/v1/time"):
                return response(200, {"serverTime": int(time.time() * 1000) + 7000})

            attempt = int(getattr(local, "signed_attempt", 0)) + 1
            local.signed_attempt = attempt
            if attempt == 1:
                first_attempt_barrier.wait(timeout=5)
                return response(
                    400,
                    {
                        "code": -1021,
                        "msg": "Timestamp for this request is outside of the recvWindow.",
                    },
                )
            return response(200, [])

        client.session.request = request

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda symbol: client.get_open_orders(symbol),
                    ("BTCUSDT", "ETHUSDT"),
                )
            )

        self.assertEqual(
            [result["result"]["list"] for result in results],
            [[], []],
        )
        self.assertEqual(
            len([item for item in calls if item[1].endswith("/fapi/v1/time")]),
            1,
        )
        self.assertEqual(
            len([item for item in calls if item[1].endswith("/fapi/v1/openOrders")]),
            4,
        )

    def test_binance_time_sync_rate_limit_stops_timestamp_retry(self):
        client = BinanceFuturesClient("key", "secret", True)
        calls = []

        def response(status_code, data, headers=None):
            item = Mock()
            item.status_code = status_code
            item.json.return_value = data
            item.text = str(data)
            item.headers = dict(headers or {})
            return item

        def request(method, url, **kwargs):
            calls.append((method, url, dict(kwargs.get("params") or {})))
            if url.endswith("/fapi/v1/time"):
                return response(
                    429,
                    {"code": -1003, "msg": "Too many requests"},
                    {"Retry-After": "7"},
                )
            return response(
                400,
                {
                    "code": -1021,
                    "msg": "Timestamp for this request is outside of the recvWindow.",
                },
            )

        client.session.request = request

        with self.assertRaises(ExchangeRateLimitError):
            client.get_open_orders("TESTUSDT")
        with self.assertRaises(ExchangeRateLimitError):
            client.get_open_orders("TESTUSDT")

        self.assertEqual(len(calls), 2)

    def test_bybit_get_order_by_link_falls_back_to_history(self):
        client = BybitClient("", "", True)
        calls = []

        def fake_request(method, path, *, params="", payload=None, auth=False):
            calls.append((method, path, params, auth))
            if path == "/v5/order/realtime":
                return {"retCode": 0, "result": {"list": []}}
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "TESTUSDT",
                            "orderId": "bybit-1",
                            "orderLinkId": "g_2_B_recover",
                            "side": "Buy",
                            "price": "100",
                            "qty": "1",
                            "avgPrice": "100",
                            "cumExecQty": "1",
                            "cumExecValue": "100",
                            "orderStatus": "Filled",
                            "reduceOnly": True,
                            "timeInForce": "GTC",
                            "orderType": "Limit",
                            "createdTime": "1714012800000",
                        }
                    ]
                },
            }

        client._request = fake_request

        response = client.get_order_by_link("TESTUSDT", "g_2_B_recover")

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][1], "/v5/order/realtime")
        self.assertIn("orderLinkId=g_2_B_recover", calls[0][2])
        self.assertEqual(calls[1][1], "/v5/order/history")
        self.assertIn("orderLinkId=g_2_B_recover", calls[1][2])
        self.assertEqual(response["result"]["orderId"], "bybit-1")

    def test_bybit_fee_rates_use_signed_endpoint_and_short_cache(self):
        client = BybitClient("key", "secret", True)
        calls = []

        def fake_request(method, path, *, params="", payload=None, auth=False):
            calls.append((method, path, params, auth))
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "makerFeeRate": "0.0001",
                            "takerFeeRate": "0.0006",
                        }
                    ]
                },
            }

        client._request = fake_request

        first = client.get_fee_rates("btcusdt")
        second = client.get_fee_rates("BTCUSDT")

        self.assertEqual(
            calls,
            [("GET", "/v5/account/fee-rate", "category=linear&symbol=BTCUSDT", True)],
        )
        self.assertEqual(first["result"]["makerFeeRate"], "0.0001")
        self.assertEqual(first["result"]["takerFeeRate"], "0.0006")
        self.assertEqual(second["result"]["source"], "exchange_cache")

    def test_bybit_fee_rates_reject_ambiguous_symbol_rows_without_cache(self):
        cases = {
            "wrong symbol": [
                {
                    "symbol": "OTHERUSDT",
                    "makerFeeRate": "0.0001",
                    "takerFeeRate": "0.0006",
                }
            ],
            "missing symbol": [
                {"makerFeeRate": "0.0001", "takerFeeRate": "0.0006"}
            ],
            "multiple rows": [
                {
                    "symbol": "BTCUSDT",
                    "makerFeeRate": "0.0001",
                    "takerFeeRate": "0.0006",
                },
                {
                    "symbol": "BTCUSDT",
                    "makerFeeRate": "0.0001",
                    "takerFeeRate": "0.0006",
                },
            ],
        }
        for label, rows in cases.items():
            with self.subTest(label=label):
                client = BybitClient("key", "secret", True)
                client._request = Mock(
                    return_value={"retCode": 0, "result": {"list": rows}}
                )

                with self.assertRaisesRegex(RuntimeError, "fee rate.*symbol"):
                    client.get_fee_rates("BTCUSDT")

                self.assertEqual(client._fee_rate_cache, {})

    def test_bybit_fee_rate_error_response_is_never_cached_as_success(self):
        client = BybitClient("key", "secret", True)
        client._request = Mock(
            return_value={
                "retCode": 10001,
                "retMsg": "invalid account",
                "result": {
                    "list": [{"makerFeeRate": "0", "takerFeeRate": "0"}],
                },
            }
        )

        with self.assertRaisesRegex(RuntimeError, "invalid account"):
            client.get_fee_rates("BTCUSDT")

        self.assertEqual(client._fee_rate_cache, {})

    def test_bybit_open_orders_follows_every_cursor_page(self):
        client = BybitClient("key", "secret")
        calls = []

        def order(index):
            return {
                "symbol": "MUUSDT",
                "orderId": str(index),
                "orderLinkId": f"g_{index}_B_page",
                "side": "Buy",
                "price": "100",
                "qty": "1",
                "avgPrice": "",
                "cumExecQty": "0",
                "cumExecValue": "0",
                "orderStatus": "New",
                "reduceOnly": False,
                "timeInForce": "GTC",
                "orderType": "Limit",
                "createdTime": str(1714012800000 + index),
            }

        def fake_request(method, path, *, params=None, payload=None, auth=False):
            calls.append(str(params or ""))
            if "cursor=" not in str(params or ""):
                return {
                    "retCode": 0,
                    "result": {
                        "list": [order(index) for index in range(50)],
                        "nextPageCursor": "page%3D2%26offset%3D50",
                    },
                }
            return {
                "retCode": 0,
                "result": {
                    "list": [order(index) for index in range(50, 75)],
                    "nextPageCursor": "",
                },
            }

        client._request = fake_request

        response = client.get_open_orders("MUUSDT")

        self.assertEqual(len(response["result"]["list"]), 75)
        self.assertEqual(len(calls), 2)
        self.assertIn("limit=50", calls[0])
        self.assertIn("cursor=page%3D2%26offset%3D50", calls[1])

    def test_bybit_http_503_is_an_unknown_request_outcome(self):
        client = BybitClient("key", "secret")
        response = Mock()
        response.status_code = 503
        response.json.return_value = {"retCode": 10016, "retMsg": "Server timeout"}
        response.text = "timeout"

        with patch("bybit_client.requests.post", return_value=response):
            with self.assertRaises(ExchangeRequestUncertainError):
                client.place_order(
                    symbol="TESTUSDT",
                    side="Buy",
                    qty="1",
                    order_type="Market",
                    order_link_id="init_B_unknown",
                )

    def test_bybit_non_json_http_503_is_an_unknown_request_outcome(self):
        client = BybitClient("key", "secret")
        response = Mock()
        response.status_code = 503
        response.json.side_effect = ValueError("invalid upstream body")
        response.text = "upstream unavailable"

        with patch("bybit_client.requests.post", return_value=response):
            with self.assertRaises(ExchangeRequestUncertainError):
                client.place_order(
                    symbol="TESTUSDT",
                    side="Buy",
                    qty="1",
                    order_type="Market",
                    order_link_id="init_B_non_json_503",
                )

    def test_bybit_timeout_ret_codes_are_unknown_request_outcomes(self):
        for ret_code in (10000, 10016):
            with self.subTest(ret_code=ret_code):
                client = BybitClient("key", "secret")
                response = Mock()
                response.status_code = 200
                response.json.return_value = {
                    "retCode": ret_code,
                    "retMsg": "Server timeout",
                }
                response.text = "Server timeout"

                with patch("bybit_client.requests.post", return_value=response):
                    with self.assertRaises(ExchangeRequestUncertainError):
                        client.place_order(
                            symbol="TESTUSDT",
                            side="Buy",
                            qty="1",
                            order_type="Market",
                            order_link_id=f"init_B_{ret_code}",
                        )

    def test_bybit_http_200_invalid_order_response_is_unknown(self):
        for response_kind in ("invalid_json", "wrong_json_shape"):
            with self.subTest(response_kind=response_kind):
                client = BybitClient("key", "secret")
                response = Mock()
                response.status_code = 200
                response.text = "truncated acknowledgement"
                response.headers = {}
                if response_kind == "invalid_json":
                    response.json.side_effect = ValueError("invalid json")
                else:
                    response.json.return_value = []

                with patch("bybit_client.requests.post", return_value=response):
                    with self.assertRaises(ExchangeRequestUncertainError):
                        client.place_order(
                            symbol="TESTUSDT",
                            side="Buy",
                            qty="1",
                            order_type="Market",
                            order_link_id=f"init_B_{response_kind}",
                        )

    def test_bybit_http_200_invalid_query_response_is_read_error(self):
        for response_kind in ("invalid_json", "wrong_json_shape"):
            with self.subTest(response_kind=response_kind):
                client = BybitClient("key", "secret")
                response = Mock()
                response.status_code = 200
                response.text = "truncated query response"
                response.headers = {}
                if response_kind == "invalid_json":
                    response.json.side_effect = ValueError("invalid json")
                else:
                    response.json.return_value = []

                with patch("bybit_client.requests.get", return_value=response):
                    with self.assertRaises(RuntimeError) as error:
                        client.get_ticker("TESTUSDT")

                self.assertNotIsInstance(
                    error.exception,
                    ExchangeRequestUncertainError,
                )

    def test_binance_http_200_invalid_single_order_response_is_unknown(self):
        for response_kind in ("invalid_json", "wrong_json_shape"):
            with self.subTest(response_kind=response_kind):
                client = BinanceFuturesClient("key", "secret", True)
                response = Mock()
                response.status_code = 200
                response.text = "truncated acknowledgement"
                response.headers = {}
                if response_kind == "invalid_json":
                    response.json.side_effect = ValueError("invalid json")
                else:
                    response.json.return_value = []
                client.session.request = Mock(return_value=response)

                with self.assertRaises(ExchangeRequestUncertainError):
                    client.place_order(
                        symbol="TESTUSDT",
                        side="Buy",
                        qty="1",
                        price="90",
                        order_type="Limit",
                        order_link_id=f"g_0_B_{response_kind}",
                    )

    def test_binance_http_200_invalid_batch_order_response_is_unknown(self):
        for response_kind in ("invalid_json", "wrong_json_shape"):
            with self.subTest(response_kind=response_kind):
                client = BinanceFuturesClient("key", "secret", True)
                response = Mock()
                response.status_code = 200
                response.text = "truncated batch acknowledgement"
                response.headers = {}
                if response_kind == "invalid_json":
                    response.json.side_effect = ValueError("invalid json")
                else:
                    response.json.return_value = {"unexpected": True}
                client.session.request = Mock(return_value=response)

                with self.assertRaises(ExchangeRequestUncertainError):
                    client.place_orders(
                        [
                            {
                                "symbol": "TESTUSDT",
                                "side": "Buy",
                                "qty": "1",
                                "price": "90",
                                "order_type": "Limit",
                                "reduce_only": False,
                                "order_link_id": f"g_0_B_batch_{response_kind}",
                                "time_in_force": None,
                            }
                        ]
                    )

    def test_binance_http_200_invalid_order_lookup_is_read_error(self):
        for response_kind in ("invalid_json", "wrong_json_shape"):
            with self.subTest(response_kind=response_kind):
                client = BinanceFuturesClient("key", "secret", True)
                response = Mock()
                response.status_code = 200
                response.text = "truncated order lookup"
                response.headers = {}
                if response_kind == "invalid_json":
                    response.json.side_effect = ValueError("invalid json")
                else:
                    response.json.return_value = []
                client.session.request = Mock(return_value=response)

                with self.assertRaises(RuntimeError) as error:
                    client.get_order_by_link("TESTUSDT", "g_0_B_lookup")

                self.assertNotIsInstance(
                    error.exception,
                    ExchangeRequestUncertainError,
                )

    def test_binance_http_200_invalid_cancel_response_is_unknown(self):
        for response_kind in ("invalid_json", "wrong_json_shape"):
            with self.subTest(response_kind=response_kind):
                client = BinanceFuturesClient("key", "secret", True)
                response = Mock()
                response.status_code = 200
                response.text = "truncated cancellation acknowledgement"
                response.headers = {}
                if response_kind == "invalid_json":
                    response.json.side_effect = ValueError("invalid json")
                else:
                    response.json.return_value = []
                client.session.request = Mock(return_value=response)

                with self.assertRaises(ExchangeRequestUncertainError):
                    client.cancel_order("TESTUSDT", "123")

    def test_binance_cancel_acknowledgement_must_match_requested_order(self):
        client = BinanceFuturesClient("key", "secret", True)
        response = Mock()
        response.status_code = 200
        response.text = "cancel acknowledgement"
        response.headers = {}
        response.json.return_value = {
            "symbol": "TESTUSDT",
            "orderId": 999,
            "clientOrderId": "g_0_S_cancel",
            "side": "SELL",
            "price": "101",
            "origQty": "1",
            "avgPrice": "0",
            "executedQty": "0",
            "cumQuote": "0",
            "status": "CANCELED",
            "reduceOnly": False,
            "timeInForce": "GTC",
            "type": "LIMIT",
        }
        client.session.request = Mock(return_value=response)

        with self.assertRaises(ExchangeRequestUncertainError):
            client.cancel_order("TESTUSDT", "123")

    def test_binance_cancel_acknowledgement_accepts_official_shape(self):
        client = BinanceFuturesClient("key", "secret", True)
        response = Mock()
        response.status_code = 200
        response.text = "cancel acknowledgement"
        response.headers = {}
        response.json.return_value = {
            "symbol": "TESTUSDT",
            "orderId": 123,
            "clientOrderId": "g_0_S_cancel",
            "side": "SELL",
            "price": "101",
            "origQty": "1",
            "avgPrice": "101",
            "executedQty": "0.4",
            "cumQuote": "40.4",
            "status": "CANCELED",
            "reduceOnly": False,
            "timeInForce": "GTC",
            "type": "LIMIT",
        }
        client.session.request = Mock(return_value=response)

        result = client.cancel_order("TESTUSDT", "123")

        self.assertEqual(result["result"]["orderId"], "123")
        self.assertEqual(result["result"]["orderStatus"], "CANCELED")
        self.assertEqual(result["result"]["executedQty"], "0.4")

    def test_bybit_cancel_acknowledgement_must_match_requested_order(self):
        client = BybitClient("key", "secret")
        response = Mock()
        response.status_code = 200
        response.text = "cancel acknowledgement"
        response.headers = {}
        response.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"orderId": "wrong-order", "orderLinkId": ""},
        }

        with patch("bybit_client.requests.post", return_value=response):
            with self.assertRaises(ExchangeRequestUncertainError):
                client.cancel_order("TESTUSDT", "requested-order")

    def test_bybit_cancel_acknowledgement_accepts_official_shape(self):
        client = BybitClient("key", "secret")
        response = Mock()
        response.status_code = 200
        response.text = "cancel acknowledgement"
        response.headers = {}
        response.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": "requested-order",
                "orderLinkId": "g_0_S_cancel",
            },
        }

        with patch("bybit_client.requests.post", return_value=response):
            result = client.cancel_order("TESTUSDT", "requested-order")

        self.assertEqual(result["result"]["orderId"], "requested-order")
        self.assertEqual(result["result"]["orderLinkId"], "g_0_S_cancel")

    def test_bybit_order_trades_follows_cursor_and_normalizes_all_executions(self):
        client = BybitClient("key", "secret")
        calls = []

        def trade(index):
            return {
                "symbol": "MUUSDT",
                "side": "Buy",
                "execQty": "0.1",
                "execPrice": "100",
                "execValue": "10",
                "execFee": "0.005",
                "feeCurrency": "USDT",
                "isMaker": False,
                "orderId": "order-1",
                "execId": str(index),
                "execTime": str(1714012800000 + index),
            }

        def fake_request(method, path, *, params=None, payload=None, auth=False):
            calls.append(str(params or ""))
            if "cursor=" not in str(params or ""):
                return {
                    "retCode": 0,
                    "result": {
                        "list": [trade(index) for index in range(100)],
                        "nextPageCursor": "next%3A100",
                    },
                }
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        trade(99),
                        *[trade(index) for index in range(100, 130)],
                        {**trade(130), "orderId": "other-order"},
                    ],
                    "nextPageCursor": "",
                },
            }

        client._request = fake_request

        response = client.get_order_trades("MUUSDT", "order-1")

        self.assertEqual(len(response["result"]["list"]), 130)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(item["qty"] == "0.1" for item in response["result"]["list"]))

    def test_bybit_trade_history_rejects_wrong_symbols_and_conflicting_ids(self):
        base = {
            "symbol": "MUUSDT",
            "orderId": "order-1",
            "execId": "exec-1",
            "side": "Buy",
            "execPrice": "100",
            "execQty": "0.2",
            "execValue": "20",
            "execFee": "0.01",
            "feeCurrency": "USDT",
            "execPnl": "0",
            "isMaker": False,
            "execTime": "1714012800000",
        }
        cases = {
            "wrong symbol": [{**base, "symbol": "OTHERUSDT"}],
            "conflicting duplicate id": [base, {**base, "execQty": "0.3", "execValue": "30"}],
        }

        for label, rows in cases.items():
            with self.subTest(label=label):
                client = BybitClient("", "", True)
                client._get_paginated = Mock(
                    return_value={"retCode": 0, "result": {"list": rows}}
                )

                with self.assertRaisesRegex(RuntimeError, "execution snapshot"):
                    client.get_order_trades("MUUSDT", "order-1")

    def test_bybit_trade_normalizer_rejects_invalid_accounting_rows(self):
        base = {
            "symbol": "MUUSDT",
            "orderId": "order-1",
            "execId": "exec-1",
            "side": "Buy",
            "execPrice": "100",
            "execQty": "0.2",
            "execValue": "20",
            "execFee": "0.01",
            "feeCurrency": "USDT",
            "execPnl": "0",
            "isMaker": False,
            "execTime": "1714012800000",
        }
        mutations = {
            "missing symbol": lambda row: row.pop("symbol"),
            "missing order id": lambda row: row.pop("orderId"),
            "missing trade id": lambda row: row.pop("execId"),
            "invalid side": lambda row: row.__setitem__("side", "HOLD"),
            "infinite price": lambda row: row.__setitem__("execPrice", "Infinity"),
            "nan quantity": lambda row: row.__setitem__("execQty", "NaN"),
            "zero volume": lambda row: row.__setitem__("execValue", "0"),
            "infinite fee": lambda row: row.__setitem__("execFee", "Infinity"),
            "infinite pnl": lambda row: row.__setitem__("execPnl", "Infinity"),
            "invalid fee asset": lambda row: row.__setitem__("feeCurrency", True),
            "invalid maker flag": lambda row: row.__setitem__("isMaker", "maybe"),
            "missing time": lambda row: row.pop("execTime"),
            "fractional time": lambda row: row.__setitem__("execTime", "1.5"),
            "invalid time": lambda row: row.__setitem__("execTime", "not-a-time"),
        }

        for label, mutate in mutations.items():
            with self.subTest(label=label):
                row = dict(base)
                mutate(row)
                client = BybitClient("", "", True)

                with self.assertRaisesRegex(RuntimeError, "execution snapshot"):
                    client._normalize_trade(row)

    def test_bybit_recent_trades_reject_malformed_rows(self):
        client = BybitClient("", "", True)
        client._request = Mock(
            return_value={"retCode": 0, "result": {"list": [None]}}
        )

        with self.assertRaisesRegex(RuntimeError, "execution snapshot"):
            client.get_recent_trades("MUUSDT")

    def test_bybit_repeated_pagination_cursor_fails_closed(self):
        client = BybitClient("key", "secret")
        client._request = Mock(
            return_value={
                "retCode": 0,
                "result": {"list": [], "nextPageCursor": "same-cursor"},
            }
        )

        with self.assertRaisesRegex(RuntimeError, "cursor did not advance"):
            client.get_open_orders("MUUSDT")

        self.assertEqual(client._request.call_count, 2)

    def test_bybit_pagination_page_limit_fails_closed(self):
        client = BybitClient("key", "secret")
        client.MAX_PAGINATION_PAGES = 2
        calls = []

        def fake_request(method, path, *, params=None, payload=None, auth=False):
            calls.append(str(params or ""))
            return {
                "retCode": 0,
                "result": {
                    "list": [{"orderId": str(len(calls))}],
                    "nextPageCursor": f"page-{len(calls) + 1}",
                },
            }

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "pagination exceeded 2 pages"):
            client.get_open_orders("MUUSDT")

        self.assertEqual(len(calls), 2)

    def test_binance_fee_asset_price_cache_expires(self):
        client = BinanceFuturesClient("", "", True)
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return {"symbol": "BNBUSDT", "price": "700"}

        client._request = fake_request
        client._asset_price_cache["BNBUSDT"] = (Decimal("600"), time.time() - 61)

        fee_usdt = client._fee_to_usdt(Decimal("0.001"), "BNB")

        self.assertEqual(fee_usdt, Decimal("0.700"))
        self.assertEqual(len(calls), 1)

    def test_binance_fee_asset_prices_require_identity_and_finite_values(self):
        current_cases = {
            "missing symbol": {"price": "700"},
            "wrong symbol": {"symbol": "OTHERUSDT", "price": "700"},
            "infinite price": {"symbol": "BNBUSDT", "price": "Infinity"},
            "overflowing price": {"symbol": "BNBUSDT", "price": "1e1000000"},
        }
        for label, ticker in current_cases.items():
            with self.subTest(kind="current", label=label):
                client = BinanceFuturesClient("", "", True)
                client._asset_price_cache["BNBUSDT"] = (
                    Decimal("Infinity"),
                    time.time(),
                )
                client._request = Mock(return_value=ticker)

                fee, source = client._fee_to_usdt_with_source(
                    Decimal("0.001"),
                    "BNB",
                )

                self.assertIsNone(fee)
                self.assertEqual(source, "current_price_unavailable")
                self.assertEqual(client._asset_price_cache, {})

        client = BinanceFuturesClient("", "", True)
        minute_start = 1714012800000
        client._request = Mock(
            return_value=[[minute_start, "Infinity", "1", "1", "1"]]
        )
        client._historical_asset_price_cache[("BNBUSDT", minute_start)] = Decimal(
            "Infinity"
        )

        fee, source = client._fee_to_usdt_with_source(
            Decimal("0.001"),
            "BNB",
            trade_time_ms=minute_start,
        )

        self.assertIsNone(fee)
        self.assertEqual(source, "historical_price_unavailable")
        self.assertEqual(client._historical_asset_price_cache, {})

    def test_binance_fee_conversion_uses_execution_minute_open_and_cache(self):
        client = BinanceFuturesClient("", "", True)
        calls = []
        trade_time = 1714012800123
        minute_start = trade_time - (trade_time % 60_000)

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            calls.append((method, path, params, auth, api_key))
            return [[minute_start, "567.00", "568", "566", "567.50"]]

        client._request = fake_request
        trade = {
            "symbol": "TESTUSDT",
            "orderId": 1,
            "id": 2,
            "side": "SELL",
            "price": "943.33",
            "qty": "0.2",
            "quoteQty": "188.666",
            "commission": "0.00011988",
            "commissionAsset": "BNB",
            "maker": False,
            "time": trade_time,
        }

        first = client._normalize_trade(trade)
        second = client._normalize_trade({**trade, "id": 3, "time": trade_time + 1000})

        self.assertEqual(first["feeUsdt"], "0.0679719600")
        self.assertEqual(first["feeUsdtSource"], "historical_minute_open")
        self.assertEqual(second["feeUsdt"], "0.0679719600")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1], "/fapi/v1/klines")
        self.assertEqual(calls[0][2]["startTime"], minute_start)

    def test_binance_historical_fee_conversion_never_uses_current_price_fallback(self):
        client = BinanceFuturesClient("", "", True)
        client._asset_price_cache["BNBUSDT"] = (Decimal("900"), time.time())

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            raise RuntimeError("historical market data unavailable")

        client._request = fake_request
        trade = client._normalize_trade(
            {
                "symbol": "TESTUSDT",
                "orderId": 1,
                "id": 2,
                "side": "SELL",
                "price": "100",
                "qty": "1",
                "quoteQty": "100",
                "commission": "0.001",
                "commissionAsset": "BNB",
                "maker": False,
                "time": 1714012800123,
            }
        )

        self.assertEqual(trade["feeUsdt"], "")
        self.assertEqual(trade["feeUsdtSource"], "historical_price_unavailable")

    def test_historical_fee_price_caches_are_bounded(self):
        client = BinanceFuturesClient("", "", True)
        client.HISTORICAL_ASSET_PRICE_CACHE_MAX_ITEMS = 2

        def fake_request(method, path, *, params=None, auth=False, api_key=False):
            minute_start = int(params["startTime"])
            return [[minute_start, "600", "600", "600", "600"]]

        client._request = fake_request
        start = 1714012800000
        for offset in range(3):
            client._fee_to_usdt(
                Decimal("0.001"),
                "BNB",
                trade_time_ms=start + offset * 60_000,
            )

        self.assertEqual(len(client._historical_asset_price_cache), 2)
        self.assertNotIn(
            ("BNBUSDT", start),
            client._historical_asset_price_cache,
        )

    def test_bybit_fee_conversion_uses_execution_minute_open(self):
        client = BybitClient("", "", True)
        calls = []
        trade_time = 1714012800123
        minute_start = trade_time - (trade_time % 60_000)

        def fake_request(method, path, *, params=None, payload=None, auth=False):
            calls.append((method, path, params, auth))
            return {
                "retCode": 0,
                "result": {
                    "list": [[str(minute_start), "600", "601", "599", "600"]]
                },
            }

        client._request = fake_request
        trade = client._normalize_trade(
            {
                "symbol": "MUUSDT",
                "orderId": "1",
                "execId": "2",
                "side": "Sell",
                "execPrice": "100",
                "execQty": "1",
                "execValue": "100",
                "execFee": "0.001",
                "feeCurrency": "BNB",
                "isMaker": False,
                "execTime": str(trade_time),
            }
        )

        self.assertEqual(trade["feeUsdt"], "0.600")
        self.assertEqual(trade["feeUsdtSource"], "historical_minute_open")
        self.assertEqual(calls[0][1], "/v5/market/kline")

    def test_bybit_fee_asset_prices_require_identity_and_finite_values(self):
        current_cases = {
            "wrong symbol": {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "OTHERUSDT", "lastPrice": "700", "markPrice": "700"}
                    ]
                },
            },
            "multiple rows": {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "BNBUSDT", "lastPrice": "700", "markPrice": "700"},
                        {"symbol": "BNBUSDT", "lastPrice": "700", "markPrice": "700"},
                    ]
                },
            },
            "infinite price": {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "BNBUSDT",
                            "lastPrice": "Infinity",
                            "markPrice": "700",
                        }
                    ]
                },
            },
        }
        for label, ticker in current_cases.items():
            with self.subTest(kind="current", label=label):
                client = BybitClient("", "", True)
                client._asset_price_cache["BNBUSDT"] = (
                    Decimal("Infinity"),
                    time.time(),
                )
                client.get_ticker = Mock(return_value=ticker)

                fee, source = client._fee_to_usdt_with_source(
                    Decimal("0.001"),
                    "BNB",
                )

                self.assertIsNone(fee)
                self.assertEqual(source, "current_price_unavailable")
                self.assertEqual(client._asset_price_cache, {})

        client = BybitClient("", "", True)
        minute_start = 1714012800000
        client._request = Mock(
            return_value={
                "retCode": 0,
                "result": {
                    "list": [[str(minute_start), "Infinity", "1", "1", "1"]]
                },
            }
        )
        client._historical_asset_price_cache[("BNBUSDT", minute_start)] = Decimal(
            "Infinity"
        )

        fee, source = client._fee_to_usdt_with_source(
            Decimal("0.001"),
            "BNB",
            trade_time_ms=minute_start,
        )

        self.assertIsNone(fee)
        self.assertEqual(source, "historical_price_unavailable")
        self.assertEqual(client._historical_asset_price_cache, {})

    def test_history_records_realized_and_unrealized_profit_separately(self):
        client = FakeClient("99", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = main.GridEngine(
            client,
            {
                "run_id": "profit-audit",
                "exchange": "binance",
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.current_price = 99
        engine.grid_position_net_qty = -1
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 1.0, "entry_value": 101.0}
        }
        engine.gross_profit = 0.6
        engine.total_fee = 0.1
        engine.total_profit = 0.5
        engine.filled_count = 250

        record = main._history_record_from_engine(engine, "stopped")

        self.assertEqual(record["realized_net_profit"], 0.5)
        self.assertEqual(record["unrealised_pnl"], 2.0)
        self.assertEqual(record["total_equity_profit"], 2.5)
        self.assertEqual(record["net_profit"], 0.5)
        self.assertEqual(record["grid_position_net_qty"], -1.0)
        self.assertEqual(record["filled_count"], 250)

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

    def test_grid_preview_rejects_round_trip_below_exchange_min_notional(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("ANSEMUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.26,
                "upper_price": 0.30,
                "grid_count": 20,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 10,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("minimum", response.json()["detail"])
        self.assertIn("notional", response.json()["detail"])
        self.assertEqual(client.orders, [])

    def test_grid_start_rejects_min_notional_before_opening_position(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("ANSEMUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.26,
                "upper_price": 0.30,
                "grid_count": 20,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 10,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/start", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("minimum notional", response.json()["detail"])
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])
        self.assertIsNone(main._get_engine("binance", "ANSEMUSDT"))

    def test_grid_preview_reports_exchange_min_notional_for_valid_quantity(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("ANSEMUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.26,
                "upper_price": 0.30,
                "grid_count": 20,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 20,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["min_notional"], 5)

    def test_short_min_notional_uses_actual_upper_open_price(self):
        client = FakeClient(
            "0.25",
            tick_size="0.01",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("EDGEUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.24,
                "upper_price": 0.28,
                "grid_count": 2,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 20,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["qty_per_grid_min"], 20)

    def test_preview_rejects_initial_market_notional_before_order_submission(self):
        client = FakeClient(
            "0.241",
            tick_size="0.001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("EDGEUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.24,
                "upper_price": 0.28,
                "grid_count": 2,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 20,
                "total_investment": 0,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("Initial opening notional", response.json()["detail"])
        self.assertEqual(client.orders, [])

    def test_preview_rechecks_initial_notional_after_market_step_rounding(self):
        client = FakeClient(
            "0.29",
            tick_size="0.001",
            qty_step="0.1",
            min_qty="0.1",
            min_notional="5",
            market_qty_step="1",
            market_min_qty="1",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("ROUNDUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.28,
                "upper_price": 0.34,
                "grid_count": 2,
                "position_sizing_mode": "investment",
                "total_investment": 2.51,
                "leverage": 2,
                "initial_order_type": "market",
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("Initial opening notional 4.93", response.json()["detail"])
        self.assertEqual(client.orders, [])

    def test_preview_rechecks_initial_notional_after_limit_tick_rounding(self):
        client = FakeClient(
            "0.30",
            tick_size="0.01",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        main._client = client
        main._clients["binance"] = client
        payload = self._payload("TICKUSDT")
        payload.update(
            {
                "direction": "short",
                "lower_price": 0.28,
                "upper_price": 0.34,
                "grid_count": 2,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 17,
                "total_investment": 0,
                "initial_order_type": "limit",
                "initial_order_price": 0.2999,
            }
        )

        response = self.client.post("/api/grid/preview", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertIn("Initial opening notional 4.93", response.json()["detail"])
        self.assertEqual(client.orders, [])

    def test_binance_rate_limit_uses_local_cooldown(self):
        client = BinanceFuturesClient("key", "secret", True)
        response = Mock()
        response.status_code = 429
        response.json.return_value = {"code": -1015, "msg": "Too many new orders"}
        response.text = "Too many new orders"
        response.headers = {"Retry-After": "10"}
        client.session = Mock()
        client.session.request.return_value = response

        with self.assertRaises(ExchangeRateLimitError):
            client.get_ticker("BTCUSDT")
        with self.assertRaises(ExchangeRateLimitError):
            client.get_ticker("BTCUSDT")

        self.assertEqual(client.session.request.call_count, 1)

    def test_binance_http_418_ip_ban_uses_local_cooldown(self):
        client = BinanceFuturesClient("key", "secret", True)
        response = Mock()
        response.status_code = 418
        response.json.return_value = {
            "code": -1003,
            "msg": "Way too much request weight used; IP banned until 1783839999000.",
        }
        response.text = "IP banned"
        response.headers = {"Retry-After": "90"}
        client.session = Mock()
        client.session.request.return_value = response

        with self.assertRaises(ExchangeRateLimitError) as first:
            client.get_ticker("BTCUSDT")
        with self.assertRaises(ExchangeRateLimitError):
            client.get_ticker("BTCUSDT")

        self.assertGreaterEqual(first.exception.retry_after, 90)
        self.assertEqual(client.session.request.call_count, 1)

    def test_bybit_rate_limit_uses_local_cooldown(self):
        client = BybitClient("key", "secret", True)
        response = Mock()
        response.status_code = 429
        response.json.return_value = {"retCode": 10006, "retMsg": "Too many requests"}
        response.headers = {"Retry-After": "10"}

        with patch("bybit_client.requests.get", return_value=response) as request:
            with self.assertRaises(ExchangeRateLimitError):
                client.get_ticker("BTCUSDT")
            with self.assertRaises(ExchangeRateLimitError):
                client.get_ticker("BTCUSDT")

        self.assertEqual(request.call_count, 1)

    def test_bybit_non_json_http_429_uses_local_cooldown(self):
        client = BybitClient("key", "secret", True)
        response = Mock()
        response.status_code = 429
        response.json.side_effect = ValueError("invalid rate-limit body")
        response.text = "rate limited"
        response.headers = {}

        with patch("bybit_client.requests.get", return_value=response) as request:
            with self.assertRaises(ExchangeRateLimitError):
                client.get_ticker("BTCUSDT")
            with self.assertRaises(ExchangeRateLimitError):
                client.get_ticker("BTCUSDT")

        self.assertEqual(request.call_count, 1)

    def test_bybit_ret_code_429_uses_local_cooldown(self):
        client = BybitClient("key", "secret", True)
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "retCode": 429,
            "retMsg": "The trading service is experiencing a high server load. Please retry",
        }
        response.text = "high server load"
        response.headers = {}

        with patch("bybit_client.requests.get", return_value=response) as request:
            with self.assertRaises(ExchangeRateLimitError):
                client.get_ticker("BTCUSDT")
            with self.assertRaises(ExchangeRateLimitError):
                client.get_ticker("BTCUSDT")

        self.assertEqual(request.call_count, 1)

    def test_bybit_http_403_access_too_frequent_uses_ten_minute_ip_cooldown(self):
        client = BybitClient("key", "secret", True)
        response = Mock()
        response.status_code = 403
        response.text = "access too frequent"
        response.headers = {}
        response.json.side_effect = ValueError("non-json ban response")

        with patch("bybit_client.requests.get", return_value=response) as request:
            with self.assertRaises(ExchangeRateLimitError) as first:
                client.get_open_orders("MUUSDT")
            with self.assertRaises(ExchangeRateLimitError) as second:
                client.get_open_orders("MUUSDT")

        self.assertGreaterEqual(first.exception.retry_after, 600)
        self.assertGreater(second.exception.retry_after, 0)
        self.assertEqual(request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
