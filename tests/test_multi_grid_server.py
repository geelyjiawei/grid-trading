import os
import sys
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
if str(ROOT_DIR / "tests") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "tests"))

import main  # noqa: E402
from test_grid_engine import FakeClient  # noqa: E402


class MultiGridServerTests(unittest.TestCase):
    def setUp(self):
        main._engines.clear()
        main._client = FakeClient("100")
        self.client = TestClient(main.app)

    def tearDown(self):
        main._engines.clear()
        main._client = None

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
        self.assertFalse(by_symbol["BTCUSDT"]["running"])
        self.assertTrue(by_symbol["ETHUSDT"]["running"])

    def test_same_symbol_cannot_start_twice(self):
        first_response = self.client.post("/api/grid/start", json=self._payload("BTCUSDT"))
        second_response = self.client.post("/api/grid/start", json=self._payload("BTCUSDT"))

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 400)

    def test_api_config_can_be_saved_and_loaded_from_disk(self):
        original_path = main.API_CONFIG_FILE
        with tempfile.TemporaryDirectory() as tmpdir:
            main.API_CONFIG_FILE = str(Path(tmpdir) / "api_config.json")
            config = {
                "api_key": "abcd1234efgh",
                "api_secret": "super-private-token",
                "testnet": True,
            }

            main._save_api_config(config)
            loaded = main._load_api_config()
            saved_text = Path(main.API_CONFIG_FILE).read_text(encoding="utf-8")

            self.assertEqual(loaded["api_key"], config["api_key"])
            self.assertEqual(loaded["api_secret"], config["api_secret"])
            self.assertEqual(loaded["testnet"], config["testnet"])
            self.assertEqual(loaded["source"], "file")
            self.assertEqual(main._mask_api_key(loaded["api_key"]), "abcd...efgh")
            self.assertIn('"encrypted": true', saved_text)
            self.assertNotIn(config["api_key"], saved_text)
            self.assertNotIn(config["api_secret"], saved_text)

        main.API_CONFIG_FILE = original_path

    def test_api_config_can_be_loaded_from_environment(self):
        old_values = {
            "BYBIT_API_KEY": os.environ.get("BYBIT_API_KEY"),
            "BYBIT_API_SECRET": os.environ.get("BYBIT_API_SECRET"),
            "BYBIT_TESTNET": os.environ.get("BYBIT_TESTNET"),
        }
        try:
            os.environ["BYBIT_API_KEY"] = "env-api-key"
            os.environ["BYBIT_API_SECRET"] = "env-api-secret"
            os.environ["BYBIT_TESTNET"] = "true"

            loaded = main._load_api_config()

            self.assertEqual(loaded["api_key"], "env-api-key")
            self.assertEqual(loaded["api_secret"], "env-api-secret")
            self.assertTrue(loaded["testnet"])
            self.assertEqual(loaded["source"], "env")
        finally:
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
