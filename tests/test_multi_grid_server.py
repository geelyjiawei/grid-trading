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
import pyotp  # noqa: E402
from binance_client import BinanceFuturesClient  # noqa: E402
from auth import hash_password  # noqa: E402
from test_grid_engine import FakeClient  # noqa: E402


class MultiGridServerTests(unittest.TestCase):
    def setUp(self):
        main._engines.clear()
        main._client = FakeClient("100")
        self.client = TestClient(main.app)

    def tearDown(self):
        main._engines.clear()
        main._client = None
        for key in (
            "AUTH_REQUIRED",
            "ADMIN_USERNAME",
            "ADMIN_PASSWORD_HASH",
            "TOTP_SECRET",
            "SESSION_SECRET",
            "AUTH_SHOW_TOTP_SETUP",
        ):
            os.environ.pop(key, None)

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
        old_values = {
            "GRID_EXCHANGE": os.environ.get("GRID_EXCHANGE"),
            "BYBIT_API_KEY": os.environ.get("BYBIT_API_KEY"),
            "BYBIT_API_SECRET": os.environ.get("BYBIT_API_SECRET"),
            "BYBIT_TESTNET": os.environ.get("BYBIT_TESTNET"),
        }
        try:
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

        self.assertEqual(order["orderId"], "123")
        self.assertEqual(order["orderLinkId"], "g_1_B_abcdef")
        self.assertEqual(order["side"], "Buy")
        self.assertEqual(position["side"], "Sell")
        self.assertEqual(position["size"], "0.25")
        self.assertEqual(position["avgPrice"], "105")

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
