import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from aster_client import AsterFuturesClient  # noqa: E402


PRIVATE_KEY = "0x" + "1" * 64
SIGNER = "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
USER = "0x0000000000000000000000000000000000000abc"


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self):
        return self._data


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.response


class AsterClientTests(unittest.TestCase):
    def test_signature_payload_uses_eip712_message_body(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")

        payload = client.signature_payload({"symbol": "ASTERUSDT", "side": "BUY"})
        typed_data = client.signature_typed_data({"symbol": "ASTERUSDT", "side": "BUY"})

        self.assertEqual(client.signer, SIGNER)
        self.assertIn("symbol=ASTERUSDT", payload)
        self.assertIn("nonce=1", payload)
        self.assertIn(f"user={USER}", payload)
        self.assertIn(f"signer={SIGNER}", payload)
        self.assertEqual(typed_data["domain"]["name"], "AsterSignTransaction")
        self.assertEqual(typed_data["domain"]["chainId"], 1666)
        self.assertEqual(typed_data["message"]["msg"], payload)

    def test_signed_request_adds_nonce_user_signer_and_signature(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(FakeResponse({"orderId": 123}))

        response = client.place_order(
            symbol="ASTERUSDT",
            side="Buy",
            qty="20",
            price="0.5",
            order_type="Limit",
            order_link_id="g_1_B_test",
            time_in_force="PostOnly",
        )
        call = client.session.calls[0]
        params = call["data"]

        self.assertEqual(response["result"]["orderId"], "123")
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://example.test/fapi/v3/order")
        self.assertEqual(params["symbol"], "ASTERUSDT")
        self.assertEqual(params["side"], "BUY")
        self.assertEqual(params["timeInForce"], "GTX")
        self.assertEqual(params["newClientOrderId"], "g_1_B_test")
        self.assertEqual(params["user"], USER)
        self.assertEqual(params["signer"], SIGNER)
        self.assertIn("nonce", params)
        self.assertRegex(params["signature"], r"^[0-9a-f]{130}$")

    def test_place_order_preserves_exchange_accepted_quantity(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(
            FakeResponse(
                {
                    "orderId": 4770039,
                    "clientOrderId": "g_0_B_test",
                    "side": "BUY",
                    "price": "0.3800000",
                    "origQty": "70",
                    "status": "NEW",
                    "reduceOnly": True,
                    "time": 123,
                }
            )
        )

        response = client.place_order(
            symbol="ANSEMUSDT",
            side="Buy",
            qty="100",
            price="0.3800000",
            order_type="Limit",
            reduce_only=True,
            order_link_id="g_0_B_test",
        )

        self.assertEqual(response["result"]["orderId"], "4770039")
        self.assertEqual(response["result"]["qty"], "70")
        self.assertEqual(response["result"]["price"], "0.3800000")
        self.assertTrue(response["result"]["reduceOnly"])

    def test_normalizes_order_trade_position_shapes(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")

        order = client._normalize_order(
            {
                "orderId": 1,
                "clientOrderId": "g_1_S_test",
                "side": "SELL",
                "price": "100",
                "origQty": "0.2",
                "status": "NEW",
                "reduceOnly": "true",
                "time": 123,
            }
        )
        trade = client._normalize_trade(
            {
                "orderId": 1,
                "id": 2,
                "side": "BUY",
                "price": "99",
                "qty": "0.2",
                "quoteQty": "19.8",
                "commission": "-0.01",
                "commissionAsset": "USDT",
                "maker": True,
                "time": 456,
            }
        )
        position = client._normalize_position(
            {
                "positionAmt": "-0.2",
                "entryPrice": "100",
                "markPrice": "99",
                "unRealizedProfit": "0.2",
                "leverage": "5",
                "liquidationPrice": "200",
            }
        )

        self.assertEqual(order["side"], "Sell")
        self.assertTrue(order["reduceOnly"])
        self.assertEqual(trade["side"], "Buy")
        self.assertEqual(trade["feeUsdt"], "-0.01")
        self.assertEqual(position["side"], "Sell")
        self.assertEqual(position["size"], "0.2")

    def test_get_order_trades_filters_order_id_client_side(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(
            FakeResponse(
                [
                    {
                        "orderId": 1,
                        "id": 10,
                        "side": "BUY",
                        "price": "99",
                        "qty": "0.2",
                        "quoteQty": "19.8",
                        "commission": "-0.01",
                        "commissionAsset": "USDT",
                    },
                    {
                        "orderId": 2,
                        "id": 11,
                        "side": "BUY",
                        "price": "98",
                        "qty": "0.2",
                        "quoteQty": "19.6",
                        "commission": "-0.01",
                        "commissionAsset": "USDT",
                    },
                ]
            )
        )

        trades = client.get_order_trades("ASTERUSDT", "2")
        params = client.session.calls[0]["params"]

        self.assertNotIn("orderId", params)
        self.assertEqual(params["limit"], 1000)
        self.assertEqual(len(trades["result"]["list"]), 1)
        self.assertEqual(trades["result"]["list"][0]["orderId"], "2")

    def test_get_order_by_link_uses_orig_client_order_id(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(
            FakeResponse(
                {
                    "orderId": 88,
                    "clientOrderId": "g_3_B_recover",
                    "side": "BUY",
                    "price": "0.38",
                    "origQty": "100",
                    "status": "NEW",
                    "reduceOnly": "true",
                }
            )
        )

        response = client.get_order_by_link("ansemusdt", "g_3_B_recover")
        call = client.session.calls[0]

        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["url"], "https://example.test/fapi/v3/order")
        self.assertEqual(call["params"]["symbol"], "ANSEMUSDT")
        self.assertEqual(call["params"]["origClientOrderId"], "g_3_B_recover")
        self.assertEqual(response["result"]["orderId"], "88")
        self.assertEqual(response["result"]["orderLinkId"], "g_3_B_recover")

    def test_get_order_by_link_returns_empty_for_definitive_not_found(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(
            FakeResponse({"code": -2013, "msg": "Order does not exist."}, status_code=400)
        )

        response = client.get_order_by_link("ANSEMUSDT", "g_3_B_missing")

        self.assertEqual(response, {"retCode": 0, "result": {}})


if __name__ == "__main__":
    unittest.main()
