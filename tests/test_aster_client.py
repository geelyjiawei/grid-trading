import json
import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from aster_client import AsterFuturesClient  # noqa: E402
from exchange_errors import ExchangeRateLimitError, ExchangeRequestUncertainError  # noqa: E402


PRIVATE_KEY = "0x" + "1" * 64
SIGNER = "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
USER = "0x0000000000000000000000000000000000000abc"


class FakeResponse:
    def __init__(self, data, status_code=200, headers=None):
        self._data = data
        self.status_code = status_code
        self.text = str(data)
        self.headers = dict(headers or {})

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
    def test_batch_orders_preserve_each_client_id_and_item_result(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return [
                {
                    "orderId": 201,
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
        self.assertEqual(calls[0][1], "/fapi/v3/batchOrders")
        self.assertEqual([item["newClientOrderId"] for item in payload], ["g_0_B_batch", "g_1_S_batch"])
        self.assertEqual([item["timeInForce"] for item in payload], ["GTC", "GTC"])
        self.assertEqual([item["reduceOnly"] for item in payload], ["true", "false"])
        self.assertEqual(response["result"]["list"][0]["retCode"], 0)
        self.assertEqual(response["result"]["list"][0]["result"]["orderLinkId"], "g_0_B_batch")
        self.assertEqual(response["result"]["list"][1]["retCode"], -2010)

    def test_instrument_info_preserves_market_lot_rules(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(
            FakeResponse(
                {
                    "symbols": [
                        {
                            "symbol": "ASTERUSDT",
                            "filters": [
                                {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                                {
                                    "filterType": "LOT_SIZE",
                                    "stepSize": "1",
                                    "minQty": "1",
                                    "maxQty": "100000",
                                },
                                {
                                    "filterType": "MARKET_LOT_SIZE",
                                    "stepSize": "1",
                                    "minQty": "1",
                                    "maxQty": "5000",
                                },
                                {"filterType": "MIN_NOTIONAL", "notional": "5"},
                            ],
                        }
                    ]
                }
            )
        )

        response = client.get_instrument_info("asterusdt")
        info = response["result"]["list"][0]

        self.assertEqual(info["lotSizeFilter"]["maxOrderQty"], "100000")
        self.assertEqual(info["lotSizeFilter"]["minNotionalValue"], "5")
        self.assertEqual(info["marketLotSizeFilter"]["qtyStep"], "1")
        self.assertEqual(info["marketLotSizeFilter"]["minOrderQty"], "1")
        self.assertEqual(info["marketLotSizeFilter"]["maxOrderQty"], "5000")

    def test_http_503_is_reported_as_unknown_submission_outcome(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(
            FakeResponse(
                {"code": -1007, "msg": "Timeout; execution status unknown"},
                status_code=503,
            )
        )

        with self.assertRaises(ExchangeRequestUncertainError):
            client.place_order(
                symbol="ASTERUSDT",
                side="Buy",
                qty="20",
                price="0.5",
                order_type="Limit",
                order_link_id="g_1_B_unknown",
            )

    def test_http_200_unknown_execution_codes_are_not_treated_as_rejections(self):
        for error_code, message in (
            (-1006, "Unexpected response; execution status unknown"),
            (-1007, "Timeout waiting for backend; send status unknown"),
        ):
            with self.subTest(error_code=error_code):
                client = AsterFuturesClient(
                    USER,
                    PRIVATE_KEY,
                    signer=SIGNER,
                    base_url="https://example.test",
                )
                client.session = FakeSession(FakeResponse({"code": error_code, "msg": message}))

                with self.assertRaises(ExchangeRequestUncertainError):
                    client.place_order(
                        symbol="ASTERUSDT",
                        side="Buy",
                        qty="20",
                        price="0.5",
                        order_type="Limit",
                        order_link_id=f"g_1_B_unknown_{abs(error_code)}",
                    )

    def test_http_200_definitive_order_rejection_remains_definitive(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.session = FakeSession(FakeResponse({"code": -2010, "msg": "Order rejected"}))

        with self.assertRaisesRegex(RuntimeError, "Order rejected"):
            client.place_order(
                symbol="ASTERUSDT",
                side="Buy",
                qty="20",
                price="0.5",
                order_type="Limit",
                order_link_id="g_1_B_rejected",
            )

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
                "avgPrice": "99.5",
                "executedQty": "0.12",
                "cumQuote": "11.94",
                "status": "CANCELED",
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
        self.assertEqual(order["qty"], "0.2")
        self.assertEqual(order["avgPrice"], "99.5")
        self.assertEqual(order["executedQty"], "0.12")
        self.assertEqual(order["cumQuote"], "11.94")
        self.assertEqual(order["orderStatus"], "CANCELED")
        self.assertEqual(trade["side"], "Buy")
        self.assertEqual(trade["fee"], "0.01")
        self.assertEqual(trade["feeUsdt"], "0.01")
        self.assertEqual(position["side"], "Sell")
        self.assertEqual(position["size"], "0.2")

    def test_commission_sign_variants_normalize_to_positive_fee_cost(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")

        for commission in ("0.01", "-0.01"):
            with self.subTest(commission=commission):
                trade = client._normalize_trade(
                    {
                        "orderId": 1,
                        "id": 2,
                        "side": "BUY",
                        "price": "10",
                        "qty": "1",
                        "quoteQty": "10",
                        "commission": commission,
                        "commissionAsset": "USDT",
                    }
                )

                self.assertEqual(trade["fee"], "0.01")
                self.assertEqual(trade["feeUsdt"], "0.01")

    def test_get_order_trades_filters_order_id_client_side(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            if path == "/fapi/v3/order":
                return {
                    "orderId": 2,
                    "executedQty": "0.2",
                    "time": 1_000,
                    "updateTime": 2_000,
                }
            return [
                {
                    "orderId": 1,
                    "id": 10,
                    "side": "BUY",
                    "price": "99",
                    "qty": "0.2",
                    "quoteQty": "19.8",
                    "commission": "-0.01",
                    "commissionAsset": "USDT",
                    "time": 1_500,
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
                    "time": 2_000,
                },
            ]

        client._request = fake_request

        trades = client.get_order_trades("ASTERUSDT", "2")
        params = calls[1][2]

        self.assertNotIn("orderId", params)
        self.assertEqual(params["limit"], 1000)
        self.assertIn("startTime", params)
        self.assertIn("endTime", params)
        self.assertEqual(len(trades["result"]["list"]), 1)
        self.assertEqual(trades["result"]["list"][0]["orderId"], "2")

    def test_order_trade_lookup_expands_for_partial_fills_across_time(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        calls = []

        def trade(trade_id, qty, timestamp):
            return {
                "orderId": 42,
                "id": trade_id,
                "side": "BUY",
                "price": "10",
                "qty": qty,
                "quoteQty": str(float(qty) * 10),
                "commission": "0",
                "commissionAsset": "USDT",
                "time": timestamp,
            }

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            if path == "/fapi/v3/order":
                return {
                    "orderId": 42,
                    "executedQty": "0.3",
                    "time": 1_000,
                    "updateTime": 1_000_000,
                }
            if params["startTime"] >= 700_000:
                return [trade(3, "0.1", 900_000)]
            return [
                trade(1, "0.1", 1_000),
                trade(2, "0.1", 500_000),
                trade(3, "0.1", 900_000),
            ]

        client._request = fake_request

        response = client.get_order_trades("ASTERUSDT", "42")

        self.assertEqual([item["tradeId"] for item in response["result"]["list"]], ["1", "2", "3"])
        self.assertEqual(len(calls), 3)
        self.assertLess(calls[2][2]["startTime"], calls[1][2]["startTime"])

    def test_order_trade_lookup_paginates_a_full_time_window_by_trade_id(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        trade_calls = []

        def target_trade():
            return {
                "orderId": 77,
                "id": 2_001,
                "side": "SELL",
                "price": "5",
                "qty": "0.2",
                "quoteQty": "1",
                "commission": "0",
                "commissionAsset": "USDT",
                "time": 1_000_000,
            }

        def fake_request(method, path, *, params=None, auth=False):
            if path == "/fapi/v3/order":
                return {
                    "orderId": 77,
                    "executedQty": "0.2",
                    "time": 1_000_000,
                    "updateTime": 1_000_000,
                }
            trade_calls.append(dict(params))
            if len(trade_calls) == 1:
                return [
                    {
                        "orderId": 9000 + index,
                        "id": index,
                        "side": "BUY",
                        "price": "1",
                        "qty": "1",
                        "time": 700_000 + index,
                    }
                    for index in range(1000)
                ]
            if params.get("fromId") == 1000:
                return [target_trade()]
            return []

        client._request = fake_request

        response = client.get_order_trades("ASTERUSDT", "77")

        self.assertEqual(len(response["result"]["list"]), 1)
        self.assertEqual(response["result"]["list"][0]["qty"], "0.2")
        self.assertEqual(len(trade_calls), 2)
        self.assertEqual(trade_calls[1]["fromId"], 1000)
        self.assertNotIn("startTime", trade_calls[1])
        self.assertNotIn("endTime", trade_calls[1])

    def test_order_trade_lookup_splits_order_lifetime_into_seven_day_windows(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        created = 1_000_000
        updated = created + (8 * 24 * 60 * 60 * 1000)
        trade_calls = []

        def trade(trade_id, timestamp):
            return {
                "orderId": 78,
                "id": trade_id,
                "side": "SELL",
                "price": "5",
                "qty": "0.1",
                "quoteQty": "0.5",
                "commission": "0",
                "commissionAsset": "USDT",
                "time": timestamp,
            }

        def fake_request(method, path, *, params=None, auth=False):
            if path == "/fapi/v3/order":
                return {
                    "orderId": 78,
                    "executedQty": "0.2",
                    "time": created,
                    "updateTime": updated,
                }
            trade_calls.append(dict(params))
            if params["startTime"] == updated - client.TRADE_PROBE_PADDING_MS:
                return [trade(2, updated)]
            if params["startTime"] <= created <= params["endTime"]:
                return [trade(1, created)]
            if params["startTime"] <= updated <= params["endTime"]:
                return [trade(2, updated)]
            return []

        client._request = fake_request

        response = client.get_order_trades("ASTERUSDT", "78")

        self.assertEqual([item["tradeId"] for item in response["result"]["list"]], ["1", "2"])
        full_windows = trade_calls[1:]
        self.assertEqual(len(full_windows), 2)
        self.assertTrue(
            all(
                item["endTime"] - item["startTime"] <= client.TRADE_WINDOW_LIMIT_MS
                for item in full_windows
            )
        )

    def test_order_trade_lookup_rejects_nonadvancing_trade_cursor(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        trade_calls = 0

        def fake_request(method, path, *, params=None, auth=False):
            nonlocal trade_calls
            if path == "/fapi/v3/order":
                return {
                    "orderId": 79,
                    "executedQty": "1",
                    "time": 1_000_000,
                    "updateTime": 1_000_000,
                }
            trade_calls += 1
            if trade_calls == 1:
                return [
                    {
                        "orderId": 9000 + index,
                        "id": index,
                        "side": "BUY",
                        "price": "1",
                        "qty": "1",
                        "time": 700_000 + index,
                    }
                    for index in range(1000)
                ]
            return [
                {
                    "orderId": 79,
                    "id": 999,
                    "side": "BUY",
                    "price": "1",
                    "qty": "1",
                    "time": 1_000_000,
                }
            ]

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "did not advance"):
            client.get_order_trades("ASTERUSDT", "79")

    def test_order_trade_lookup_caps_history_requests(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.TRADE_PAGE_LIMIT = 2
        client.MAX_TRADE_HISTORY_QUERIES = 1
        trade_calls = 0

        def fake_request(method, path, *, params=None, auth=False):
            nonlocal trade_calls
            if path == "/fapi/v3/order":
                return {
                    "orderId": 80,
                    "executedQty": "1",
                    "time": 1_000_000,
                    "updateTime": 1_000_000,
                }
            trade_calls += 1
            return [
                {
                    "orderId": 9000 + index,
                    "id": index,
                    "side": "BUY",
                    "price": "1",
                    "qty": "1",
                    "time": 1_000_000 + index,
                }
                for index in range(2)
            ]

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "too many paginated queries"):
            client.get_order_trades("ASTERUSDT", "80")
        self.assertEqual(trade_calls, 1)

    def test_order_trade_lookup_rejects_unordered_full_page(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        client.TRADE_PAGE_LIMIT = 2

        def fake_request(method, path, *, params=None, auth=False):
            if path == "/fapi/v3/order":
                return {
                    "orderId": 81,
                    "executedQty": "1",
                    "time": 1_000_000,
                    "updateTime": 1_000_000,
                }
            return [
                {
                    "orderId": 9002,
                    "id": 2,
                    "side": "BUY",
                    "price": "1",
                    "qty": "1",
                    "time": 1_000_002,
                },
                {
                    "orderId": 9001,
                    "id": 1,
                    "side": "BUY",
                    "price": "1",
                    "qty": "1",
                    "time": 1_000_001,
                },
            ]

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "not strictly ordered"):
            client.get_order_trades("ASTERUSDT", "81")

    def test_order_trade_lookup_rejects_incomplete_execution_quantity(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")

        def fake_request(method, path, *, params=None, auth=False):
            if path == "/fapi/v3/order":
                return {
                    "orderId": 88,
                    "executedQty": "0.3",
                    "time": 1_000_000,
                    "updateTime": 1_000_000,
                }
            return [
                {
                    "orderId": 88,
                    "id": 1,
                    "side": "BUY",
                    "price": "10",
                    "qty": "0.1",
                    "quoteQty": "1",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "time": 1_000_000,
                }
            ]

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            client.get_order_trades("ASTERUSDT", "88")

    def test_order_trade_lookup_falls_back_when_order_is_definitively_missing(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((path, params))
            if path == "/fapi/v3/order":
                raise RuntimeError("Order does not exist")
            return [
                {
                    "orderId": 99,
                    "id": 4,
                    "side": "SELL",
                    "price": "2",
                    "qty": "1",
                    "quoteQty": "2",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "time": 1_000_000,
                }
            ]

        client._request = fake_request

        response = client.get_order_trades("ASTERUSDT", "99")

        self.assertEqual(response["result"]["list"][0]["orderId"], "99")
        self.assertEqual(calls[1][1], {"symbol": "ASTERUSDT", "limit": 1000})

    def test_order_trade_lookup_never_trusts_full_fallback_page(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")

        def fake_request(method, path, *, params=None, auth=False):
            if path == "/fapi/v3/order":
                raise RuntimeError("Unknown order")
            return [
                {
                    "orderId": index,
                    "id": index,
                    "side": "BUY",
                    "price": "1",
                    "qty": "1",
                    "time": index,
                }
                for index in range(1000)
            ]

        client._request = fake_request

        with self.assertRaisesRegex(RuntimeError, "page is full"):
            client.get_order_trades("ASTERUSDT", "missing")

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

    def test_fee_rates_use_signed_exchange_endpoint_and_short_cache(self):
        client = AsterFuturesClient(USER, PRIVATE_KEY, signer=SIGNER, base_url="https://example.test")
        calls = []

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return {
                "symbol": "ANSEMUSDT",
                "makerCommissionRate": "0",
                "takerCommissionRate": "0.000400",
            }

        client._request = fake_request

        first = client.get_fee_rates("ansemusdt")
        second = client.get_fee_rates("ANSEMUSDT")

        self.assertEqual(
            calls,
            [("GET", "/fapi/v3/commissionRate", {"symbol": "ANSEMUSDT"}, True)],
        )
        self.assertEqual(first["result"]["makerFeeRate"], "0")
        self.assertEqual(first["result"]["takerFeeRate"], "0.000400")
        self.assertEqual(first["result"]["source"], "exchange")
        self.assertEqual(second["result"]["source"], "exchange_cache")

    def test_nonquote_fee_conversion_uses_execution_minute_open(self):
        client = AsterFuturesClient(
            USER,
            PRIVATE_KEY,
            signer=SIGNER,
            base_url="https://example.test",
        )
        calls = []
        trade_time = 1714012800123
        minute_start = trade_time - (trade_time % 60_000)

        def fake_request(method, path, *, params=None, auth=False):
            calls.append((method, path, params, auth))
            return [[minute_start, "600", "601", "599", "600"]]

        client._request = fake_request
        trade = client._normalize_trade(
            {
                "orderId": 1,
                "id": 2,
                "side": "SELL",
                "price": "100",
                "qty": "1",
                "quoteQty": "100",
                "commission": "-0.001",
                "commissionAsset": "BNB",
                "maker": False,
                "time": trade_time,
            }
        )

        self.assertEqual(trade["feeUsdt"], "0.600")
        self.assertEqual(trade["feeUsdtSource"], "historical_minute_open")
        self.assertEqual(calls[0][1], "/fapi/v3/klines")

    def test_rate_limit_activates_local_cooldown_without_second_http_request(self):
        client = AsterFuturesClient(
            USER,
            PRIVATE_KEY,
            signer=SIGNER,
            base_url="https://example.test",
        )
        client.session = FakeSession(
            FakeResponse(
                {
                    "code": -1015,
                    "msg": "Too many new orders; current limit is 1200 orders per MINUTE.",
                },
                status_code=429,
                headers={"Retry-After": "7"},
            )
        )

        with self.assertRaises(ExchangeRateLimitError) as first:
            client.get_open_orders("ANSEMUSDT")
        with self.assertRaises(ExchangeRateLimitError) as second:
            client.get_open_orders("ANSEMUSDT")

        self.assertGreaterEqual(first.exception.retry_after, 60)
        self.assertGreater(second.exception.retry_after, 0)
        self.assertEqual(len(client.session.calls), 1)

    def test_batch_item_rate_limit_activates_cooldown_before_next_request(self):
        client = AsterFuturesClient(
            USER,
            PRIVATE_KEY,
            signer=SIGNER,
            base_url="https://example.test",
        )
        client.session = FakeSession(
            FakeResponse(
                [
                    {
                        "code": -1015,
                        "msg": "Too many new orders; current limit is 1200 orders per MINUTE.",
                    }
                ]
            )
        )

        with self.assertRaises(ExchangeRateLimitError):
            client.place_orders(
                [
                    {
                        "symbol": "ANSEMUSDT",
                        "side": "Buy",
                        "qty": "20",
                        "price": "0.30",
                        "order_type": "Limit",
                        "reduce_only": False,
                        "order_link_id": "g_0_B_rate_limited",
                        "time_in_force": None,
                    }
                ]
            )
        with self.assertRaises(ExchangeRateLimitError):
            client.get_open_orders("ANSEMUSDT")
        self.assertEqual(len(client.session.calls), 1)

    def test_http_418_ip_ban_activates_local_cooldown(self):
        client = AsterFuturesClient(
            USER,
            PRIVATE_KEY,
            signer=SIGNER,
            base_url="https://example.test",
        )
        client.session = FakeSession(
            FakeResponse(
                {"code": -1003, "msg": "IP banned until 1783839999000"},
                status_code=418,
                headers={"Retry-After": "90"},
            )
        )

        with self.assertRaises(ExchangeRateLimitError) as first:
            client.get_open_orders("ANSEMUSDT")
        with self.assertRaises(ExchangeRateLimitError):
            client.get_open_orders("ANSEMUSDT")

        self.assertGreaterEqual(first.exception.retry_after, 90)
        self.assertEqual(len(client.session.calls), 1)


if __name__ == "__main__":
    unittest.main()
