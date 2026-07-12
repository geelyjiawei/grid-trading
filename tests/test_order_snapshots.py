import copy
import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from aster_client import AsterFuturesClient  # noqa: E402
from binance_client import BinanceFuturesClient  # noqa: E402
from bybit_client import BybitClient  # noqa: E402
from exchange_errors import ExchangeRequestUncertainError  # noqa: E402
from exchange_snapshots import validate_order_row, validate_order_rows  # noqa: E402


def normalized_order(**changes):
    row = {
        "symbol": "MUUSDT",
        "orderId": "1001",
        "orderLinkId": "g_1_S_snapshot",
        "side": "Sell",
        "price": "100.50",
        "qty": "2.00",
        "avgPrice": "0",
        "executedQty": "0",
        "cumQuote": "0",
        "orderStatus": "NEW",
        "reduceOnly": False,
        "timeInForce": "GTC",
        "orderType": "LIMIT",
        "createdTime": "1714012800000",
    }
    row.update(changes)
    return row


def binance_order(**changes):
    row = {
        "symbol": "MUUSDT",
        "orderId": 1001,
        "clientOrderId": "g_1_S_snapshot",
        "side": "SELL",
        "price": "100.50",
        "origQty": "2.00",
        "avgPrice": "0",
        "executedQty": "0",
        "cumQuote": "0",
        "status": "NEW",
        "reduceOnly": False,
        "timeInForce": "GTC",
        "type": "LIMIT",
        "time": 1714012800000,
    }
    row.update(changes)
    return row


def bybit_order(**changes):
    row = {
        "symbol": "MUUSDT",
        "orderId": "1001",
        "orderLinkId": "g_1_S_snapshot",
        "side": "Sell",
        "price": "100.50",
        "qty": "2.00",
        "avgPrice": "",
        "cumExecQty": "0",
        "cumExecValue": "0",
        "orderStatus": "New",
        "reduceOnly": False,
        "timeInForce": "GTC",
        "orderType": "Limit",
        "createdTime": "1714012800000",
    }
    row.update(changes)
    return row


class SharedOrderSnapshotTests(unittest.TestCase):
    def test_strict_order_rows_reject_corruption_atomically(self):
        mutations = {
            "missing symbol": lambda row: row.pop("symbol"),
            "wrong symbol": lambda row: row.__setitem__("symbol", "OTHERUSDT"),
            "missing order id": lambda row: row.pop("orderId"),
            "invalid side": lambda row: row.__setitem__("side", "Hold"),
            "missing quantity": lambda row: row.pop("qty"),
            "zero quantity": lambda row: row.__setitem__("qty", "0"),
            "nan quantity": lambda row: row.__setitem__("qty", "NaN"),
            "infinite quantity": lambda row: row.__setitem__("qty", "Infinity"),
            "zero limit price": lambda row: row.__setitem__("price", "0"),
            "missing price": lambda row: row.pop("price"),
            "missing average price": lambda row: row.pop("avgPrice"),
            "negative average price": lambda row: row.__setitem__("avgPrice", "-1"),
            "missing executed quantity": lambda row: row.pop("executedQty"),
            "executed beyond order": lambda row: row.__setitem__("executedQty", "2.01"),
            "missing quote volume": lambda row: row.pop("cumQuote"),
            "negative quote volume": lambda row: row.__setitem__("cumQuote", "-1"),
            "invalid status": lambda row: row.__setitem__("orderStatus", "MAYBE_FILLED"),
            "inconsistent partial status": lambda row: row.__setitem__(
                "orderStatus", "PARTIALLY_FILLED"
            ),
            "inconsistent filled status": lambda row: row.update(
                {"orderStatus": "FILLED", "executedQty": "1"}
            ),
            "invalid reduce flag": lambda row: row.__setitem__("reduceOnly", "yes"),
            "missing time in force": lambda row: row.pop("timeInForce"),
            "invalid time in force": lambda row: row.__setitem__(
                "timeInForce", "SOMEDAY"
            ),
            "missing order type": lambda row: row.pop("orderType"),
            "invalid order type": lambda row: row.__setitem__("orderType", "MAGIC"),
            "fractional creation time": lambda row: row.__setitem__("createdTime", "1.5"),
        }

        for label, mutate in mutations.items():
            with self.subTest(label=label):
                good = normalized_order(orderId="good", orderLinkId="g_good")
                bad = normalized_order(orderId="bad", orderLinkId="g_bad")
                mutate(bad)
                with self.assertRaisesRegex(RuntimeError, "order snapshot"):
                    validate_order_rows(
                        [good, bad],
                        expected_symbol="MUUSDT",
                        require_details=True,
                    )

    def test_official_status_and_time_in_force_spellings_are_canonicalized(self):
        cases = {
            "PartiallyFilled": "PARTIALLY_FILLED",
            "Cancelled": "CANCELED",
            "EXPIRED_IN_MATCH": "EXPIRED_IN_MATCH",
            "PendingCancel": "PENDING_CANCEL",
            "Untriggered": "UNTRIGGERED",
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                execution = {}
                if expected == "PARTIALLY_FILLED":
                    execution = {
                        "avgPrice": "100.50",
                        "executedQty": "1.00",
                        "cumQuote": "100.50",
                    }
                row = normalized_order(
                    orderStatus=source,
                    timeInForce="PostOnly",
                    **execution,
                )
                validated = validate_order_row(
                    row,
                    expected_symbol="MUUSDT",
                    require_details=True,
                )
                self.assertEqual(validated["status"], expected)
                self.assertEqual(validated["time_in_force"], "POST_ONLY")

    def test_open_order_identity_must_be_unique_but_history_links_may_be_reused(self):
        first = normalized_order(orderId="1", orderLinkId="reused")
        second = normalized_order(orderId="2", orderLinkId="reused")

        with self.assertRaisesRegex(RuntimeError, "duplicate client order ID"):
            validate_order_rows(
                [first, second],
                expected_symbol="MUUSDT",
                require_details=True,
                unique_link_ids=True,
            )

        history = validate_order_rows(
            [first, second],
            expected_symbol="MUUSDT",
            require_details=True,
            unique_link_ids=False,
        )
        self.assertEqual([item["order_id"] for item in history], ["1", "2"])

        duplicate_id = copy.deepcopy(second)
        duplicate_id["orderId"] = "1"
        with self.assertRaisesRegex(RuntimeError, "duplicate exchange order ID"):
            validate_order_rows(
                [first, duplicate_id],
                expected_symbol="MUUSDT",
                require_details=True,
                unique_link_ids=False,
            )

        with self.assertRaisesRegex(RuntimeError, "non-open orderStatus"):
            validate_order_rows(
                [normalized_order(orderStatus="CANCELED")],
                expected_symbol="MUUSDT",
                require_details=True,
                allowed_statuses={"NEW", "PARTIALLY_FILLED"},
            )

    def test_legacy_client_rows_are_compatible_but_present_corruption_is_rejected(self):
        legacy = {
            "orderId": "1",
            "orderLinkId": "manual",
            "side": "Buy",
            "price": "100",
            "qty": "1",
            "orderStatus": "NEW",
            "reduceOnly": False,
        }
        validated = validate_order_row(
            legacy,
            expected_symbol="MUUSDT",
            require_details=False,
        )
        self.assertEqual(validated["order_id"], "1")

        for field, value in (("qty", "NaN"), ("price", "Infinity"), ("reduceOnly", "maybe")):
            with self.subTest(field=field):
                malformed = dict(legacy)
                malformed[field] = value
                with self.assertRaisesRegex(RuntimeError, "order snapshot"):
                    validate_order_row(
                        malformed,
                        expected_symbol="MUUSDT",
                        require_details=False,
                    )

    def test_zero_quantity_is_only_valid_for_bybit_close_all_contract(self):
        close_all = normalized_order(
            qty="0",
            orderStatus="Untriggered",
            reduceOnly=True,
            closeOnTrigger=True,
        )
        validated = validate_order_row(
            close_all,
            expected_symbol="MUUSDT",
            require_details=True,
        )
        self.assertTrue(validated["close_all_position"])

        for changes in (
            {"closeOnTrigger": False},
            {"reduceOnly": False},
            {"closeOnTrigger": "maybe"},
        ):
            with self.subTest(changes=changes):
                row = dict(close_all)
                row.update(changes)
                with self.assertRaisesRegex(RuntimeError, "order snapshot"):
                    validate_order_row(
                        row,
                        expected_symbol="MUUSDT",
                        require_details=True,
                    )

        conditional_close_all = normalized_order(
            qty="0",
            price="0",
            reduceOnly=False,
            closePosition=True,
            orderType="STOP_MARKET",
            timeInForce="GTD",
        )
        validated = validate_order_row(
            conditional_close_all,
            expected_symbol="MUUSDT",
            require_details=True,
        )
        self.assertTrue(validated["close_all_position"])
        self.assertEqual(validated["time_in_force"], "GTD")


class AdapterOrderSnapshotTests(unittest.TestCase):
    @staticmethod
    def order_request(link_id="g_1_B_ack"):
        return {
            "symbol": "MUUSDT",
            "side": "Buy",
            "qty": "1",
            "price": "100",
            "order_type": "Limit",
            "reduce_only": False,
            "order_link_id": link_id,
            "time_in_force": None,
        }

    def test_binance_query_methods_validate_identity_and_shape(self):
        client = BinanceFuturesClient("", "", True)
        client._request = lambda *args, **kwargs: [binance_order()]
        response = client.get_open_orders("MUUSDT")
        row = response["result"]["list"][0]
        self.assertEqual(row["symbol"], "MUUSDT")
        self.assertEqual(row["orderType"], "LIMIT")

        corruptions = {
            "wrong symbol": {"symbol": "OTHERUSDT"},
            "invalid side": {"side": "HOLD"},
            "nan quantity": {"origQty": "NaN"},
            "invalid status": {"status": "MAYBE"},
            "invalid time": {"time": "1.5"},
        }
        for label, changes in corruptions.items():
            with self.subTest(label=label):
                client._request = lambda *args, _changes=changes, **kwargs: [
                    binance_order(**_changes)
                ]
                with self.assertRaisesRegex(RuntimeError, "order snapshot"):
                    client.get_open_orders("MUUSDT")

        client._request = lambda *args, **kwargs: binance_order(orderId=1002)
        with self.assertRaisesRegex(RuntimeError, "belongs to order 1002"):
            client.get_order("MUUSDT", "1001")

        client._request = lambda *args, **kwargs: binance_order(
            clientOrderId="another-link"
        )
        with self.assertRaisesRegex(RuntimeError, "client order"):
            client.get_order_by_link("MUUSDT", "g_1_S_snapshot")

    def test_binance_open_orders_reject_duplicates_but_history_allows_reused_links(self):
        client = BinanceFuturesClient("", "", True)
        reused = [binance_order(), binance_order(orderId=1002)]
        client._request = lambda *args, **kwargs: reused

        with self.assertRaisesRegex(RuntimeError, "duplicate client order ID"):
            client.get_open_orders("MUUSDT")

        history = client.get_order_history("MUUSDT")["result"]["list"]
        self.assertEqual([row["orderId"] for row in history], ["1001", "1002"])

        client._request = lambda *args, **kwargs: [
            binance_order(),
            binance_order(clientOrderId="another-link"),
        ]
        with self.assertRaisesRegex(RuntimeError, "duplicate exchange order ID"):
            client.get_order_history("MUUSDT")

    def test_aster_query_methods_validate_identity_and_shape(self):
        client = AsterFuturesClient("", "", base_url="https://example.test")
        client._request = lambda *args, **kwargs: [binance_order()]
        response = client.get_open_orders("MUUSDT")
        self.assertEqual(response["result"]["list"][0]["symbol"], "MUUSDT")

        client._request = lambda *args, **kwargs: [
            binance_order(reduceOnly="not-a-boolean")
        ]
        with self.assertRaisesRegex(RuntimeError, "order snapshot"):
            client.get_open_orders("MUUSDT")

        client._request = lambda *args, **kwargs: binance_order(orderId=1002)
        with self.assertRaisesRegex(RuntimeError, "belongs to order 1002"):
            client.get_order("MUUSDT", "1001")

    def test_binance_like_clients_accept_official_conditional_close_all_shape(self):
        raw = binance_order(
            price="0",
            origQty="0",
            type="STOP_MARKET",
            closePosition=True,
        )
        for client in (
            BinanceFuturesClient("", "", True),
            AsterFuturesClient("", "", base_url="https://example.test"),
        ):
            with self.subTest(client=type(client).__name__):
                client._request = lambda *args, **kwargs: [raw]
                row = client.get_open_orders("MUUSDT")["result"]["list"][0]
                self.assertEqual(row["qty"], "0")
                self.assertTrue(row["closePosition"])

    def test_bybit_queries_reject_ambiguous_or_mismatched_rows(self):
        client = BybitClient("", "", True)
        client._request = lambda *args, **kwargs: {
            "retCode": 0,
            "result": {"list": [bybit_order()]},
        }
        response = client.get_open_orders("MUUSDT")
        row = response["result"]["list"][0]
        self.assertEqual(row["orderStatus"], "NEW")
        self.assertEqual(row["executedQty"], "0")

        client._request = lambda *args, **kwargs: {
            "retCode": 0,
            "result": {
                "list": [bybit_order(), bybit_order(orderId="1002")],
            },
        }
        with self.assertRaisesRegex(RuntimeError, "exactly one order"):
            client.get_order("MUUSDT", "1001")

        client._request = lambda *args, **kwargs: {
            "retCode": 0,
            "result": {"list": [bybit_order(symbol="OTHERUSDT")]},
        }
        with self.assertRaisesRegex(RuntimeError, "belongs to OTHERUSDT"):
            client.get_order("MUUSDT", "1001")

        client._request = lambda *args, **kwargs: {
            "retCode": 0,
            "result": {"list": [bybit_order(qty="Infinity")]},
        }
        with self.assertRaisesRegex(RuntimeError, "order snapshot"):
            client.get_open_orders("MUUSDT")

        client._request = lambda *args, **kwargs: {
            "retCode": 0,
            "result": {
                "list": [
                    bybit_order(
                        qty="0",
                        orderStatus="Untriggered",
                        reduceOnly=True,
                        closeOnTrigger=True,
                    )
                ]
            },
        }
        close_all = client.get_open_orders("MUUSDT")["result"]["list"][0]
        self.assertEqual(close_all["qty"], "0")
        self.assertTrue(close_all["closeOnTrigger"])

    def test_bybit_empty_realtime_query_uses_validated_history(self):
        client = BybitClient("", "", True)
        calls = []

        def request(method, path, **kwargs):
            calls.append(path)
            rows = (
                []
                if path.endswith("realtime")
                else [
                    bybit_order(
                        orderStatus="Filled",
                        avgPrice="100.50",
                        cumExecQty="2.00",
                        cumExecValue="201.00",
                    )
                ]
            )
            return {"retCode": 0, "result": {"list": rows}}

        client._request = request
        response = client.get_order_by_link("MUUSDT", "g_1_S_snapshot")

        self.assertEqual(calls, ["/v5/order/realtime", "/v5/order/history"])
        self.assertEqual(response["result"]["orderId"], "1001")
        self.assertEqual(response["result"]["orderStatus"], "FILLED")

    def test_bybit_success_shapes_and_pagination_duplicates_fail_closed(self):
        malformed = {
            "not an object": [],
            "missing result": {"retCode": 0},
            "invalid result": {"retCode": 0, "result": []},
            "missing list": {"retCode": 0, "result": {}},
            "invalid list": {"retCode": 0, "result": {"list": {}}},
            "invalid cursor": {
                "retCode": 0,
                "result": {"list": [], "nextPageCursor": 3},
            },
        }
        for label, response in malformed.items():
            with self.subTest(label=label):
                client = BybitClient("", "", True)
                client._request = lambda *args, _response=response, **kwargs: _response
                with self.assertRaisesRegex(RuntimeError, "pagination"):
                    client.get_open_orders("MUUSDT")

        client = BybitClient("", "", True)
        client._request = lambda *args, **kwargs: {
            "retCode": 0,
            "result": {
                "list": [bybit_order(), bybit_order()],
                "nextPageCursor": "",
            },
        }
        with self.assertRaisesRegex(RuntimeError, "duplicate exchange order ID"):
            client.get_open_orders("MUUSDT")

    def test_bybit_realtime_query_error_never_falls_back_to_stale_history(self):
        client = BybitClient("", "", True)
        calls = []

        def request(method, path, **kwargs):
            calls.append(path)
            return {"retCode": 10016, "retMsg": "service unavailable"}

        client._request = request
        response = client.get_order("MUUSDT", "1001")

        self.assertEqual(response["retCode"], 10016)
        self.assertEqual(calls, ["/v5/order/realtime"])

        client._request = lambda *args, **kwargs: []
        with self.assertRaisesRegex(RuntimeError, "response must be an object"):
            client.get_order_by_link("MUUSDT", "g_1_S_snapshot")

    def test_binance_style_single_ack_identity_is_never_guessed(self):
        factories = (
            lambda: BinanceFuturesClient("", "", True),
            lambda: AsterFuturesClient("", "", base_url="https://example.test"),
        )
        for factory in factories:
            client = factory()
            with self.subTest(client=type(client).__name__, case="minimal valid"):
                client._request = lambda *args, **kwargs: {
                    "orderId": 77,
                    "clientOrderId": "g_1_B_ack",
                }
                result = client.place_order(**self.order_request())["result"]
                self.assertEqual(result["orderId"], "77")
                self.assertEqual(result["orderLinkId"], "g_1_B_ack")
                self.assertNotIn("side", result)

            corruptions = {
                "missing order id": {"clientOrderId": "g_1_B_ack"},
                "missing client id": {"orderId": 77},
                "wrong client id": {
                    "orderId": 77,
                    "clientOrderId": "another-link",
                },
                "wrong symbol": {
                    "orderId": 77,
                    "clientOrderId": "g_1_B_ack",
                    "symbol": "OTHERUSDT",
                },
                "invalid present quantity": {
                    "orderId": 77,
                    "clientOrderId": "g_1_B_ack",
                    "origQty": "NaN",
                },
            }
            for label, response in corruptions.items():
                with self.subTest(client=type(client).__name__, case=label):
                    client._request = (
                        lambda *args, _response=response, **kwargs: _response
                    )
                    with self.assertRaises(ExchangeRequestUncertainError):
                        client.place_order(**self.order_request())

    def test_binance_style_batch_ack_requires_one_unique_identity_per_request(self):
        factories = (
            lambda: BinanceFuturesClient("", "", True),
            lambda: AsterFuturesClient("", "", base_url="https://example.test"),
        )
        requests = [self.order_request("g_1_B_ack"), self.order_request("g_2_B_ack")]
        malformed = {
            "short response": [
                {"orderId": 1, "clientOrderId": "g_1_B_ack"},
            ],
            "wrong link": [
                {"orderId": 1, "clientOrderId": "g_1_B_ack"},
                {"orderId": 2, "clientOrderId": "wrong-link"},
            ],
            "duplicate order id": [
                {"orderId": 1, "clientOrderId": "g_1_B_ack"},
                {"orderId": 1, "clientOrderId": "g_2_B_ack"},
            ],
            "unidentified item": [
                {"orderId": 1, "clientOrderId": "g_1_B_ack"},
                None,
            ],
            "zero error without identity": [
                {"orderId": 1, "clientOrderId": "g_1_B_ack"},
                {"code": 0},
            ],
        }
        for factory in factories:
            client = factory()
            with self.subTest(client=type(client).__name__, case="valid"):
                client._request = lambda *args, **kwargs: [
                    {"orderId": 1, "clientOrderId": "g_1_B_ack"},
                    {"orderId": 2, "clientOrderId": "g_2_B_ack"},
                ]
                result = client.place_orders(requests)["result"]["list"]
                self.assertEqual([row["retCode"] for row in result], [0, 0])

            for label, response in malformed.items():
                with self.subTest(client=type(client).__name__, case=label):
                    client._request = (
                        lambda *args, _response=response, **kwargs: _response
                    )
                    with self.assertRaises(ExchangeRequestUncertainError):
                        client.place_orders(requests)

            with self.subTest(client=type(client).__name__, case="definitive error"):
                client._request = lambda *args, **kwargs: [
                    {"code": -2010, "msg": "order rejected"}
                ]
                rejected = client.place_orders([requests[0]])["result"]["list"][0]
                self.assertEqual(rejected["retCode"], -2010)

    def test_bybit_single_ack_identity_is_validated_before_success(self):
        client = BybitClient("", "", True)
        valid = {
            "retCode": 0,
            "result": {"orderId": "77", "orderLinkId": "g_1_B_ack"},
        }
        client._request = lambda *args, **kwargs: valid
        result = client.place_order(**self.order_request())["result"]
        self.assertEqual(result["orderId"], "77")

        for response in (
            {"retCode": 0, "result": {}},
            {"retCode": 0, "result": {"orderId": "77"}},
            {
                "retCode": 0,
                "result": {"orderId": "77", "orderLinkId": "wrong-link"},
            },
            [],
        ):
            with self.subTest(response=response):
                client._request = (
                    lambda *args, _response=response, **kwargs: _response
                )
                with self.assertRaises(ExchangeRequestUncertainError):
                    client.place_order(**self.order_request())

        rejected = {"retCode": 10001, "retMsg": "parameter error"}
        client._request = lambda *args, **kwargs: rejected
        self.assertIs(client.place_order(**self.order_request()), rejected)

    def test_binance_style_cancel_ack_requires_full_cancelled_order(self):
        factories = (
            lambda: BinanceFuturesClient("", "", True),
            lambda: AsterFuturesClient("", "", base_url="https://example.test"),
        )
        valid = binance_order(status="CANCELED")
        valid.pop("time")
        corruptions = {
            "missing side": lambda row: row.pop("side"),
            "wrong order": lambda row: row.__setitem__("orderId", 1002),
            "wrong symbol": lambda row: row.__setitem__("symbol", "OTHERUSDT"),
            "not cancelled": lambda row: row.__setitem__("status", "NEW"),
            "execution beyond quantity": lambda row: row.__setitem__(
                "executedQty", "2.01"
            ),
        }
        for factory in factories:
            client = factory()
            with self.subTest(client=type(client).__name__, case="valid"):
                client._request = lambda *args, **kwargs: copy.deepcopy(valid)
                result = client.cancel_order("MUUSDT", "1001")["result"]
                self.assertEqual(result["orderId"], "1001")
                self.assertEqual(result["orderStatus"], "CANCELED")

            for label, mutate in corruptions.items():
                with self.subTest(client=type(client).__name__, case=label):
                    response = copy.deepcopy(valid)
                    mutate(response)
                    client._request = (
                        lambda *args, _response=response, **kwargs: _response
                    )
                    with self.assertRaises(ExchangeRequestUncertainError):
                        client.cancel_order("MUUSDT", "1001")

    def test_bybit_cancel_ack_identity_is_validated_before_success(self):
        client = BybitClient("", "", True)
        valid = {
            "retCode": 0,
            "result": {"orderId": "1001", "orderLinkId": "g_1_S_snapshot"},
        }
        client._request = lambda *args, **kwargs: valid
        result = client.cancel_order("MUUSDT", "1001")["result"]
        self.assertEqual(result["orderId"], "1001")

        for response in (
            {"retCode": 0, "result": {}},
            {
                "retCode": 0,
                "result": {"orderId": "1002", "orderLinkId": "g_1_S_snapshot"},
            },
            [],
        ):
            with self.subTest(response=response):
                client._request = (
                    lambda *args, _response=response, **kwargs: _response
                )
                with self.assertRaises(ExchangeRequestUncertainError):
                    client.cancel_order("MUUSDT", "1001")

        rejected = {"retCode": 10001, "retMsg": "parameter error"}
        client._request = lambda *args, **kwargs: rejected
        self.assertIs(client.cancel_order("MUUSDT", "1001"), rejected)


if __name__ == "__main__":
    unittest.main()
