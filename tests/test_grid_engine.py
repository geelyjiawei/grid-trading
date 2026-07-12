import asyncio
import copy
import logging
import random
import sys
import time
import unittest
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from unittest.mock import patch


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from exchange_errors import ExchangeRateLimitError  # noqa: E402
import grid_engine  # noqa: E402
from grid_engine import GridEngine  # noqa: E402


class FakeClient:
    def __init__(
        self,
        ticker_price="100",
        tick_size="0.1",
        qty_step="0.1",
        min_qty="0.1",
        min_notional="0",
        market_qty_step=None,
        market_min_qty=None,
        max_market_qty="0",
    ):
        self.orders = []
        self.order_seq = 0
        self.ticker_price = float(ticker_price)
        self.tick_size = str(tick_size)
        self.qty_step = str(qty_step)
        self.min_qty = str(min_qty)
        self.min_notional = str(min_notional)
        self.market_qty_step = str(market_qty_step or qty_step)
        self.market_min_qty = str(market_min_qty or min_qty)
        self.max_market_qty = str(max_market_qty)
        self.open_limit_order_ids = set()
        self.positions = []
        self.reject_post_only_reduce = False
        self.reject_reduce_limit = False
        self.cancelled_orders = []
        self.instant_fill_reduce_limits = False
        self.maker_fee_rate = "0.0002"
        self.taker_fee_rate = "0.0005"
        self.fee_rate_calls = []

    def get_instrument_info(self, symbol):
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "priceFilter": {"tickSize": self.tick_size},
                        "lotSizeFilter": {
                            "qtyStep": self.qty_step,
                            "minOrderQty": self.min_qty,
                            "minNotionalValue": self.min_notional,
                        },
                        "marketLotSizeFilter": {
                            "qtyStep": self.market_qty_step,
                            "minOrderQty": self.market_min_qty,
                            "maxOrderQty": self.max_market_qty,
                        },
                    }
                ]
            },
        }

    def set_leverage(self, symbol, leverage):
        return {"retCode": 0}

    def get_ticker(self, symbol):
        return {"retCode": 0, "result": {"list": [{"lastPrice": str(self.ticker_price)}]}}

    def get_fee_rates(self, symbol):
        self.fee_rate_calls.append(symbol)
        return {
            "retCode": 0,
            "result": {
                "symbol": symbol,
                "makerFeeRate": self.maker_fee_rate,
                "takerFeeRate": self.taker_fee_rate,
                "source": "exchange",
                "fetchedAt": 1714012800000,
            },
        }

    def place_order(self, **kwargs):
        if (
            self.reject_post_only_reduce
            and kwargs.get("order_type") == "Limit"
            and kwargs.get("reduce_only")
            and kwargs.get("time_in_force") == "PostOnly"
        ):
            return {"retCode": 400, "retMsg": "Post Only order will be rejected"}
        if (
            self.reject_reduce_limit
            and kwargs.get("order_type") == "Limit"
            and kwargs.get("reduce_only")
            and kwargs.get("time_in_force") != "PostOnly"
        ):
            return {"retCode": 400, "retMsg": "Reduce limit rejected"}
        self.order_seq += 1
        order = dict(kwargs)
        order["orderId"] = str(self.order_seq)
        self.orders.append(order)
        if (
            kwargs.get("order_type") == "Limit"
            and kwargs.get("reduce_only")
            and self.instant_fill_reduce_limits
        ):
            order["orderStatus"] = "FILLED"
        elif kwargs.get("order_type") == "Limit":
            self.open_limit_order_ids.add(order["orderId"])
        if kwargs.get("order_type") == "Market":
            self._apply_market_position(order)
        return {"retCode": 0, "result": {"orderId": order["orderId"]}}

    def _apply_market_position(self, order):
        side = order.get("side")
        qty = float(order.get("qty") or 0)
        if qty <= 0:
            return

        if order.get("reduce_only"):
            target_side = "Sell" if side == "Buy" else "Buy"
            for position in list(self.positions):
                if position.get("side") != target_side:
                    continue
                new_size = max(0.0, float(position.get("size") or 0) - qty)
                if new_size <= 0:
                    self.positions.remove(position)
                else:
                    position["size"] = str(new_size)
                return
            return

        for position in self.positions:
            if position.get("side") == side:
                position["size"] = str(float(position.get("size") or 0) + qty)
                position.setdefault("avgPrice", order.get("price", str(self.ticker_price)))
                return
        self.positions.append(
            {
                "side": side,
                "size": str(qty),
                "avgPrice": order.get("price", str(self.ticker_price)),
            }
        )

    def cancel_all_orders(self, symbol):
        self.open_limit_order_ids = {
            oid
            for oid in self.open_limit_order_ids
            if next((order for order in self.orders if str(order.get("orderId")) == oid), {}).get("symbol") != symbol
        }
        return {"retCode": 0}

    def cancel_order(self, symbol, order_id):
        self.cancelled_orders.append(str(order_id))
        self.open_limit_order_ids.discard(str(order_id))
        order = next(
            (item for item in self.orders if str(item.get("orderId")) == str(order_id)),
            None,
        )
        if order:
            order["orderStatus"] = "CANCELED"
        return {"retCode": 0}

    def get_open_orders(self, symbol):
        by_id = {
            str(order["orderId"]): order
            for order in self.orders
            if order.get("symbol") == symbol
        }
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "orderId": oid,
                        "orderLinkId": by_id.get(oid, {}).get("order_link_id", ""),
                        "side": by_id.get(oid, {}).get("side", ""),
                        "price": by_id.get(oid, {}).get("price", "0"),
                        "qty": by_id.get(oid, {}).get("qty", "0"),
                        "orderStatus": "NEW",
                        "reduceOnly": by_id.get(oid, {}).get("reduce_only", False),
                        "createdTime": "1",
                    }
                    for oid in sorted(self.open_limit_order_ids)
                    if oid in by_id
                ]
            },
        }

    def get_order(self, symbol, order_id):
        order = next((item for item in self.orders if str(item.get("orderId")) == str(order_id)), None)
        if not order:
            return {"retCode": 0, "result": {}}
        if str(order_id) in self.open_limit_order_ids:
            status = "NEW"
        else:
            status = order.get("orderStatus", "FILLED")
        return {
            "retCode": 0,
            "result": {
                "orderId": str(order_id),
                "orderStatus": status,
                "side": order.get("side", ""),
                "price": order.get("price", "0"),
                "qty": order.get("qty", "0"),
                "reduceOnly": order.get("reduce_only", False),
            },
        }

    def get_order_by_link(self, symbol, order_link_id):
        order = next(
            (
                item
                for item in self.orders
                if item.get("symbol") == symbol
                and str(item.get("order_link_id", "")) == str(order_link_id)
            ),
            None,
        )
        if not order:
            return {"retCode": 0, "result": {}}
        response = self.get_order(symbol, str(order["orderId"]))
        response["result"]["orderLinkId"] = str(order_link_id)
        return response

    def get_positions(self, symbol):
        return {"retCode": 0, "result": {"list": self.positions}}

    def round_to_step(self, value, step):
        step_decimal = Decimal(str(step))
        value_decimal = Decimal(str(value))
        rounded = (
            (value_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN)
            * step_decimal
        )
        decimals = max(0, -step_decimal.as_tuple().exponent)
        return f"{rounded:.{decimals}f}"


class BatchFakeClient(FakeClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_calls = []

    def place_orders(self, orders):
        self.batch_calls.append([dict(order) for order in orders])
        return {
            "retCode": 0,
            "result": {
                "list": [self.place_order(**order) for order in orders],
            },
        }


def fake_trade_response(client, order_id):
    explicit = getattr(client, "trade_details", {}).get(str(order_id), [])
    if explicit:
        return {"retCode": 0, "result": {"list": explicit}}
    order = next(
        (item for item in client.orders if str(item.get("orderId")) == str(order_id)),
        None,
    )
    if order and order.get("order_type") == "Market":
        qty = float(order.get("qty") or 0)
        price = float(client.ticker_price)
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "price": str(price),
                        "qty": str(qty),
                        "volume": str(price * qty),
                        "feeUsdt": "0",
                        "feeAsset": "USDT",
                        "isMaker": False,
                    }
                ]
            },
        }
    return {"retCode": 0, "result": {"list": []}}


class GridEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_grid_uses_batch_orders_when_supported(self):
        client = BatchFakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        self.assertGreater(len(client.batch_calls), 0)
        self.assertTrue(
            all(order.get("order_type") == "Limit" for batch in client.batch_calls for order in batch)
        )
        self.assertEqual(
            len([order for order in client.orders if order.get("order_type") == "Limit"]),
            len(engine.active_orders),
        )

    async def test_reconcile_does_not_fetch_trade_details_for_new_open_orders(self):
        client = FakeClient("100")
        trade_detail_calls = []
        client.get_order_trades = lambda symbol, order_id: trade_detail_calls.append(order_id) or {
            "retCode": 0,
            "result": {"list": []},
        }
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        trade_detail_calls.clear()
        changed = engine._reconcile_exchange_open_orders()

        self.assertFalse(changed)
        self.assertEqual(trade_detail_calls, [])

    async def test_reconcile_syncs_exchange_accepted_open_order_quantity(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        link_id, active_order = next(iter(engine.active_orders.items()))
        active_order["qty"] = "100"
        exchange_order = next(
            item for item in client.orders if str(item.get("orderId")) == str(active_order["order_id"])
        )
        exchange_order["qty"] = "70"

        changed = engine._reconcile_exchange_open_orders()

        self.assertTrue(changed)
        self.assertEqual(engine.active_orders[link_id]["qty"], "70")

    async def test_fixed_grid_qty_sets_initial_position_from_active_grid_count(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 2,
                "leverage": 5,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        market_open = next(order for order in client.orders if order.get("order_type") == "Market")
        reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(market_open["qty"], "4.0")
        self.assertEqual(engine.config["active_grid_count"], 2)
        self.assertEqual(sorted(order["qty"] for order in reduce_orders), ["2.0", "2.0"])

    async def test_initial_market_open_rejects_exchange_max_before_submission(self):
        client = FakeClient(
            "100",
            tick_size="1",
            qty_step="0.1",
            min_qty="0.1",
            max_market_qty="0.4",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "market maximum"):
            await engine.initialize()

        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])
        self.assertIsNone(engine.opening_order)

    async def test_fixed_grid_market_open_rejects_quantity_step_drift(self):
        client = FakeClient(
            "100",
            tick_size="1",
            qty_step="0.1",
            min_qty="0.1",
            market_qty_step="0.3",
            market_min_qty="0.3",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 2,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "cannot be represented exactly"):
            await engine.initialize()

        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])

    async def test_precision_prefers_bybit_market_max_over_limit_max(self):
        client = FakeClient("100")

        def instrument_info(symbol):
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "priceFilter": {"tickSize": "0.1"},
                            "lotSizeFilter": {
                                "qtyStep": "0.001",
                                "minOrderQty": "0.001",
                                "maxOrderQty": "1000",
                                "maxMktOrderQty": "120",
                            },
                        }
                    ]
                },
            }

        client.get_instrument_info = instrument_info
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
            },
        )

        engine._fetch_precision()

        self.assertEqual(engine.max_market_qty, 120.0)

    async def test_quantity_rounding_snaps_only_binary_float_dust_to_grid_step(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
            },
        )
        engine._fetch_precision()

        self.assertEqual(engine._order_qty_text(3.1399999999999997, reduce_only=True), "3.14")
        self.assertEqual(engine._order_qty_text(3.139999, reduce_only=True), "3.13")
        self.assertEqual(engine._qty_to_steps(0.19999999999999996), 20)

    async def test_fragmented_exchange_fills_keep_exact_counter_quantity(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.01")
        client.get_order_trades = lambda symbol, order_id: {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "price": "110",
                        "qty": "0.18",
                        "volume": "19.80",
                        "feeUsdt": "0",
                        "feeAsset": "USDT",
                        "isMaker": True,
                    },
                    {
                        "price": "110",
                        "qty": "0.02",
                        "volume": "2.20",
                        "feeUsdt": "0",
                        "feeAsset": "USDT",
                        "isMaker": True,
                    },
                ]
            },
        }
        engine = GridEngine(
            client,
            {
                "symbol": "MUUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [90, 110]
        engine.target_qty_by_level = {"0": 0.2}
        engine.reduce_lots_complete = True
        order = {
            "link_id": "g_0_S_mu_fragmented",
            "order_id": "mu-fragmented",
            "level_idx": 0,
            "side": "Sell",
            "price": "110",
            "qty": "0.20",
            "status": "FILLED",
            "order_type": "Limit",
            "time_in_force": "GTC",
            "reduce_only": False,
            "entry_price": None,
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
        }

        handled = engine._handle_closed_order(order, allow_estimate=False)

        self.assertTrue(handled)
        self.assertEqual(order["processed_fill_qty"], 0.2)
        self.assertEqual(
            Decimal(str(engine.reduce_lots_by_level["0"]["qty"])),
            Decimal("0.2"),
        )
        self.assertEqual(Decimal(str(engine.grid_position_net_qty)), Decimal("-0.2"))
        counter_orders = [item for item in client.orders if item["reduce_only"]]
        self.assertEqual(len(counter_orders), 1)
        self.assertEqual(counter_orders[0]["qty"], "0.20")

    async def test_repeated_position_updates_remain_exact_on_quantity_step(self):
        for direction, open_side, reduce_side, expected in (
            ("short", "Sell", "Buy", Decimal("-3.74")),
            ("long", "Buy", "Sell", Decimal("3.74")),
        ):
            client = FakeClient("100", qty_step="0.01", min_qty="0.01")
            engine = GridEngine(client, {"symbol": "MUUSDT", "direction": direction})
            engine._fetch_precision()

            for _ in range(374):
                engine._apply_grid_position_fill(
                    {"side": open_side, "reduce_only": False},
                    0.01,
                )

            self.assertEqual(
                Decimal(str(engine.grid_position_net_qty)),
                expected,
                msg=direction,
            )

            for _ in range(374):
                engine._apply_grid_position_fill(
                    {"side": reduce_side, "reduce_only": True},
                    0.01,
                )

            self.assertEqual(
                Decimal(str(engine.grid_position_net_qty)),
                Decimal("0"),
                msg=direction,
            )

    async def test_repeated_trade_value_updates_do_not_accumulate_float_dust(self):
        engine = GridEngine(FakeClient("100"), {"symbol": "MUUSDT", "direction": "short"})

        for _ in range(1000):
            engine._record_trade_value(
                1,
                0.001,
                gross_profit=0.0003,
                volume=0.001,
                fee=0.0002,
                fee_asset="USDT",
                fee_source="exchange",
            )

        self.assertEqual(Decimal(str(engine.total_volume)), Decimal("1.0"))
        self.assertEqual(Decimal(str(engine.total_fee)), Decimal("0.2"))
        self.assertEqual(Decimal(str(engine.gross_profit)), Decimal("0.3"))
        self.assertEqual(Decimal(str(engine.total_profit)), Decimal("0.1"))

    async def test_reduce_protection_detects_level_gaps_even_when_total_matches(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.grid_levels = engine._calculate_levels()
        engine.grid_position_net_qty = -2.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "1": {"qty": 1.0, "entry_value": 100.0},
            "2": {"qty": 1.0, "entry_value": 100.0},
        }
        placed = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="2.0",
            price="103.3",
            order_type="Limit",
            reduce_only=True,
            order_link_id="wrong",
        )
        wrong_order_id = str(placed["result"]["orderId"])
        engine.active_orders = {
            "wrong": {
                "link_id": "wrong",
                "order_id": wrong_order_id,
                "level_idx": 2,
                "side": "Buy",
                "price": "103.3",
                "qty": "2.0",
                "reduce_only": True,
                "order_type": "Limit",
                "time_in_force": "GTC",
                "entry_price": 100,
            }
        }

        snapshot = engine.reduce_protection_snapshot()

        self.assertTrue(snapshot["has_risk"])
        self.assertEqual(snapshot["missing_by_level"][0]["level"], 1)
        self.assertEqual(snapshot["excess_by_level"][0]["level"], 2)

        self.assertTrue(engine._handle_reduce_protection_level_risk())
        self.assertEqual(engine.active_orders, {})
        self.assertIn(wrong_order_id, client.cancelled_orders)

        self.assertTrue(engine._handle_reduce_protection_level_risk())
        reduce_orders = [
            order for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(
            sorted((order["level_idx"], float(order["qty"])) for order in reduce_orders),
            [(1, 1.0), (2, 1.0)],
        )

    async def test_reduce_protection_incomplete_ledger_warns_without_guessing_orders(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.grid_levels = engine._calculate_levels()
        engine.grid_position_net_qty = -1.0
        engine.reduce_lots_complete = False
        engine.reduce_lots_by_level = {}
        engine.active_orders = {}

        snapshot = engine.reduce_protection_snapshot()

        self.assertTrue(snapshot["has_risk"])
        self.assertFalse(snapshot["ledger_ok"])
        self.assertTrue(engine._handle_reduce_protection_level_risk())
        self.assertEqual(engine.active_orders, {})
        self.assertIn("Reduce protection risk", engine.trigger_message)

    async def test_reduce_lot_ledger_rebuilds_from_exchange_open_reduce_orders(self):
        client = FakeClient("0.386", tick_size="0.001", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.386,
                "lower_price": 0.38,
                "grid_count": 3,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.38, 0.382, 0.384, 0.386]
        engine.grid_position_net_qty = -3
        engine.reduce_lots_complete = False
        engine.reduce_lots_by_level = {"0": {"qty": 1, "entry_value": 0.382}}
        engine.completed_pairs = 1
        engine.filled_orders = []
        engine.active_orders = {
            "g_1_B_a": {
                "link_id": "g_1_B_a",
                "order_id": "1",
                "level_idx": 1,
                "side": "Buy",
                "price": "0.382",
                "qty": "1",
                "reduce_only": True,
                "entry_price": 0.384,
            },
            "g_2_B_b": {
                "link_id": "g_2_B_b",
                "order_id": "2",
                "level_idx": 2,
                "side": "Buy",
                "price": "0.384",
                "qty": "2",
                "reduce_only": True,
                "entry_price": 0.386,
            },
        }
        client.orders = [
            {
                "symbol": "TESTUSDT",
                "orderId": "1",
                "order_link_id": "g_1_B_a",
                "side": "Buy",
                "price": "0.382",
                "qty": "1",
                "reduce_only": True,
                "order_type": "Limit",
            },
            {
                "symbol": "TESTUSDT",
                "orderId": "2",
                "order_link_id": "g_2_B_b",
                "side": "Buy",
                "price": "0.384",
                "qty": "2",
                "reduce_only": True,
                "order_type": "Limit",
            },
        ]
        client.open_limit_order_ids = {"1", "2"}

        snapshot = engine.reduce_protection_snapshot()

        self.assertFalse(snapshot["has_risk"])
        self.assertTrue(snapshot["ledger_ok"])
        self.assertTrue(engine.reduce_lots_complete)
        self.assertEqual(
            engine.reduce_lots_by_level,
            {
                "1": {"qty": 1.0, "entry_value": 0.384},
                "2": {"qty": 2.0, "entry_value": 0.772},
            },
        )
        self.assertIn("rebuilt from current exchange", engine.trigger_message)

    async def test_reduce_lot_ledger_does_not_rebuild_when_exchange_reduce_qty_mismatches(self):
        client = FakeClient("0.386", tick_size="0.001", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.386,
                "lower_price": 0.38,
                "grid_count": 3,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.38, 0.382, 0.384, 0.386]
        engine.grid_position_net_qty = -3
        engine.completed_pairs = 1
        client.orders = [
            {
                "symbol": "TESTUSDT",
                "orderId": "1",
                "order_link_id": "g_1_B_a",
                "side": "Buy",
                "price": "0.382",
                "qty": "1",
                "reduce_only": True,
                "order_type": "Limit",
            }
        ]
        client.open_limit_order_ids = {"1"}

        snapshot = engine.reduce_protection_snapshot()

        self.assertTrue(snapshot["has_risk"])
        self.assertFalse(engine.reduce_lots_complete)
        self.assertNotIn("rebuilt from current exchange", engine.trigger_message)

    async def test_reduce_lot_ledger_does_not_rebuild_from_manual_reduce_order(self):
        client = FakeClient("0.386", tick_size="0.001", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.386,
                "lower_price": 0.38,
                "grid_count": 3,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.38, 0.382, 0.384, 0.386]
        engine.grid_position_net_qty = -1
        engine.completed_pairs = 1
        client.orders = [
            {
                "symbol": "TESTUSDT",
                "orderId": "manual",
                "order_link_id": "",
                "side": "Buy",
                "price": "0.382",
                "qty": "1",
                "reduce_only": True,
                "order_type": "Limit",
            }
        ]
        client.open_limit_order_ids = {"manual"}

        snapshot = engine.reduce_protection_snapshot()

        self.assertTrue(snapshot["has_risk"])
        self.assertFalse(engine.reduce_lots_complete)
        self.assertNotIn("rebuilt from current exchange", engine.trigger_message)

    async def test_reduce_protection_risk_still_restores_filled_counter_orders(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.grid_levels = engine._calculate_levels()
        engine.grid_position_net_qty = -2.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "1": {"qty": 1.0, "entry_value": 100.0},
            "2": {"qty": 1.0, "entry_value": 100.0},
        }
        engine.active_orders = {
            "wrong": {
                "link_id": "wrong",
                "order_id": "wrong",
                "level_idx": 2,
                "side": "Buy",
                "price": "100",
                "qty": "2",
                "reduce_only": True,
                "processed_fill_qty": 0,
                "processed_fill_volume": 0,
                "processed_fill_fee": 0,
            }
        }

        reduce_fill = {
            "link_id": "filled",
            "order_id": "filled",
            "level_idx": 1,
            "side": "Buy",
            "price": "95",
            "qty": "1",
            "reduce_only": True,
            "processed_fill_qty": 0,
            "processed_fill_volume": 0,
            "processed_fill_fee": 0,
        }
        engine._record_execution_delta(
            reduce_fill,
            {
                "price": 95,
                "qty": 1,
                "volume": 95,
                "fee": 0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
            },
        )

        open_orders = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Sell" and not order.get("reduce_only") and order.get("level_idx") == 1
        ]
        self.assertEqual(len(open_orders), 1)
        self.assertEqual(engine.paused_replacements, [])

    async def test_reduce_protection_risk_still_places_reduce_counter_orders(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.grid_levels = engine._calculate_levels()
        engine.grid_position_net_qty = -2.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "1": {"qty": 1.0, "entry_value": 100.0},
            "2": {"qty": 1.0, "entry_value": 100.0},
        }
        engine.active_orders = {
            "wrong": {
                "link_id": "wrong",
                "order_id": "wrong",
                "level_idx": 2,
                "side": "Buy",
                "price": "100",
                "qty": "2",
                "reduce_only": True,
                "processed_fill_qty": 0,
                "processed_fill_volume": 0,
                "processed_fill_fee": 0,
            }
        }

        add_fill = {
            "link_id": "filled",
            "order_id": "filled",
            "level_idx": 1,
            "side": "Sell",
            "price": "100",
            "qty": "1",
            "reduce_only": False,
            "processed_fill_qty": 0,
            "processed_fill_volume": 0,
            "processed_fill_fee": 0,
        }
        engine._record_execution_delta(
            add_fill,
            {
                "price": 100,
                "qty": 1,
                "volume": 100,
                "fee": 0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
            },
        )

        reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("level_idx") == 1
        ]
        self.assertEqual(len(reduce_orders), 1)
        self.assertEqual(float(reduce_orders[0]["qty"]), 1.0)
        self.assertEqual(engine.paused_replacements, [])

    async def test_fixed_grid_qty_limit_open_uses_limit_price_for_initial_qty(self):
        client = FakeClient("1012", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 1020,
                "lower_price": 1000,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 5,
                "initial_order_type": "post_only",
                "initial_order_price": 1014,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        opening_order = next(order for order in client.orders if order.get("order_link_id", "").startswith("open_"))
        self.assertTrue(engine.waiting_initial_order)
        self.assertEqual(opening_order["price"], "1014.0")
        self.assertEqual(opening_order["qty"], "2.8")
        self.assertEqual(engine.config["active_grid_count"], 14)
        self.assertAlmostEqual(engine.config["derived_total_qty"], 2.8)
        self.assertEqual(len(engine._pending_targets["profit_targets"]), 14)

    async def test_regular_limit_open_honors_marketable_user_price(self):
        client = FakeClient("1012", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 1020,
                "lower_price": 1000,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 5,
                "initial_order_type": "limit",
                "initial_order_price": 1008,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        opening_order = next(order for order in client.orders if order.get("order_link_id", "").startswith("open_"))
        self.assertEqual(opening_order["price"], "1008.0")
        self.assertEqual(opening_order["qty"], "1.6")
        self.assertEqual(engine.config["active_grid_count"], 8)

    async def test_limit_open_allows_current_outside_range_when_limit_is_inside(self):
        client = FakeClient("990", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 1020,
                "lower_price": 1000,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 5,
                "initial_order_type": "limit",
                "initial_order_price": 1014,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        opening_order = next(order for order in client.orders if order.get("order_link_id", "").startswith("open_"))
        self.assertEqual(opening_order["price"], "1014.0")
        self.assertEqual(opening_order["qty"], "2.8")
        self.assertEqual(engine.config["active_grid_count"], 14)

    async def test_post_only_crossing_price_uses_maker_safe_reference(self):
        client = FakeClient("1014", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 1020,
                "lower_price": 1000,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 5,
                "initial_order_type": "post_only",
                "initial_order_price": 1012,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        opening_order = next(order for order in client.orders if order.get("order_link_id", "").startswith("open_"))
        self.assertEqual(opening_order["price"], "1014.1")
        self.assertEqual(opening_order["qty"], "3.0")
        self.assertEqual(engine.config["active_grid_count"], 15)

    async def test_initial_limit_order_uses_gtc_not_post_only(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "initial_order_type": "limit",
                "initial_order_price": 101,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        opening_order = next(order for order in client.orders if order.get("order_link_id", "").startswith("open_"))
        self.assertTrue(engine.waiting_initial_order)
        self.assertEqual(opening_order["price"], "101.0")
        self.assertIsNone(opening_order.get("time_in_force"))
        self.assertEqual(engine.opening_order["time_in_force"], "GTC")

    async def test_fast_poll_window_wakes_loop_after_order_activity(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._wake_event = asyncio.Event()

        self.assertEqual(engine._poll_interval(), 3.0)
        engine._mark_fast_poll()

        self.assertLess(engine._poll_interval(), 1.0)
        self.assertTrue(engine._wake_event.is_set())

    async def test_waiting_initial_order_uses_fast_poll(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )
        engine.waiting_initial_order = True

        self.assertLess(engine._poll_interval(), 1.0)

    async def test_user_stream_events_only_wake_relevant_symbol(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        self.assertTrue(
            engine._is_relevant_user_stream_event({"e": "ORDER_TRADE_UPDATE", "o": {"s": "TESTUSDT"}})
        )
        self.assertFalse(
            engine._is_relevant_user_stream_event({"e": "ORDER_TRADE_UPDATE", "o": {"s": "OTHERUSDT"}})
        )
        self.assertFalse(engine._is_relevant_user_stream_event({"e": "ACCOUNT_UPDATE", "a": {}}))

    async def test_user_stream_disconnect_closes_its_listen_key(self):
        client = FakeClient("100")
        closed_keys = []
        client.start_user_stream = lambda: "listen-key-1"
        client.keepalive_user_stream = lambda listen_key: {"retCode": 0}
        client.close_user_stream = lambda listen_key: closed_keys.append(listen_key) or {
            "retCode": 0
        }
        client.user_stream_url = lambda listen_key: f"wss://example.invalid/{listen_key}"
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})

        class FakeSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def recv(self):
                engine.running = False
                return '{"e":"ACCOUNT_UPDATE"}'

        class FakeWebsockets:
            @staticmethod
            def connect(*args, **kwargs):
                return FakeSocket()

        engine.running = True
        with patch.dict(sys.modules, {"websockets": FakeWebsockets}):
            await engine._user_stream_loop()

        self.assertEqual(closed_keys, ["listen-key-1"])
        self.assertEqual(engine._user_stream_listen_key, "")

    async def test_user_stream_keepalive_failure_closes_key_and_reconnects(self):
        client = FakeClient("100")
        started_keys = []
        closed_keys = []
        keepalive_calls = 0

        def start_user_stream():
            key = f"listen-key-{len(started_keys) + 1}"
            started_keys.append(key)
            return key

        def keepalive_user_stream(listen_key):
            nonlocal keepalive_calls
            keepalive_calls += 1
            if keepalive_calls == 1:
                raise RuntimeError("simulated keepalive failure")
            engine.running = False
            return {"retCode": 0}

        client.start_user_stream = start_user_stream
        client.keepalive_user_stream = keepalive_user_stream
        client.close_user_stream = lambda listen_key: closed_keys.append(listen_key) or {
            "retCode": 0
        }
        client.user_stream_url = lambda listen_key: f"wss://example.invalid/{listen_key}"
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})

        class FakeSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def recv(self):
                await asyncio.sleep(60)

        class FakeWebsockets:
            @staticmethod
            def connect(*args, **kwargs):
                return FakeSocket()

        engine.running = True
        with (
            patch.object(grid_engine, "USER_STREAM_KEEPALIVE_SECONDS", 0),
            patch.object(grid_engine, "USER_STREAM_RECONNECT_SECONDS", 0),
            patch.dict(sys.modules, {"websockets": FakeWebsockets}),
        ):
            await asyncio.wait_for(engine._user_stream_loop(), timeout=1)

        self.assertEqual(started_keys, ["listen-key-1", "listen-key-2"])
        self.assertEqual(closed_keys, ["listen-key-1", "listen-key-2"])
        self.assertEqual(keepalive_calls, 2)
        self.assertEqual(engine._user_stream_listen_key, "")

    async def test_user_stream_keepalive_rate_limit_registers_cooldown_before_retry(self):
        client = FakeClient("100")
        closed_keys = []
        client.start_user_stream = lambda: "listen-key-rate-limit"
        client.keepalive_user_stream = lambda listen_key: (_ for _ in ()).throw(
            ExchangeRateLimitError("listen key rate limited", retry_after=17)
        )
        client.close_user_stream = lambda listen_key: closed_keys.append(listen_key) or {
            "retCode": 0
        }
        client.user_stream_url = lambda listen_key: f"wss://example.invalid/{listen_key}"
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})

        class FakeSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def recv(self):
                await asyncio.sleep(60)

        class FakeWebsockets:
            @staticmethod
            def connect(*args, **kwargs):
                return FakeSocket()

        engine.running = True
        with (
            patch.object(grid_engine, "USER_STREAM_KEEPALIVE_SECONDS", 0),
            patch.dict(sys.modules, {"websockets": FakeWebsockets}),
        ):
            stream_task = asyncio.create_task(engine._user_stream_loop())
            for _ in range(100):
                if closed_keys:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(closed_keys, ["listen-key-rate-limit"])
            self.assertGreater(engine._rate_limit_remaining(), 15)
            stream_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await stream_task

        self.assertEqual(engine._user_stream_listen_key, "")

    async def test_stopping_user_stream_cancels_blocked_receive_and_closes_key(self):
        client = FakeClient("100")
        connected = asyncio.Event()
        closed_keys = []
        client.start_user_stream = lambda: "listen-key-stop"
        client.keepalive_user_stream = lambda listen_key: {"retCode": 0}
        client.close_user_stream = lambda listen_key: closed_keys.append(listen_key) or {
            "retCode": 0
        }
        client.user_stream_url = lambda listen_key: f"wss://example.invalid/{listen_key}"
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})

        class FakeSocket:
            async def __aenter__(self):
                connected.set()
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def recv(self):
                await asyncio.sleep(60)

        class FakeWebsockets:
            @staticmethod
            def connect(*args, **kwargs):
                return FakeSocket()

        engine.running = True
        with patch.dict(sys.modules, {"websockets": FakeWebsockets}):
            engine._user_stream_task = asyncio.create_task(engine._user_stream_loop())
            await asyncio.wait_for(connected.wait(), timeout=1)
            await engine._stop_user_stream()

        self.assertIsNone(engine._user_stream_task)
        self.assertEqual(closed_keys, ["listen-key-stop"])
        self.assertEqual(engine._user_stream_listen_key, "")

    async def test_short_grid_deploys_market_short_buys_below_and_sells_above(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        market_orders = [o for o in client.orders if o.get("order_type") == "Market"]
        limit_orders = [o for o in client.orders if o.get("order_type") == "Limit"]
        buy_reduce_orders = [o for o in limit_orders if o["side"] == "Buy" and o["reduce_only"]]
        sell_open_orders = [o for o in limit_orders if o["side"] == "Sell" and not o["reduce_only"]]

        self.assertEqual(len(market_orders), 1)
        self.assertEqual(market_orders[0]["side"], "Sell")
        self.assertGreater(len(buy_reduce_orders), 0)
        self.assertGreater(len(sell_open_orders), 0)
        self.assertTrue(all(o.get("time_in_force") is None for o in limit_orders))

        market_qty = sum(float(o["qty"]) for o in market_orders)
        reduce_qty = sum(float(o["qty"]) for o in buy_reduce_orders)
        self.assertAlmostEqual(market_qty, reduce_qty)

    async def test_short_grid_records_existing_same_side_position_as_baseline(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "882", "avgPrice": "0.64"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        status = engine.get_status()
        self.assertEqual(status["baseline_position"]["side"], "Sell")
        self.assertEqual(status["baseline_position"]["qty"], 882)
        self.assertGreater(status["grid_position_qty"], 0)
        self.assertAlmostEqual(
            status["expected_position_net_qty"],
            -(882 + status["grid_position_qty"]),
        )

    async def test_short_grid_rejects_opposite_existing_position(self):
        client = FakeClient("100")
        client.positions = [{"side": "Buy", "size": "2.0", "avgPrice": "99"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "would be offset"):
            await engine.initialize()

    async def test_restore_derives_grid_position_from_active_reduce_orders(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "445", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine.restore_state(
            {
                "running": True,
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {
                    "g_1_B_restore": {
                        "link_id": "g_1_B_restore",
                        "order_id": "1",
                        "level_idx": 1,
                        "side": "Buy",
                        "price": "95.0",
                        "qty": "445",
                        "status": "open",
                        "order_type": "Limit",
                        "time_in_force": "GTC",
                        "reduce_only": True,
                        "entry_price": 100,
                    }
                },
                "filled_orders": [],
            }
        )

        self.assertEqual(engine.get_status()["grid_position_net_qty"], -445)

    async def test_legacy_restore_derives_only_remaining_partial_reduce_quantity(self):
        client = FakeClient("100", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
            },
        )

        engine.restore_state(
            {
                "running": False,
                "config": engine.config,
                "grid_levels": [90, 100, 110],
                "active_orders": {
                    "g_0_B_partial": {
                        "link_id": "g_0_B_partial",
                        "order_id": "1",
                        "level_idx": 0,
                        "side": "Buy",
                        "price": "90",
                        "qty": "1.0",
                        "processed_fill_qty": 0.4,
                        "reduce_only": True,
                    }
                },
            }
        )

        self.assertAlmostEqual(engine.grid_position_net_qty, -0.6)

    async def test_legacy_neutral_restore_initializes_net_position_from_fills(self):
        client = FakeClient("100", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
            },
        )

        engine.restore_state(
            {
                "running": False,
                "config": engine.config,
                "grid_levels": [90, 100, 110],
                "filled_orders": [
                    {"side": "Buy", "qty": 2.0},
                    {"side": "Sell", "qty": 0.7},
                ],
            }
        )

        self.assertAlmostEqual(engine.grid_position_net_qty, 1.3)

    async def test_restore_migrates_same_side_unmanaged_position_to_baseline(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "1238", "avgPrice": "0.6405"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine.restore_state(
            {
                "running": True,
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {
                    "g_1_B_restore": {
                        "link_id": "g_1_B_restore",
                        "order_id": "1",
                        "level_idx": 1,
                        "side": "Buy",
                        "price": "95.0",
                        "qty": "445",
                        "status": "open",
                        "order_type": "Limit",
                        "time_in_force": "GTC",
                        "reduce_only": True,
                        "entry_price": 100,
                    }
                },
                "filled_orders": [],
            }
        )

        status = engine.get_status()
        self.assertEqual(status["grid_position_net_qty"], -445)
        self.assertEqual(status["baseline_position"]["side"], "Sell")
        self.assertEqual(status["baseline_position"]["qty"], 793)
        self.assertEqual(status["expected_position_net_qty"], -1238)

    async def test_reconcile_does_not_guess_boundary_reduce_when_short_level_unknown(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "528", "avgPrice": "0.6097"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.restore_state(
            {
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {
                    "g_2_S_restore": {
                        "link_id": "g_2_S_restore",
                        "order_id": "1",
                        "level_idx": 2,
                        "side": "Sell",
                        "price": "105.0",
                        "qty": "51",
                        "status": "open",
                        "order_type": "Limit",
                        "time_in_force": "GTC",
                        "reduce_only": False,
                        "entry_price": None,
                    }
                },
                "grid_position_net_qty": -457,
                "initial_entry_price": 100,
            }
        )

        engine._reconcile_grid_position_protection()

        status = engine.get_status()
        self.assertEqual(status["grid_position_net_qty"], -457)
        self.assertFalse(status["position_ledger_consistent"])
        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(repair_orders, [])

    async def test_position_sync_preserves_ownership_and_lots_on_unexplained_delta(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        client.positions = [{"side": "Sell", "size": "3", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -2
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 1, "entry_value": 100},
            "1": {"qty": 1, "entry_value": 100},
        }

        engine._sync_grid_position_with_exchange()
        engine._position_mismatch_seen_at = 0
        engine._sync_grid_position_with_exchange()

        self.assertEqual(engine.grid_position_net_qty, -2)
        self.assertTrue(engine.reduce_lots_complete)
        self.assertEqual(
            engine.reduce_lots_by_level,
            {
                "0": {"qty": 1, "entry_value": 100},
                "1": {"qty": 1, "entry_value": 100},
            },
        )
        self.assertIn("Position ledger mismatch", engine.trigger_message)

    async def test_restore_discards_incomplete_reduce_lots(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine.restore_state(
            {
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "reduce_lots_complete": False,
                "reduce_lots_by_level": {"2": {"qty": 72, "entry_value": 29.232}},
            }
        )

        self.assertFalse(engine.reduce_lots_complete)
        self.assertEqual(engine.reduce_lots_by_level, {})

    async def test_reconcile_repairs_missing_short_reduce_protection_from_fill_level(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Sell", "size": "0.2", "avgPrice": "105"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -0.2
        engine.filled_orders = [
            {
                "side": "Sell",
                "price": 105,
                "qty": 0.2,
                "level_idx": 2,
                "reduce_only": False,
            }
        ]

        engine._reconcile_grid_position_protection()

        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(len(repair_orders), 1)
        self.assertEqual(repair_orders[0]["price"], "100.0")
        self.assertEqual(repair_orders[0]["qty"], "0.20")

    async def test_reconcile_repairs_missing_reduce_from_persisted_lot_ledger(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Sell", "size": "0.2", "avgPrice": "105"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -0.2
        engine.completed_pairs = 100
        engine.filled_orders = []
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"2": {"qty": 0.2, "entry_value": 21.0}}

        engine._reconcile_grid_position_protection()

        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(len(repair_orders), 1)
        self.assertEqual(repair_orders[0]["price"], "100.0")
        self.assertEqual(repair_orders[0]["qty"], "0.20")

    async def test_record_fill_updates_reduce_lot_ledger(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.reduce_lots_complete = True

        engine._record_fill(
            {"side": "Sell", "price": 105, "qty": "0.2", "level_idx": 2, "reduce_only": False},
            {"price": 105, "qty": 0.2, "volume": 21.0, "fee": 0, "fee_asset": "USDT", "fee_source": "test"},
        )

        self.assertEqual(engine.reduce_lots_by_level, {"2": {"qty": 0.2, "entry_value": 21.0}})

        engine._record_fill(
            {"side": "Buy", "price": 100, "qty": "0.05", "level_idx": 2, "reduce_only": True},
            {"price": 100, "qty": 0.05, "volume": 5.0, "fee": 0, "fee_asset": "USDT", "fee_source": "test"},
        )

        self.assertAlmostEqual(engine.reduce_lots_by_level["2"]["qty"], 0.15)
        self.assertAlmostEqual(engine.reduce_lots_by_level["2"]["entry_value"], 15.75)

    async def test_reconcile_repairs_only_missing_reduce_deficit_from_fill_ledger(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Sell", "size": "0.2", "avgPrice": "105"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -0.2
        engine.active_orders = {
            "existing": {
                "link_id": "existing",
                "order_id": "existing",
                "level_idx": 2,
                "side": "Buy",
                "price": "100.0",
                "qty": "0.1",
                "reduce_only": True,
                "entry_price": 105,
            }
        }
        engine.filled_orders = [
            {
                "side": "Sell",
                "price": 105,
                "qty": 0.2,
                "level_idx": 2,
                "reduce_only": False,
            }
        ]

        engine._reconcile_grid_position_protection()

        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(len(repair_orders), 2)
        self.assertAlmostEqual(sum(float(order["qty"]) for order in repair_orders), 0.2)
        self.assertEqual(sorted(float(order["qty"]) for order in repair_orders), [0.1, 0.1])

    async def test_reconcile_repairs_missing_long_reduce_protection_from_fill_level(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Buy", "size": "0.2", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = 0.2
        engine.filled_orders = [
            {
                "side": "Buy",
                "price": 100,
                "qty": 0.2,
                "level_idx": 2,
                "reduce_only": False,
            }
        ]

        engine._reconcile_grid_position_protection()

        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and order["reduce_only"]
        ]
        self.assertEqual(len(repair_orders), 1)
        self.assertEqual(repair_orders[0]["price"], "105.0")
        self.assertEqual(repair_orders[0]["qty"], "0.20")

    async def test_reconcile_does_not_guess_boundary_reduce_when_fill_history_is_truncated(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Sell", "size": "0.2", "avgPrice": "105"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -0.2
        engine.completed_pairs = 1
        engine.filled_orders = [
            {
                "side": "Sell",
                "price": 105,
                "qty": 0.2,
                "level_idx": 2,
                "reduce_only": False,
            }
        ]

        engine._reconcile_grid_position_protection()

        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(repair_orders, [])
        self.assertNotIn("boundary reduce-only fallback", engine.trigger_message)

    async def test_reconcile_skips_fill_ledger_when_reduce_protection_is_complete(self):
        client = FakeClient("100", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Sell", "size": "0.2", "avgPrice": "105"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -0.2
        engine.completed_pairs = 10
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"2": {"qty": 0.2, "entry_value": 21.0}}
        engine.active_orders = {
            "existing": {
                "link_id": "existing",
                "order_id": "existing",
                "level_idx": 2,
                "side": "Buy",
                "price": "100.0",
                "qty": "0.2",
                "reduce_only": True,
                "entry_price": 105,
            }
        }

        def fail_if_called():
            raise AssertionError("fill ledger rebuild should not run when reduce protection is complete")

        engine._reduce_lots_from_fill_ledger = fail_if_called
        engine.trigger_message = "Repaired 1 missing reduce-only protection order(s) from fill ledger: 0.01"

        engine._reconcile_grid_position_protection()

        self.assertEqual(engine.trigger_message, "")

    async def test_reconcile_does_not_erase_grid_ownership_when_exchange_is_flat(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.restore_state(
            {
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {},
                "grid_position_net_qty": -457,
            }
        )

        engine._reconcile_grid_position_protection()

        status = engine.get_status()
        self.assertEqual(status["grid_position_net_qty"], -457)
        self.assertFalse(status["position_ledger_consistent"])

    async def test_manual_same_side_position_is_never_absorbed_into_grid_ownership(self):
        client = FakeClient("100", qty_step="0.1", min_qty="0.1")
        client.positions = [{"side": "Sell", "size": "5", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]
        engine.running = True
        engine.grid_ready = True
        engine.baseline_position_side = "Sell"
        engine.baseline_position_qty = 3.0
        engine.baseline_position_entry_price = 100.0
        engine.grid_position_net_qty = -1.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"0": {"qty": 1.0, "entry_value": 100.0}}
        engine.active_orders = {
            "g_0_B_grid": {
                "link_id": "g_0_B_grid",
                "order_id": "1",
                "level_idx": 0,
                "side": "Buy",
                "price": "90",
                "qty": "1.0",
                "status": "NEW",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 100,
                "processed_fill_qty": 0.0,
            }
        }

        engine._reconcile_grid_position_protection()
        engine._position_mismatch_seen_at = 0
        engine._reconcile_grid_position_protection()

        self.assertEqual(engine.baseline_position_qty, 3.0)
        self.assertEqual(engine.grid_position_net_qty, -1.0)
        self.assertEqual(
            engine.reduce_lots_by_level,
            {"0": {"qty": 1.0, "entry_value": 100.0}},
        )
        self.assertEqual(len(engine.active_orders), 1)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.cancelled_orders, [])
        self.assertIn("Position ledger mismatch", engine.trigger_message)

    async def test_reduce_order_qty_is_capped_to_remaining_grid_position(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "1.2", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -1.2
        engine.initial_entry_price = 100
        engine.active_orders = {
            "g_1_B_existing": {
                "link_id": "g_1_B_existing",
                "order_id": "1",
                "level_idx": 1,
                "side": "Buy",
                "price": "95.0",
                "qty": "1.0",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 100,
            }
        }

        placed = engine._place(
            "Buy",
            95,
            1,
            reduce_only=True,
            qty_override=0.5,
            entry_price=100,
            allow_duplicate=True,
        )

        self.assertIsNotNone(placed)
        self.assertAlmostEqual(float(engine.active_orders[placed]["qty"]), 0.5)
        active_reduce_qty = sum(
            float(order["qty"])
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        )
        self.assertAlmostEqual(active_reduce_qty, 1.5)

    async def test_reduce_order_is_not_placed_when_grid_position_has_no_allowance(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "1.0", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.restore_state(
            {
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {
                    "g_1_B_existing": {
                        "link_id": "g_1_B_existing",
                        "order_id": "1",
                        "level_idx": 1,
                        "side": "Buy",
                        "price": "95.0",
                        "qty": "1.0",
                        "status": "open",
                        "order_type": "Limit",
                        "time_in_force": "GTC",
                        "reduce_only": True,
                        "entry_price": 100,
                    }
                },
                "grid_position_net_qty": -1.0,
                "initial_entry_price": 100,
            }
        )
        before_order_count = len(client.orders)

        placed = engine._place(
            "Buy",
            95,
            1,
            reduce_only=True,
            qty_override=0.5,
            entry_price=100,
            allow_duplicate=True,
        )

        self.assertIsNotNone(placed)
        self.assertEqual(len(client.orders), before_order_count + 1)

    async def test_reduce_overcommit_trims_excess_without_stopping(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -1.0
        engine.initial_entry_price = 100
        engine.active_orders = {
            "g_1_B_existing": {
                "link_id": "g_1_B_existing",
                "order_id": "1",
                "level_idx": 1,
                "side": "Buy",
                "price": "95.0",
                "qty": "1.5",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 100,
            }
        }
        engine.running = True
        engine.grid_ready = True

        engine._reconcile_grid_position_protection()

        self.assertTrue(engine.running)
        self.assertTrue(engine.grid_ready)
        self.assertAlmostEqual(engine._active_reduce_qty("Buy"), 1.5)
        self.assertEqual(client.cancelled_orders, [])
        self.assertAlmostEqual(engine._active_reduce_qty("Buy"), 1.5)
        replacement_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(len(replacement_orders), 1)
        self.assertEqual(replacement_orders[0]["price"], "95.0")
        self.assertAlmostEqual(float(replacement_orders[0]["qty"]), 1.5)
        self.assertEqual(engine.trigger_message, "")

    async def test_fixed_grid_partial_reduce_fill_reopens_exact_filled_qty(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        filled_reduce = {
            "side": "Buy",
            "price": "95",
            "qty": "41",
            "level_idx": 1,
            "reduce_only": True,
            "fill_price": 95,
        }
        engine.reduce_lots_by_level["1"] = {
            "qty": 59.0,
            "entry_value": float(engine.reduce_lots_by_level["1"]["entry_value"]) * 0.59,
        }

        placed = engine._place_counter_order(filled_reduce)

        self.assertTrue(placed)
        reopen_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 1
        )
        self.assertEqual(reopen_order["qty"], "41")

    async def test_reduce_counter_order_does_not_net_against_existing_partial_reduce(self):
        client = FakeClient("0.282", tick_size="0.00001", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.2877,
                "lower_price": 0.2603,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 200,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.2603 + (0.2877 - 0.2603) / 20 * i for i in range(21)]
        engine.active_orders = {
            "existing_partial_reduce": {
                "link_id": "existing_partial_reduce",
                "order_id": "existing_partial_reduce",
                "level_idx": 11,
                "side": "Buy",
                "price": str(engine.grid_levels[11]),
                "qty": "119",
                "status": "PARTIALLY_FILLED",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.27674,
                "processed_fill_qty": 82.0,
            }
        }

        placed = engine._place_counter_order(
            {
                "side": "Sell",
                "price": "0.27674",
                "qty": "82",
                "level_idx": 11,
                "reduce_only": False,
                "fill_price": 0.27674,
            }
        )

        placed_reduce_orders = [
            order
            for order in client.orders
            if order["side"] == "Buy" and order["reduce_only"] and order["price"] == "0.27537"
        ]
        self.assertTrue(placed)
        self.assertEqual(len(placed_reduce_orders), 1)
        self.assertEqual(placed_reduce_orders[0]["qty"], "82")

    async def test_reduce_protection_counts_partial_reduce_remaining_qty(self):
        client = FakeClient("0.282", tick_size="0.00001", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.2877,
                "lower_price": 0.2603,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 200,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.2603 + (0.2877 - 0.2603) / 20 * i for i in range(21)]
        engine.grid_position_net_qty = -119
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"11": {"qty": 119.0, "entry_value": 32.93206}}
        engine.active_orders = {
            "existing_partial_reduce": {
                "link_id": "existing_partial_reduce",
                "order_id": "existing_partial_reduce",
                "level_idx": 11,
                "side": "Buy",
                "price": str(engine.grid_levels[11]),
                "qty": "119",
                "status": "PARTIALLY_FILLED",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.27674,
                "processed_fill_qty": 82.0,
            }
        }

        snapshot = engine.reduce_protection_snapshot()

        self.assertTrue(snapshot["has_risk"])
        self.assertEqual(snapshot["active_reduce_qty"], 37.0)
        self.assertEqual(snapshot["missing_by_level"][0]["level"], 11)
        self.assertEqual(snapshot["missing_by_level"][0]["active_qty"], 37.0)
        self.assertEqual(snapshot["missing_by_level"][0]["missing_qty"], 82.0)

        repaired = engine._repair_missing_reduce_protection_from_ledger()
        repair_orders = [
            order
            for order in client.orders
            if order["side"] == "Buy" and order["reduce_only"] and order["price"] == "0.27537"
        ]
        self.assertTrue(repaired)
        self.assertEqual(len(repair_orders), 1)
        self.assertEqual(repair_orders[0]["qty"], "82")

    async def test_open_side_coverage_repairs_missing_level_qty_from_lot_ledger(self):
        client = FakeClient("0.282", tick_size="0.00001", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.2877,
                "lower_price": 0.2603,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 200,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [0.2603 + (0.2877 - 0.2603) / 20 * i for i in range(21)]
        engine.target_qty_by_level = {str(i): 200.0 for i in range(20)}
        engine.grid_position_net_qty = -300
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 120.0, "entry_value": 31.4},
            "1": {"qty": 180.0, "entry_value": 47.2},
        }
        engine.active_orders = {
            "level0_reduce": {
                "link_id": "level0_reduce",
                "order_id": "level0_reduce",
                "level_idx": 0,
                "side": "Buy",
                "price": str(engine.grid_levels[0]),
                "qty": "120",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.26167,
                "processed_fill_qty": 0.0,
            },
            "level1_reduce": {
                "link_id": "level1_reduce",
                "order_id": "level1_reduce",
                "level_idx": 1,
                "side": "Buy",
                "price": str(engine.grid_levels[1]),
                "qty": "180",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.26304,
                "processed_fill_qty": 0.0,
            },
            "level0_open_partial": {
                "link_id": "level0_open_partial",
                "order_id": "level0_open_partial",
                "level_idx": 0,
                "side": "Sell",
                "price": str(engine.grid_levels[1]),
                "qty": "30",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "entry_price": None,
                "processed_fill_qty": 0.0,
            },
        }

        repaired = engine._repair_open_side_coverage_from_lots()

        placed_open_orders = [
            order
            for order in client.orders
            if order["side"] == "Sell" and not order["reduce_only"]
        ]
        placed_by_level = {
            int(order["order_link_id"].split("_")[1]): order["qty"]
            for order in placed_open_orders
        }
        self.assertTrue(repaired)
        self.assertEqual(placed_by_level[0], "50")
        self.assertEqual(placed_by_level[1], "20")
        self.assertEqual(placed_by_level[2], "200")
        self.assertEqual(placed_by_level[19], "200")
        self.assertEqual(len(placed_open_orders), 20)

        repaired_again = engine._repair_open_side_coverage_from_lots()
        placed_open_orders_again = [
            order
            for order in client.orders
            if order["side"] == "Sell" and not order["reduce_only"]
        ]
        self.assertFalse(repaired_again)
        self.assertEqual(len(placed_open_orders_again), len(placed_open_orders))

    async def test_long_open_side_coverage_repairs_missing_level_qty_from_lot_ledger(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 120,
                "lower_price": 100,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 50,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [100, 105, 110, 115, 120]
        engine.target_qty_by_level = {str(i): 50.0 for i in range(4)}
        engine.grid_position_net_qty = 70
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 20.0, "entry_value": 2000.0},
            "1": {"qty": 50.0, "entry_value": 5250.0},
        }
        engine.active_orders = {
            "level0_reduce": {
                "link_id": "level0_reduce",
                "order_id": "level0_reduce",
                "level_idx": 0,
                "side": "Sell",
                "price": "105",
                "qty": "20",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 100,
                "processed_fill_qty": 0.0,
            },
            "level1_reduce": {
                "link_id": "level1_reduce",
                "order_id": "level1_reduce",
                "level_idx": 1,
                "side": "Sell",
                "price": "110",
                "qty": "50",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 105,
                "processed_fill_qty": 0.0,
            },
            "level0_open_partial": {
                "link_id": "level0_open_partial",
                "order_id": "level0_open_partial",
                "level_idx": 0,
                "side": "Buy",
                "price": "100",
                "qty": "10",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "entry_price": None,
                "processed_fill_qty": 0.0,
            },
        }

        repaired = engine._repair_open_side_coverage_from_lots()

        placed_open_orders = [
            order
            for order in client.orders
            if order["side"] == "Buy" and not order["reduce_only"]
        ]
        placed_by_level = {
            int(order["order_link_id"].split("_")[1]): order["qty"]
            for order in placed_open_orders
        }
        self.assertTrue(repaired)
        self.assertEqual(placed_by_level[0], "20")
        self.assertNotIn(1, placed_by_level)
        self.assertEqual(placed_by_level[2], "50")
        self.assertEqual(placed_by_level[3], "50")

    async def test_fixed_grid_fragmented_reduce_fills_restore_exact_level_target(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "MUUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [90, 110]
        engine.target_qty_by_level = {"0": 0.2}
        engine.reduce_lots_complete = True

        # The first reduce fragment leaves 0.18 in the position lot.
        engine.reduce_lots_by_level = {"0": {"qty": 0.18, "entry_value": 19.8}}
        self.assertTrue(
            engine._place_counter_order(
                {
                    "side": "Buy",
                    "price": "90",
                    "qty": "0.02",
                    "level_idx": 0,
                    "reduce_only": True,
                    "fill_price": 90,
                }
            )
        )

        # The second fragment closes the lot. Existing 0.02 plus the new
        # counter must total 0.20, not shrink to 0.18.
        engine.reduce_lots_by_level = {}
        self.assertTrue(
            engine._place_counter_order(
                {
                    "side": "Buy",
                    "price": "90",
                    "qty": "0.18",
                    "level_idx": 0,
                    "reduce_only": True,
                    "fill_price": 90,
                }
            )
        )

        reopened_qty = sum(
            float(order["qty"])
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"]
        )
        self.assertAlmostEqual(reopened_qty, 0.2)
        self.assertEqual(sorted(float(order["qty"]) for order in client.orders), [0.02, 0.18])

    async def test_fixed_grid_oversized_reduce_fill_reopens_only_level_target(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "MUUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [90, 110]
        engine.target_qty_by_level = {"0": 0.2}
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {}

        self.assertTrue(
            engine._place_counter_order(
                {
                    "side": "Buy",
                    "price": "90",
                    "qty": "0.21",
                    "level_idx": 0,
                    "reduce_only": True,
                    "fill_price": 90,
                }
            )
        )

        reopened = next(order for order in client.orders if not order["reduce_only"])
        self.assertEqual(reopened["qty"], "0.20")

    async def test_grid_coverage_snapshot_detects_mu_level_shrink_and_excess(self):
        client = FakeClient("991", tick_size="1", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "MUUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 955,
                "lower_price": 935,
                "grid_count": 6,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [935, 938, 941, 944, 947, 950, 955]
        engine.target_qty_by_level = {str(level): 0.2 for level in range(6)}
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 0.21, "entry_value": 196.6},
            "1": {"qty": 0.19, "entry_value": 178.6},
            "2": {"qty": 0.14, "entry_value": 132.0},
            "3": {"qty": 0.02, "entry_value": 18.9},
            "4": {"qty": 0.2, "entry_value": 190.0},
            "5": {"qty": 0.2, "entry_value": 191.0},
        }

        snapshot = engine.grid_coverage_snapshot()

        self.assertTrue(snapshot["has_risk"])
        self.assertAlmostEqual(snapshot["target_qty"], 1.2)
        self.assertAlmostEqual(snapshot["coverage_qty"], 0.96)
        self.assertAlmostEqual(snapshot["net_delta_qty"], -0.24)
        self.assertEqual([item["level"] for item in snapshot["missing_by_level"]], [1, 2, 3])
        self.assertAlmostEqual(
            sum(item["missing_qty"] for item in snapshot["missing_by_level"]),
            0.25,
        )
        self.assertEqual(snapshot["excess_by_level"][0]["level"], 0)
        self.assertAlmostEqual(snapshot["excess_by_level"][0]["excess_qty"], 0.01)

    async def test_reduce_lot_keeps_remainder_equal_to_qty_step_below_min_order_quantity(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        engine = GridEngine(client, {"symbol": "MUUSDT", "direction": "short"})
        engine._fetch_precision()
        lots = {
            4: {
                "qty": Decimal("0.20"),
                "entry_value": Decimal("188.00"),
            }
        }

        removed = engine._lot_remove(lots, 4, Decimal("0.19"))

        self.assertTrue(removed)
        self.assertEqual(lots[4]["qty"], Decimal("0.01"))
        self.assertEqual(lots[4]["entry_value"], Decimal("9.40"))

    async def test_partial_fill_below_min_order_quantity_is_recorded_without_rounding_up(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "MUUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "grid_order_post_only": False,
                "maker_fee_rate": 0,
                "taker_fee_rate": 0,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [90, 110]
        engine.target_qty_by_level = {"0": 0.2}
        engine.reduce_lots_complete = True
        order = {
            "link_id": "g_0_S_partial",
            "order_id": "open-1",
            "level_idx": 0,
            "side": "Sell",
            "price": "110",
            "qty": "0.20",
            "status": "PARTIALLY_FILLED",
            "order_type": "Limit",
            "time_in_force": "GTC",
            "reduce_only": False,
            "entry_price": None,
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
        }
        engine.active_orders = {order["link_id"]: order}

        recorded = engine._record_execution_delta(
            order,
            {
                "price": 110.0,
                "qty": 0.01,
                "volume": 1.1,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        self.assertTrue(recorded)
        self.assertEqual(order["processed_fill_qty"], 0.01)
        self.assertEqual(engine.reduce_lots_by_level["0"]["qty"], 0.01)
        self.assertEqual(engine.grid_position_net_qty, -0.01)
        reduce_orders = [item for item in client.orders if item.get("reduce_only")]
        self.assertEqual(len(reduce_orders), 1)
        self.assertEqual(reduce_orders[0]["qty"], "0.01")
        self.assertNotEqual(reduce_orders[0]["qty"], "0.10")

    async def test_exchange_reduce_lot_rebuild_uses_remaining_not_original_quantity(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="5")
        client.get_open_orders = lambda symbol: {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "orderId": "reduce-1",
                        "orderLinkId": "g_1_B_partial",
                        "side": "Buy",
                        "price": "100",
                        "qty": "10",
                        "executedQty": "3",
                        "orderStatus": "PARTIALLY_FILLED",
                        "reduceOnly": True,
                    }
                ]
            },
        }
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 10,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]
        engine.grid_position_net_qty = -7
        engine.active_orders = {
            "g_1_B_partial": {
                "link_id": "g_1_B_partial",
                "order_id": "reduce-1",
                "level_idx": 1,
                "side": "Buy",
                "price": "100",
                "qty": "10",
                "reduce_only": True,
                "entry_price": 110,
            }
        }

        lots, reason = engine._reduce_lots_from_exchange_open_orders()

        self.assertEqual(reason, "")
        self.assertIsNotNone(lots)
        self.assertEqual(lots[1]["qty"], Decimal("7"))
        self.assertEqual(lots[1]["entry_value"], Decimal("770"))

    async def test_mixed_missing_and_excess_grid_coverage_never_auto_adds_orders(self):
        client = FakeClient("991", tick_size="1", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "MUUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 955,
                "lower_price": 935,
                "grid_count": 6,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 3,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_ready = True
        engine.grid_levels = [935, 938, 941, 944, 947, 950, 955]
        engine.target_qty_by_level = {str(level): 0.2 for level in range(6)}
        engine.grid_position_net_qty = -0.96
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 0.21, "entry_value": 196.6},
            "1": {"qty": 0.19, "entry_value": 178.6},
            "2": {"qty": 0.14, "entry_value": 132.0},
            "3": {"qty": 0.02, "entry_value": 18.9},
            "4": {"qty": 0.2, "entry_value": 190.0},
            "5": {"qty": 0.2, "entry_value": 191.0},
        }

        handled = engine._repair_open_side_coverage_from_lots()

        self.assertTrue(handled)
        self.assertEqual(client.orders, [])
        self.assertIn("automatic top-up is paused", engine.trigger_message)

    async def test_restore_mu_mixed_coverage_reports_risk_without_mutating_orders(self):
        client = FakeClient("995.78", tick_size="0.01", qty_step="0.01", min_qty="0.01")
        client.positions = [{"side": "Sell", "size": "3.74", "avgPrice": "949.7754"}]
        config = {
            "exchange": "binance",
            "symbol": "MUUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 955,
            "lower_price": 935,
            "grid_count": 20,
            "total_investment": 0,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 0.2,
            "leverage": 5,
            "grid_order_post_only": False,
        }
        levels = [float(price) for price in range(935, 956)]
        fragments_by_level = [
            [0.20, 0.01],
            [0.20],
            [0.20],
            [0.20],
            [0.19],
            [0.19],
            [0.14],
            [0.20],
            [0.19],
            [0.20],
            [0.02],
            [0.20],
            [0.20],
            [0.20],
            [0.19, 0.01],
            [0.20],
            [0.19, 0.01],
            [0.20],
            [0.20],
            [0.20],
        ]
        quantities = [
            float(sum((Decimal(str(fragment)) for fragment in fragments), Decimal("0")))
            for fragments in fragments_by_level
        ]
        active_orders = {}
        for level_idx, fragments in enumerate(fragments_by_level):
            for fragment_idx, qty in enumerate(fragments):
                link_id = f"g_{level_idx}_B_mu_restore_{fragment_idx}"
                response = client.place_order(
                    symbol="MUUSDT",
                    side="Buy",
                    qty=str(qty),
                    price=str(levels[level_idx]),
                    order_type="Limit",
                    time_in_force="GTC",
                    reduce_only=True,
                    order_link_id=link_id,
                )
                active_orders[link_id] = {
                    "link_id": link_id,
                    "order_id": response["result"]["orderId"],
                    "level_idx": level_idx,
                    "side": "Buy",
                    "price": str(levels[level_idx]),
                    "qty": str(qty),
                    "status": "open",
                    "order_type": "Limit",
                    "time_in_force": "GTC",
                    "reduce_only": True,
                    "entry_price": 949.7754,
                }

        state = {
            "running": True,
            "config": config,
            "grid_levels": levels,
            "active_orders": active_orders,
            "reduce_lots_complete": True,
            "reduce_lots_by_level": {
                str(level_idx): {"qty": qty, "entry_value": qty * 949.7754}
                for level_idx, qty in enumerate(quantities)
            },
            "target_qty_by_level": {str(level_idx): 0.2 for level_idx in range(20)},
            "grid_position_net_qty": -3.74,
            "grid_ready": True,
            "tick_size": "0.01",
            "qty_step": "0.01",
            "min_qty": 0.01,
        }
        seed_order_count = len(client.orders)
        seed_open_ids = set(client.open_limit_order_ids)
        engine = GridEngine(client, config)

        engine.restore_state(state)

        coverage = engine.grid_coverage_snapshot()
        self.assertEqual(len(client.orders), seed_order_count)
        self.assertEqual(seed_order_count, 23)
        self.assertEqual(client.cancelled_orders, [])
        self.assertEqual(client.open_limit_order_ids, seed_open_ids)
        self.assertEqual(len(engine.active_orders), 23)
        self.assertEqual(
            sum(1 for order in engine.active_orders.values() if order["level_idx"] == 0),
            2,
        )
        self.assertEqual(
            sum(1 for order in engine.active_orders.values() if order["level_idx"] == 14),
            2,
        )
        self.assertEqual(
            sum(1 for order in engine.active_orders.values() if order["level_idx"] == 16),
            2,
        )
        self.assertAlmostEqual(coverage["target_qty"], 4.0)
        self.assertAlmostEqual(coverage["coverage_qty"], 3.74)
        self.assertAlmostEqual(
            sum(item["missing_qty"] for item in coverage["missing_by_level"]),
            0.27,
        )
        self.assertAlmostEqual(coverage["excess_by_level"][0]["excess_qty"], 0.01)
        self.assertAlmostEqual(coverage["net_delta_qty"], -0.26)
        self.assertTrue(coverage["has_risk"])
        self.assertIn("automatic top-up is paused", engine.trigger_message)

    async def test_mu_oversized_same_level_fragments_heal_to_one_exact_target(self):
        for fill_sequence in ((0.20, 0.01), (0.01, 0.20)):
            client = FakeClient("975", tick_size="1", qty_step="0.01", min_qty="0.01")
            engine = GridEngine(
                client,
                {
                    "symbol": "MUUSDT",
                    "direction": "short",
                    "grid_mode": "arithmetic",
                    "upper_price": 955,
                    "lower_price": 935,
                    "grid_count": 1,
                    "total_investment": 0,
                    "position_sizing_mode": "fixed_grid_qty",
                    "grid_order_qty": 0.2,
                    "qty_per_grid": 0.2,
                    "leverage": 5,
                    "grid_order_post_only": False,
                },
            )
            engine._fetch_precision()
            engine.grid_ready = True
            engine.grid_levels = [935, 936]
            engine.target_qty_by_level = {"0": 0.2}
            engine.reduce_lots_complete = True
            engine.reduce_lots_by_level = {
                "0": {"qty": 0.21, "entry_value": 0.21 * 949.7754}
            }
            engine.grid_position_net_qty = -0.21

            for order_index, qty in enumerate(fill_sequence):
                order = {
                    "link_id": f"g_0_B_mu_fragment_{order_index}",
                    "order_id": f"reduce-{order_index}",
                    "level_idx": 0,
                    "side": "Buy",
                    "price": "935",
                    "qty": str(qty),
                    "status": "FILLED",
                    "order_type": "Limit",
                    "time_in_force": "GTC",
                    "reduce_only": True,
                    "entry_price": 949.7754,
                    "processed_fill_qty": 0.0,
                    "processed_fill_volume": 0.0,
                    "processed_fill_fee": 0.0,
                }
                handled = engine._record_execution_delta(
                    order,
                    {
                        "price": 935.0,
                        "qty": qty,
                        "volume": 935.0 * qty,
                        "fee": 0.0,
                        "fee_asset": "USDT",
                        "fee_source": "exchange",
                        "maker_count": 1,
                        "taker_count": 0,
                    },
                )
                self.assertTrue(handled, msg=fill_sequence)

            reopened = [order for order in client.orders if not order["reduce_only"]]
            self.assertEqual(
                sum(Decimal(order["qty"]) for order in reopened),
                Decimal("0.20"),
                msg=fill_sequence,
            )
            self.assertEqual(engine.paused_replacements, [], msg=fill_sequence)
            self.assertEqual(engine.reduce_lots_by_level, {}, msg=fill_sequence)
            self.assertEqual(Decimal(str(engine.grid_position_net_qty)), Decimal("0"))

    async def test_baseline_loss_of_one_qty_step_halts_even_when_below_min_order_quantity(self):
        client = FakeClient("100", qty_step="0.01", min_qty="1")
        client.positions = [{"side": "Sell", "size": "2.99", "avgPrice": "100"}]
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})
        engine._fetch_precision()
        engine.baseline_position_side = "Sell"
        engine.baseline_position_qty = 3.0
        engine.running = True
        engine.grid_ready = True

        halted = engine._halt_if_baseline_breached()

        self.assertTrue(halted)
        self.assertFalse(engine.running)
        self.assertFalse(engine.grid_ready)

    async def test_baseline_breach_keeps_cleanup_running_when_cancel_is_unconfirmed(self):
        class UnconfirmedCancelClient(FakeClient):
            def cancel_order(self, symbol, order_id):
                raise TimeoutError("cancel acknowledgement lost")

        client = UnconfirmedCancelClient("100", qty_step="0.1", min_qty="0.1")
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "100"}]
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})
        engine._fetch_precision()
        engine.baseline_position_side = "Sell"
        engine.baseline_position_qty = 2.0
        engine.running = True
        engine.grid_ready = True
        link_id = engine._place("Buy", 95, 1, reduce_only=True, qty_override=0.5)

        halted = engine._halt_if_baseline_breached()

        self.assertTrue(halted)
        self.assertTrue(engine.running)
        self.assertTrue(engine.manual_stop_pending)
        self.assertFalse(engine.grid_ready)
        self.assertIn(link_id, engine.active_orders)

    async def test_unrealized_pnl_uses_partial_reduce_order_remaining_quantity(self):
        client = FakeClient("90", tick_size="1", qty_step="0.01", min_qty="0.1")
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})
        engine._fetch_precision()
        engine.grid_position_net_qty = -0.05
        engine.initial_entry_price = 100
        engine.active_orders = {
            "partial-reduce": {
                "side": "Buy",
                "qty": "0.10",
                "processed_fill_qty": 0.05,
                "reduce_only": True,
                "entry_price": 100,
            }
        }

        self.assertAlmostEqual(engine.estimate_grid_unrealized_pnl(90), 0.5)

    async def test_fixed_grid_random_fill_and_cancel_sequences_preserve_level_quantity(self):
        grid_logger = logging.getLogger("grid_engine")
        logger_was_disabled = grid_logger.disabled
        grid_logger.disabled = True
        self.addCleanup(setattr, grid_logger, "disabled", logger_was_disabled)

        for direction in ("short", "long"):
            for seed in range(20):
                rng = random.Random(seed)
                client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
                engine = GridEngine(
                    client,
                    {
                        "symbol": "TESTUSDT",
                        "direction": direction,
                        "grid_mode": "arithmetic",
                        "upper_price": 110,
                        "lower_price": 90,
                        "grid_count": 1,
                        "total_investment": 0,
                        "position_sizing_mode": "fixed_grid_qty",
                        "grid_order_qty": 20,
                        "leverage": 3,
                        "grid_order_post_only": False,
                        "maker_fee_rate": 0,
                        "taker_fee_rate": 0,
                        "trigger_price": None,
                        "stop_loss_price": None,
                        "take_profit_price": None,
                    },
                )
                engine._fetch_precision()
                engine.grid_ready = True
                engine.grid_levels = [90, 110]
                engine.target_qty_by_level = {"0": 20.0}
                engine.reduce_lots_complete = True
                open_side = "Sell" if direction == "short" else "Buy"
                open_price = 110 if direction == "short" else 90
                self.assertIsNotNone(
                    engine._place(
                        open_side,
                        open_price,
                        0,
                        reduce_only=False,
                        qty_override=20,
                    )
                )

                for _ in range(150):
                    candidates = [
                        order
                        for order in engine.active_orders.values()
                        if float(order.get("qty", 0))
                        - float(order.get("processed_fill_qty", 0) or 0)
                        >= engine.min_qty
                    ]
                    self.assertTrue(candidates)
                    order = rng.choice(candidates)
                    link_id = str(order["link_id"])
                    planned = int(float(order["qty"]))
                    processed = int(float(order.get("processed_fill_qty", 0) or 0))
                    remaining = planned - processed

                    if rng.random() < 0.2:
                        client.open_limit_order_ids.discard(str(order["order_id"]))
                        price = float(order["price"])

                        def cancelled_trades(symbol, order_id, *, _processed=processed, _price=price):
                            trades = []
                            if _processed > 0:
                                trades.append(
                                    {
                                        "qty": str(_processed),
                                        "price": str(_price),
                                        "volume": str(_processed * _price),
                                        "feeUsdt": "0",
                                        "feeAsset": "USDT",
                                        "isMaker": True,
                                    }
                                )
                            return {"retCode": 0, "result": {"list": trades}}

                        client.get_order_trades = cancelled_trades
                        engine._handle_cancelled_order(link_id, order)
                    else:
                        delta = rng.randint(1, remaining)
                        cumulative = processed + delta
                        price = float(order["price"])
                        stats = {
                            "price": price,
                            "qty": cumulative,
                            "volume": price * cumulative,
                            "fee": 0.0,
                            "fee_asset": "USDT",
                            "fee_source": "exchange",
                            "maker_count": 1,
                            "taker_count": 0,
                        }
                        self.assertTrue(engine._record_execution_delta(order, stats))
                        if cumulative == planned:
                            engine.active_orders.pop(link_id, None)
                            client.open_limit_order_ids.discard(str(order["order_id"]))

                    engine._repair_missing_reduce_protection_from_ledger()
                    engine._repair_open_side_coverage_from_lots()
                    coverage = engine.grid_coverage_snapshot()
                    protection = engine.reduce_protection_snapshot()
                    lot_qty = sum(
                        float(lot["qty"])
                        for lot in engine.reduce_lots_by_level.values()
                    )

                    self.assertFalse(
                        coverage["has_risk"],
                        msg=f"direction={direction} seed={seed} coverage={coverage}",
                    )
                    self.assertFalse(
                        protection["has_risk"],
                        msg=f"direction={direction} seed={seed} protection={protection}",
                    )
                    self.assertAlmostEqual(coverage["coverage_qty"], 20.0)
                    expected_position = -lot_qty if direction == "short" else lot_qty
                    self.assertAlmostEqual(engine.grid_position_net_qty, expected_position)

    async def test_twenty_level_random_transitions_preserve_every_level_quantity(self):
        grid_logger = logging.getLogger("grid_engine")
        logger_was_disabled = grid_logger.disabled
        grid_logger.disabled = True
        self.addCleanup(setattr, grid_logger, "disabled", logger_was_disabled)

        for direction in ("short", "long"):
            for seed in range(2):
                rng = random.Random(seed)
                client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
                engine = GridEngine(
                    client,
                    {
                        "symbol": "TESTUSDT",
                        "direction": direction,
                        "grid_mode": "arithmetic",
                        "upper_price": 110,
                        "lower_price": 90,
                        "grid_count": 20,
                        "total_investment": 0,
                        "position_sizing_mode": "fixed_grid_qty",
                        "grid_order_qty": 100,
                        "leverage": 2,
                        "grid_order_post_only": False,
                        "trigger_price": None,
                        "stop_loss_price": None,
                        "take_profit_price": None,
                    },
                )
                await engine.initialize()
                opening_qty = Decimal(str(engine.initial_qty))
                opening_value = Decimal(str(engine.initial_entry_price)) * opening_qty
                cashflow = opening_value if direction == "short" else -opening_value
                trade_net_qty = -opening_qty if direction == "short" else opening_qty

                for transition in range(100):
                    candidates = [
                        order
                        for order in engine.active_orders.values()
                        if float(order.get("qty", 0))
                        - float(order.get("processed_fill_qty", 0) or 0)
                        >= engine.min_qty
                    ]
                    self.assertTrue(
                        candidates,
                        msg=f"direction={direction} seed={seed} transition={transition}",
                    )
                    order = rng.choice(candidates)
                    link_id = str(order["link_id"])
                    planned = int(float(order["qty"]))
                    processed = int(float(order.get("processed_fill_qty", 0) or 0))
                    remaining = planned - processed

                    if rng.random() < 0.18:
                        client.cancel_order("TESTUSDT", str(order["order_id"]))
                        engine._handle_cancelled_order(link_id, order)
                    else:
                        cumulative = processed + rng.randint(1, remaining)
                        price = float(order["price"])
                        fill_qty = Decimal(cumulative - processed)
                        fill_value = Decimal(str(price)) * fill_qty
                        if order["side"] == "Buy":
                            cashflow -= fill_value
                            trade_net_qty += fill_qty
                        else:
                            cashflow += fill_value
                            trade_net_qty -= fill_qty
                        self.assertTrue(
                            engine._record_execution_delta(
                                order,
                                {
                                    "price": price,
                                    "qty": cumulative,
                                    "volume": price * cumulative,
                                    "fee": 0.0,
                                    "fee_asset": "USDT",
                                    "fee_source": "exchange",
                                    "maker_count": 1,
                                    "taker_count": 0,
                                },
                            )
                        )
                        if cumulative == planned:
                            engine.active_orders.pop(link_id, None)
                            client.open_limit_order_ids.discard(str(order["order_id"]))

                    engine._resume_paused_replacements()
                    engine._repair_missing_reduce_protection_from_ledger()
                    engine._repair_open_side_coverage_from_lots()
                    coverage = engine.grid_coverage_snapshot()
                    protection = engine.reduce_protection_snapshot()
                    context = (
                        f"direction={direction} seed={seed} transition={transition} "
                        f"coverage={coverage} protection={protection}"
                    )
                    self.assertFalse(coverage["has_risk"], msg=context)
                    self.assertFalse(protection["has_risk"], msg=context)
                    self.assertAlmostEqual(coverage["coverage_qty"], 2000.0)

                    lot_qty = sum(
                        float(lot["qty"])
                        for lot in engine.reduce_lots_by_level.values()
                    )
                    expected_position = -lot_qty if direction == "short" else lot_qty
                    self.assertAlmostEqual(engine.grid_position_net_qty, expected_position)
                    self.assertAlmostEqual(
                        float(trade_net_qty),
                        engine.grid_position_net_qty,
                        msg=context,
                    )

                    mark_price = Decimal(str(engine.current_price))
                    cashflow_gross_equity = cashflow + mark_price * trade_net_qty
                    accounted_gross_equity = Decimal(str(engine.gross_profit)) + Decimal(
                        str(engine.estimate_grid_unrealized_pnl(float(mark_price)))
                    )
                    self.assertAlmostEqual(
                        float(cashflow_gross_equity),
                        float(accounted_gross_equity),
                        places=7,
                        msg=context,
                    )
                    self.assertAlmostEqual(
                        float(cashflow_gross_equity - Decimal(str(engine.total_fee))),
                        engine.total_profit
                        + engine.estimate_grid_unrealized_pnl(float(mark_price)),
                        places=7,
                        msg=context,
                    )

    async def test_decimal_twenty_level_random_transitions_survive_restarts(self):
        grid_logger = logging.getLogger("grid_engine")
        logger_was_disabled = grid_logger.disabled
        grid_logger.disabled = True
        self.addCleanup(setattr, grid_logger, "disabled", logger_was_disabled)

        class FlakyDecimalClient(FakeClient):
            def __init__(self):
                super().__init__("100", tick_size="1", qty_step="0.01", min_qty="0.01")
                self.reject_next_limit = False

            def place_order(self, **kwargs):
                if self.reject_next_limit and kwargs.get("order_type") == "Limit":
                    self.reject_next_limit = False
                    return {"retCode": 503, "retMsg": "temporary deterministic rejection"}
                return super().place_order(**kwargs)

        for direction in ("short", "long"):
            for seed in range(10):
                rng = random.Random(seed)
                client = FlakyDecimalClient()
                config = {
                    "symbol": "TESTUSDT",
                    "direction": direction,
                    "grid_mode": "arithmetic",
                    "upper_price": 110,
                    "lower_price": 90,
                    "grid_count": 20,
                    "total_investment": 0,
                    "position_sizing_mode": "fixed_grid_qty",
                    "grid_order_qty": 0.2,
                    "leverage": 2,
                    "grid_order_post_only": False,
                    "maker_fee_rate": 0,
                    "taker_fee_rate": 0,
                    "trigger_price": None,
                    "stop_loss_price": None,
                    "take_profit_price": None,
                }
                engine = GridEngine(client, config)
                await engine.initialize()

                opening_qty = Decimal(str(engine.initial_qty))
                opening_value = Decimal(str(engine.initial_entry_price)) * opening_qty
                cashflow = opening_value if direction == "short" else -opening_value
                trade_net_qty = -opening_qty if direction == "short" else opening_qty
                qty_step = Decimal("0.01")

                for transition in range(120):
                    candidates = [
                        order
                        for order in engine.active_orders.values()
                        if Decimal(str(order.get("qty", 0) or 0))
                        - Decimal(str(order.get("processed_fill_qty", 0) or 0))
                        >= qty_step
                    ]
                    self.assertTrue(candidates)
                    order = rng.choice(candidates)
                    link_id = str(order["link_id"])
                    planned = Decimal(str(order["qty"]))
                    processed = Decimal(str(order.get("processed_fill_qty", 0) or 0))
                    remaining_steps = int((planned - processed) / qty_step)
                    client.reject_next_limit = rng.random() < 0.12

                    if rng.random() < 0.2:
                        client.cancel_order("TESTUSDT", str(order["order_id"]))
                        engine._handle_cancelled_order(link_id, order)
                    else:
                        cumulative = processed + qty_step * rng.randint(1, remaining_steps)
                        price = Decimal(str(order["price"]))
                        fill_qty = cumulative - processed
                        fill_value = price * fill_qty
                        if order["side"] == "Buy":
                            cashflow -= fill_value
                            trade_net_qty += fill_qty
                        else:
                            cashflow += fill_value
                            trade_net_qty -= fill_qty
                        self.assertTrue(
                            engine._record_execution_delta(
                                order,
                                {
                                    "price": float(price),
                                    "qty": str(cumulative),
                                    "volume": str(price * cumulative),
                                    "fee": 0.0,
                                    "fee_asset": "USDT",
                                    "fee_source": "exchange",
                                    "maker_count": 1,
                                    "taker_count": 0,
                                },
                            )
                        )
                        if cumulative == planned:
                            engine.active_orders.pop(link_id, None)
                            client.open_limit_order_ids.discard(str(order["order_id"]))

                    client.reject_next_limit = False
                    for queued in engine.paused_replacements:
                        queued["replacement_retry_after"] = 0.0
                    engine._resume_paused_replacements()
                    engine._repair_missing_reduce_protection_from_ledger()
                    engine._repair_open_side_coverage_from_lots()
                    self.assertEqual(engine.paused_replacements, [])

                    if (transition + 1) % 20 == 0:
                        position_qty = abs(trade_net_qty)
                        client.positions = []
                        if position_qty:
                            client.positions.append(
                                {
                                    "side": "Sell" if trade_net_qty < 0 else "Buy",
                                    "size": str(position_qty),
                                    "avgPrice": "100",
                                }
                            )
                        saved = copy.deepcopy(engine.to_state())
                        saved["running"] = True
                        restored = GridEngine(client, config)
                        restored.restore_state(saved)
                        engine = restored

                    coverage = engine.grid_coverage_snapshot()
                    protection = engine.reduce_protection_snapshot()
                    context = (
                        f"direction={direction} seed={seed} transition={transition} "
                        f"coverage={coverage} protection={protection}"
                    )
                    self.assertFalse(coverage["has_risk"], msg=context)
                    self.assertFalse(protection["has_risk"], msg=context)
                    self.assertEqual(Decimal(str(coverage["coverage_qty"])), Decimal("4.0"))
                    self.assertEqual(coverage["missing_by_level"], [], msg=context)
                    self.assertEqual(coverage["excess_by_level"], [], msg=context)

                    lot_qty = sum(
                        (Decimal(str(lot["qty"])) for lot in engine.reduce_lots_by_level.values()),
                        Decimal("0"),
                    )
                    expected_position = -lot_qty if direction == "short" else lot_qty
                    self.assertEqual(Decimal(str(engine.grid_position_net_qty)), expected_position)
                    self.assertEqual(trade_net_qty, expected_position)

                    mark_price = Decimal(str(engine.current_price))
                    cashflow_gross_equity = cashflow + mark_price * trade_net_qty
                    accounted_gross_equity = Decimal(str(engine.gross_profit)) + Decimal(
                        str(engine.estimate_grid_unrealized_pnl(float(mark_price)))
                    )
                    self.assertAlmostEqual(
                        float(cashflow_gross_equity),
                        float(accounted_gross_equity),
                        places=7,
                        msg=context,
                    )

    async def test_fixed_grid_reduce_normalization_does_not_hide_incomplete_ledger(self):
        client = FakeClient("0.418", tick_size="0.001", qty_step="1", min_qty="1")
        client.positions = [{"side": "Sell", "size": "338", "avgPrice": "0.4"}]
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.42,
                "lower_price": 0.38,
                "grid_count": 20,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.41, 0.412, 0.414, 0.416, 0.418, 0.42]
        engine.grid_position_net_qty = -338
        engine.reduce_lots_complete = False
        engine.reduce_lots_by_level = {}
        engine.active_orders = {
            "g_4_B_a": {
                "link_id": "g_4_B_a",
                "order_id": "1",
                "level_idx": 4,
                "side": "Buy",
                "price": "0.418",
                "qty": "41",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.42,
            },
            "g_3_B_b": {
                "link_id": "g_3_B_b",
                "order_id": "2",
                "level_idx": 3,
                "side": "Buy",
                "price": "0.416",
                "qty": "100",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.42,
            },
            "g_2_B_c": {
                "link_id": "g_2_B_c",
                "order_id": "3",
                "level_idx": 2,
                "side": "Buy",
                "price": "0.414",
                "qty": "56",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.42,
            },
            "g_1_B_d": {
                "link_id": "g_1_B_d",
                "order_id": "4",
                "level_idx": 1,
                "side": "Buy",
                "price": "0.412",
                "qty": "39",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.42,
            },
            "g_0_B_e": {
                "link_id": "g_0_B_e",
                "order_id": "5",
                "level_idx": 0,
                "side": "Buy",
                "price": "0.410",
                "qty": "102",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 0.42,
            },
        }

        engine._reconcile_grid_position_protection()

        reduce_qtys_by_level = sorted(
            (order["level_idx"], float(order["qty"]))
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        )
        self.assertEqual(
            reduce_qtys_by_level,
            [(0, 102.0), (1, 39.0), (2, 56.0), (3, 100.0), (4, 41.0)],
        )
        self.assertAlmostEqual(engine._active_reduce_qty("Buy"), 338.0)
        self.assertEqual(client.cancelled_orders, [])
        self.assertEqual(engine.trigger_message, "")
        snapshot = engine.reduce_protection_snapshot()
        self.assertTrue(snapshot["has_risk"])
        self.assertFalse(snapshot["ledger_ok"])

    async def test_partial_grid_fill_is_processed_incrementally(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        client.trade_details = {}
        client.get_order_trades = lambda symbol, order_id: fake_trade_response(client, order_id)
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        add_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"]
        )
        exchange_order = next(item for item in client.orders if item["orderId"] == add_order["order_id"])
        client.open_limit_order_ids.discard(add_order["order_id"])
        exchange_order["orderStatus"] = "PARTIALLY_FILLED"
        client.trade_details[add_order["order_id"]] = [
            {
                "price": add_order["price"],
                "qty": "42",
                "volume": str(float(add_order["price"]) * 42),
                "feeUsdt": "0",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]

        changed = engine._reconcile_exchange_open_orders([])
        changed_again = engine._reconcile_exchange_open_orders([])

        self.assertTrue(changed)
        self.assertFalse(changed_again)
        self.assertIn(add_order["link_id"], engine.active_orders)
        self.assertEqual(engine.active_orders[add_order["link_id"]]["processed_fill_qty"], 42.0)
        self.assertEqual(
            sum(1 for order in engine.filled_orders if order["side"] == "Sell" and not order["reduce_only"]),
            1,
        )

        exchange_order["orderStatus"] = "FILLED"
        client.trade_details[add_order["order_id"]].append(
            {
                "price": add_order["price"],
                "qty": "58",
                "volume": str(float(add_order["price"]) * 58),
                "feeUsdt": "0",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        )

        engine._reconcile_exchange_open_orders([])

        self.assertNotIn(add_order["link_id"], engine.active_orders)
        open_fill_qty = sum(
            float(order["qty"])
            for order in engine.filled_orders
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == add_order["level_idx"]
        )
        reduce_qty = sum(
            float(order["qty"])
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"] and order["level_idx"] == add_order["level_idx"]
        )
        self.assertEqual(open_fill_qty, 100.0)
        self.assertEqual(reduce_qty, 100.0)

    async def test_filled_order_waits_for_trade_page_to_reach_snapshot_quantity(self):
        class FilledSnapshotClient(FakeClient):
            trade_qty = 0.4

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                response["result"].update(
                    {
                        "orderStatus": "FILLED",
                        "executedQty": "1",
                        "cumQuote": "101",
                        "avgPrice": "101",
                    }
                )
                return response

            def get_order_trades(self, symbol, order_id):
                qty = self.trade_qty
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "price": "101",
                                "qty": str(qty),
                                "volume": str(qty * 101),
                                "feeUsdt": str(qty * 101 * 0.0002),
                                "feeAsset": "USDT",
                                "isMaker": True,
                            }
                        ]
                    },
                }

        client = FilledSnapshotClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.reduce_lots_complete = True
        link_id = engine._place("Sell", 101, 0, reduce_only=False, qty_override=1)
        original_order_id = engine.active_orders[link_id]["order_id"]
        client.open_limit_order_ids.discard(original_order_id)

        await engine._check_fills()

        self.assertIn(link_id, engine.active_orders)
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.4)
        reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Buy" and order.get("reduce_only")
        ]
        self.assertEqual(len(reduce_orders), 1)
        self.assertEqual(float(reduce_orders[0]["qty"]), 0.4)
        self.assertAlmostEqual(engine.total_fee, 0.00808)

        client.trade_qty = 1.0
        await engine._check_fills()

        self.assertNotIn(link_id, engine.active_orders)
        self.assertAlmostEqual(engine.grid_position_net_qty, -1.0)
        self.assertAlmostEqual(engine.total_fee, 0.0202)
        self.assertEqual(
            sum(
                float(order["qty"])
                for order in engine.active_orders.values()
                if order.get("side") == "Buy" and order.get("reduce_only")
            ),
            1.0,
        )

    async def test_unknown_order_with_partial_trade_proof_stays_in_ledger(self):
        class UnknownPartialClient(FakeClient):
            def get_order(self, symbol, order_id):
                return {"retCode": 0, "result": {}}

            def get_order_trades(self, symbol, order_id):
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "price": "101",
                                "qty": "0.4",
                                "volume": "40.4",
                                "feeUsdt": "0.00808",
                                "feeAsset": "USDT",
                                "isMaker": True,
                            }
                        ]
                    },
                }

        client = UnknownPartialClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.reduce_lots_complete = True
        link_id = engine._place("Sell", 101, 0, reduce_only=False, qty_override=1)
        client.open_limit_order_ids.clear()

        changed = engine._reconcile_exchange_open_orders([])

        self.assertTrue(changed)
        self.assertIn(link_id, engine.active_orders)
        self.assertAlmostEqual(engine.active_orders[link_id]["processed_fill_qty"], 0.4)
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.4)
        self.assertEqual(
            sum(
                float(order["qty"])
                for order in engine.active_orders.values()
                if order.get("side") == "Buy" and order.get("reduce_only")
            ),
            0.4,
        )

    async def test_partial_opening_order_waits_for_terminal_status(self):
        client = FakeClient("100", qty_step="1", min_qty="1")
        client.trade_details = {}
        client.get_order_trades = lambda symbol, order_id: {
            "retCode": 0,
            "result": {"list": client.trade_details.get(order_id, [])},
        }
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 100,
                "leverage": 2,
                "initial_order_type": "limit",
                "initial_order_price": 100,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        opening_order = engine.opening_order
        self.assertIsNotNone(opening_order)
        exchange_order = next(item for item in client.orders if item["orderId"] == opening_order["order_id"])
        client.open_limit_order_ids.discard(opening_order["order_id"])
        exchange_order["orderStatus"] = "PARTIALLY_FILLED"
        client.trade_details[opening_order["order_id"]] = [
            {
                "price": opening_order["price"],
                "qty": "42",
                "volume": str(float(opening_order["price"]) * 42),
                "feeUsdt": "0",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]

        await engine._check_initial_order()

        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertIn("partially filled", engine.trigger_message)

    async def test_position_sync_waits_for_order_ledger_before_overcommit_check(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "75", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_position_net_qty = -80.0
        engine.initial_entry_price = 100
        engine.active_orders = {
            "g_1_B_existing": {
                "link_id": "g_1_B_existing",
                "order_id": "1",
                "level_idx": 1,
                "side": "Buy",
                "price": "95.0",
                "qty": "80.0",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": True,
                "entry_price": 100,
            }
        }
        engine.running = True
        engine.grid_ready = True

        engine._reconcile_grid_position_protection()

        self.assertTrue(engine.running)
        self.assertTrue(engine.grid_ready)
        self.assertEqual(engine.grid_position_net_qty, -80.0)
        self.assertEqual(client.cancelled_orders, [])

    async def test_baseline_breach_halts_before_old_position_is_reduced_further(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "1.28", "avgPrice": "100"}]
        placed = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="0.5",
            price="95.0",
            order_type="Limit",
            reduce_only=True,
            order_link_id="g_1_B_existing",
        )
        managed_order_id = str(placed["result"]["orderId"])
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine.restore_state(
            {
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {
                    "g_1_B_existing": {
                        "link_id": "g_1_B_existing",
                        "order_id": managed_order_id,
                        "level_idx": 1,
                        "side": "Buy",
                        "price": "95.0",
                        "qty": "0.5",
                        "status": "open",
                        "order_type": "Limit",
                        "time_in_force": "GTC",
                        "reduce_only": True,
                        "entry_price": 100,
                    }
                },
                "grid_position_net_qty": -0.5,
                "baseline_position_side": "Sell",
                "baseline_position_qty": 3.0,
                "initial_entry_price": 100,
            }
        )

        engine._reconcile_grid_position_protection()

        self.assertFalse(engine.running)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(engine.active_orders, {})
        self.assertIn("Baseline position protection halted grid", engine.trigger_message)

    async def test_restore_adopts_exchange_grid_orders_missing_from_local_state(self):
        client = FakeClient("100")
        client.place_order(
            symbol="TESTUSDT",
            side="Sell",
            qty="2.0",
            price="105.0",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_2_S_exchange",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine.restore_state(
            {
                "running": True,
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {},
                "grid_position_net_qty": 0,
                "grid_ready": True,
            }
        )

        self.assertIn("g_2_S_exchange", engine.active_orders)
        adopted = engine.active_orders["g_2_S_exchange"]
        self.assertEqual(adopted["level_idx"], 2)
        self.assertEqual(adopted["side"], "Sell")
        self.assertFalse(adopted["reduce_only"])

    async def test_strict_order_ownership_pauses_instead_of_adopting_unknown_grid_order(self):
        client = FakeClient("100")
        exchange_order = client.place_order(
            symbol="TESTUSDT",
            side="Sell",
            qty="1.0",
            price="105.0",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_2_S_oldrun",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "qty_per_grid": 1,
                "strict_order_ownership": True,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        open_orders = client.get_open_orders("TESTUSDT")["result"]["list"]

        changed = engine._reconcile_exchange_open_orders(open_orders)
        blocked_link = engine._place("Sell", 110, 3, reduce_only=False, qty_override=1)

        self.assertTrue(changed)
        self.assertEqual(engine.active_orders, {})
        self.assertEqual(len(engine.ownership_conflicts), 1)
        self.assertEqual(
            engine.ownership_conflicts[0]["order_id"],
            str(exchange_order["result"]["orderId"]),
        )
        self.assertIsNone(blocked_link)
        self.assertEqual(len(client.orders), 1)

        client.open_limit_order_ids.clear()
        engine._reconcile_exchange_open_orders([])
        placed_link = engine._place("Sell", 110, 3, reduce_only=False, qty_override=1)

        self.assertEqual(engine.ownership_conflicts, [])
        self.assertIsNotNone(placed_link)
        self.assertEqual(len(client.orders), 2)

    async def test_restore_stopped_grid_does_not_place_protection_orders(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "0.9", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        def place_order(**kwargs):
            raise AssertionError("Stopped grid restore must not place orders")

        client.place_order = place_order
        engine.restore_state(
            {
                "running": False,
                "config": engine.config,
                "grid_levels": [90, 95, 100, 105, 110],
                "active_orders": {},
                "grid_position_net_qty": -0.9,
                "grid_ready": False,
            }
        )

        self.assertFalse(engine.running)
        self.assertEqual(engine.active_orders, {})

    async def test_restore_interrupted_initialization_enters_safe_cleanup_without_new_orders(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        config = {
            "symbol": "TESTUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 4,
            "total_investment": 100,
            "leverage": 2,
        }
        engine = GridEngine(client, config)
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.running = True
        engine.initial_side = "Sell"
        engine.initial_qty = 1.0
        engine.initial_entry_price = 101.0
        engine.grid_position_net_qty = -1.0
        engine.initialization_in_progress = True
        engine._pending_targets = {
            "reference_price": 101.0,
            "profit_targets": [],
            "add_targets": [],
            "allocated_qtys": [],
            "allocated_qty_by_level": {},
            "qty_per_grid": 1.0,
        }
        interrupted_state = engine.to_state()

        restored = GridEngine(client, config)
        restored.restore_state(interrupted_state)

        self.assertFalse(restored.initialization_in_progress)
        self.assertTrue(restored.initialization_failed)
        self.assertTrue(restored.manual_stop_pending)
        self.assertFalse(restored.grid_ready)
        self.assertAlmostEqual(restored.grid_position_net_qty, -1.0)
        self.assertEqual(client.orders, [])
        self.assertIn("interrupted", restored.trigger_message)

    async def test_unknown_missing_order_is_not_removed_without_trade_proof(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        await engine.initialize()
        order = next(iter(engine.active_orders.values()))
        client.open_limit_order_ids.discard(order["order_id"])
        matching = next(item for item in client.orders if item["orderId"] == order["order_id"])
        matching["orderStatus"] = "UNKNOWN"

        await engine._check_fills()

        self.assertIn(order["link_id"], engine.active_orders)
        self.assertEqual(engine.filled_orders, [])

    async def test_cancelled_order_history_fallback_removes_stale_active_order(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        await engine.initialize()
        order = next(item for item in engine.active_orders.values() if item["side"] == "Buy" and item["reduce_only"])
        client.open_limit_order_ids.discard(order["order_id"])
        matching = next(item for item in client.orders if item["orderId"] == order["order_id"])
        matching["orderStatus"] = "CANCELED"

        def get_order(symbol, order_id):
            raise RuntimeError("Order does not exist.")

        def get_order_history(symbol, limit=1000):
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "orderId": str(item["orderId"]),
                            "orderLinkId": item.get("order_link_id", ""),
                            "orderStatus": item.get("orderStatus", "FILLED"),
                        }
                        for item in client.orders
                        if item.get("symbol") == symbol
                    ]
                },
            }

        client.get_order = get_order
        client.get_order_history = get_order_history

        await engine._check_fills()

        self.assertNotIn(order["link_id"], engine.active_orders)
        self.assertEqual(engine.filled_orders, [])
        self.assertLessEqual(engine._active_reduce_qty("Buy"), engine._grid_position_qty())

    async def test_long_grid_replenishes_buy_after_take_profit_fill(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )

        await engine.initialize()

        take_profit_order = next(
            order for order in engine.active_orders.values() if order["side"] == "Sell" and order["reduce_only"]
        )
        before_ids = set(engine.active_orders)
        client.open_limit_order_ids.discard(take_profit_order["order_id"])
        await engine._check_fills()
        after_ids = set(engine.active_orders)
        new_ids = after_ids - before_ids
        new_orders = [engine.active_orders[link_id] for link_id in new_ids]

        self.assertEqual(len(engine.filled_orders), 1)
        self.assertTrue(engine.filled_orders[0]["reduce_only"])
        self.assertEqual(engine.get_status()["completed_pairs"], 1)
        self.assertTrue(any(order["side"] == "Buy" and not order["reduce_only"] for order in new_orders))

    async def test_cancelled_grid_order_is_replaced_without_recording_a_fill(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )

        await engine.initialize()
        order = next(iter(engine.active_orders.values()))
        before_fills = len(engine.filled_orders)
        before_pairs = engine.completed_pairs
        client.open_limit_order_ids.discard(order["order_id"])
        original = next(item for item in client.orders if item["orderId"] == order["order_id"])
        original["orderStatus"] = "CANCELED"

        await engine._check_fills()

        self.assertEqual(len(engine.filled_orders), before_fills)
        self.assertEqual(engine.completed_pairs, before_pairs)
        replacement_orders = [
            item
            for item in engine.active_orders.values()
            if item["side"] == order["side"]
            and item["level_idx"] == order["level_idx"]
            and item["reduce_only"] == order["reduce_only"]
        ]
        self.assertEqual(len(replacement_orders), 1)
        self.assertNotEqual(replacement_orders[0]["order_id"], order["order_id"])

    async def test_cancelled_order_replacement_is_not_blocked_by_another_same_level_order(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "qty_per_grid": 10,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        first_link = engine._place(
            "Buy",
            90,
            0,
            reduce_only=False,
            qty_override=10,
            allow_duplicate=True,
        )
        second_link = engine._place(
            "Buy",
            90,
            0,
            reduce_only=False,
            qty_override=10,
            allow_duplicate=True,
        )
        cancelled = engine.active_orders[first_link]
        client.open_limit_order_ids.discard(cancelled["order_id"])
        client.orders[0]["orderStatus"] = "CANCELED"

        engine._handle_cancelled_order(
            first_link,
            cancelled,
            snapshot={
                "orderId": cancelled["order_id"],
                "orderStatus": "CANCELED",
                "executedQty": "0",
            },
        )

        same_level = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy"
            and order["level_idx"] == 0
            and not order["reduce_only"]
        ]
        self.assertIn(second_link, engine.active_orders)
        self.assertEqual(len(same_level), 2)
        self.assertEqual(sum(float(order["qty"]) for order in same_level), 20.0)
        self.assertEqual(engine.paused_replacements, [])

    async def test_cancelled_remainder_intent_is_durable_before_replacement_submission(self):
        class RejectNextLimitClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reject_next_limit = False

            def place_order(self, **kwargs):
                if self.reject_next_limit and kwargs.get("order_type") == "Limit":
                    self.reject_next_limit = False
                    return {"retCode": 400, "retMsg": "deterministic rejection"}
                return super().place_order(**kwargs)

        client = RejectNextLimitClient("100", tick_size="1", qty_step="1", min_qty="1")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 100,
            "lower_price": 90,
            "grid_count": 1,
            "total_investment": 100,
            "leverage": 2,
            "qty_per_grid": 10,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 100]
        original_link = engine._place(
            "Buy",
            90,
            0,
            reduce_only=False,
            qty_override=10,
        )
        original_order = engine.active_orders[original_link]
        client.open_limit_order_ids.discard(original_order["order_id"])
        client.orders[0]["orderStatus"] = "CANCELED"
        client.reject_next_limit = True
        durable_states = []

        def crash_after_old_order_is_removed(current_engine):
            snapshot = copy.deepcopy(current_engine.to_state())
            durable_states.append(snapshot)
            if original_link not in snapshot["active_orders"]:
                raise OSError("simulated crash after cancellation transaction")

        engine.state_callback = crash_after_old_order_is_removed
        with self.assertRaisesRegex(OSError, "cancellation transaction"):
            engine._handle_cancelled_order(
                original_link,
                original_order,
                snapshot={
                    "orderId": original_order["order_id"],
                    "orderStatus": "CANCELED",
                    "executedQty": "0",
                },
            )

        self.assertEqual(len(client.orders), 1)
        durable = durable_states[-1]
        self.assertNotIn(original_link, durable["active_orders"])
        self.assertEqual(len(durable["paused_replacements"]), 1)
        replacement_link_id = durable["paused_replacements"][0]["replacement_link_id"]

        client.reject_next_limit = False
        restored = GridEngine(client, config)
        restored.restore_state(durable)
        restored.paused_replacements[0]["replacement_retry_after"] = 0
        restored._resume_paused_replacements()

        self.assertEqual(restored.paused_replacements, [])
        self.assertIn(replacement_link_id, restored.active_orders)
        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

    async def test_failed_cancel_replacement_survives_restart_and_retries_exact_order(self):
        class RejectNextLimitClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reject_next_limit = False

            def place_order(self, **kwargs):
                if self.reject_next_limit and kwargs.get("order_type") == "Limit":
                    self.reject_next_limit = False
                    return {"retCode": 400, "retMsg": "temporary deterministic rejection"}
                return super().place_order(**kwargs)

        client = RejectNextLimitClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 4,
            "total_investment": 100,
            "leverage": 2,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        await engine.initialize()
        order = next(iter(engine.active_orders.values()))
        original_shape = (
            order["side"],
            order["price"],
            order["qty"],
            order["level_idx"],
            order["reduce_only"],
        )
        client.open_limit_order_ids.discard(order["order_id"])
        original = next(item for item in client.orders if item["orderId"] == order["order_id"])
        original["orderStatus"] = "CANCELED"
        client.reject_next_limit = True

        await engine._check_fills()

        self.assertNotIn(order["link_id"], engine.active_orders)
        self.assertEqual(len(engine.paused_replacements), 1)
        self.assertEqual(engine.paused_replacements[0]["replacement_mode"], "same_order")
        self.assertIn("queued", engine.trigger_message)

        saved = engine.to_state()
        restored = GridEngine(client, config)
        restored.restore_state(saved)
        self.assertEqual(len(restored.paused_replacements), 1)
        restored.paused_replacements[0]["replacement_retry_after"] = 0

        restored._resume_paused_replacements()

        self.assertEqual(restored.paused_replacements, [])
        replacement = next(
            item
            for item in restored.active_orders.values()
            if (
                item["side"],
                item["price"],
                item["qty"],
                item["level_idx"],
                item["reduce_only"],
            )
            == original_shape
        )
        self.assertNotEqual(replacement["order_id"], order["order_id"])

    async def test_queued_exact_replacement_retries_with_existing_same_level_order(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "qty_per_grid": 10,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        existing_link = engine._place(
            "Buy",
            90,
            0,
            reduce_only=False,
            qty_override=10,
        )
        engine.paused_replacements = [
            {
                "link_id": "g_0_B_cancelled",
                "order_id": "cancelled-order",
                "level_idx": 0,
                "side": "Buy",
                "price": "90",
                "qty": "10",
                "reduce_only": False,
                "entry_price": None,
                "replacement_mode": "same_order",
                "replacement_source_link_id": "g_0_B_cancelled",
                "replacement_link_id": "g_0_B_retry01",
                "replacement_retry_after": 0,
                "completed_pair_counted": True,
            }
        ]

        engine._resume_paused_replacements()

        self.assertIn(existing_link, engine.active_orders)
        self.assertIn("g_0_B_retry01", engine.active_orders)
        self.assertTrue(engine.active_orders["g_0_B_retry01"]["completed_pair_counted"])
        self.assertEqual(engine.paused_replacements, [])
        self.assertEqual(len(client.open_limit_order_ids), 2)

    async def test_generic_coverage_repair_waits_for_exact_replacement_queue(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
            },
        )
        engine.paused_replacements = [{"replacement_mode": "same_order"}]

        with patch.object(engine, "_sync_grid_position_with_exchange") as sync_position:
            engine._reconcile_grid_position_protection()

        sync_position.assert_not_called()

    async def test_cancel_replacement_queue_survives_crash_inside_failed_retry_persist(self):
        class RejectLimitClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reject_limit = False

            def place_order(self, **kwargs):
                if self.reject_limit and kwargs.get("order_type") == "Limit":
                    return {"retCode": 400, "retMsg": "temporary deterministic rejection"}
                return super().place_order(**kwargs)

        client = RejectLimitClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 4,
            "total_investment": 100,
            "leverage": 2,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        await engine.initialize()
        order = next(iter(engine.active_orders.values()))
        client.open_limit_order_ids.discard(order["order_id"])
        original = next(item for item in client.orders if item["orderId"] == order["order_id"])
        original["orderStatus"] = "CANCELED"
        client.reject_limit = True

        await engine._check_fills()

        queued = engine.paused_replacements[0]
        queued["replacement_retry_after"] = 0
        replacement_link_id = queued["replacement_link_id"]
        durable_states = []
        write_ahead_seen = False

        def crash_after_rejected_order_is_removed(current_engine):
            nonlocal write_ahead_seen
            snapshot = copy.deepcopy(current_engine.to_state())
            durable_states.append(snapshot)
            if replacement_link_id in snapshot["active_orders"]:
                write_ahead_seen = True
            elif write_ahead_seen:
                raise OSError("simulated crash after deterministic retry rejection")

        engine.state_callback = crash_after_rejected_order_is_removed
        with self.assertRaisesRegex(OSError, "simulated crash"):
            engine._resume_paused_replacements()

        self.assertTrue(write_ahead_seen)
        self.assertEqual(
            durable_states[-1]["paused_replacements"][0]["replacement_link_id"],
            replacement_link_id,
        )

        client.reject_limit = False
        restored = GridEngine(client, config)
        restored.restore_state(durable_states[-1])
        restored.paused_replacements[0]["replacement_retry_after"] = 0
        restored._resume_paused_replacements()

        self.assertEqual(restored.paused_replacements, [])
        self.assertIn(replacement_link_id, restored.active_orders)

    async def test_accepted_cancel_replacement_is_not_duplicated_after_pre_dequeue_crash(self):
        class RejectNextLimitClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reject_next_limit = False

            def place_order(self, **kwargs):
                if self.reject_next_limit and kwargs.get("order_type") == "Limit":
                    self.reject_next_limit = False
                    return {"retCode": 400, "retMsg": "temporary deterministic rejection"}
                return super().place_order(**kwargs)

        client = RejectNextLimitClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 4,
            "total_investment": 100,
            "leverage": 2,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        await engine.initialize()
        order = next(iter(engine.active_orders.values()))
        client.open_limit_order_ids.discard(order["order_id"])
        original = next(item for item in client.orders if item["orderId"] == order["order_id"])
        original["orderStatus"] = "CANCELED"
        client.reject_next_limit = True

        await engine._check_fills()

        queued = engine.paused_replacements[0]
        queued["replacement_retry_after"] = 0
        replacement_link_id = queued["replacement_link_id"]
        durable_states = []

        def crash_after_exchange_acceptance_before_dequeue(current_engine):
            snapshot = copy.deepcopy(current_engine.to_state())
            durable_states.append(snapshot)
            replacement = snapshot["active_orders"].get(replacement_link_id)
            if replacement and replacement.get("order_id"):
                raise OSError("simulated crash before retry queue dequeue")

        engine.state_callback = crash_after_exchange_acceptance_before_dequeue
        with self.assertRaisesRegex(OSError, "simulated crash"):
            engine._resume_paused_replacements()

        accepted_count = sum(
            1 for item in client.orders if item.get("order_link_id") == replacement_link_id
        )
        self.assertEqual(accepted_count, 1)
        self.assertEqual(
            durable_states[-1]["paused_replacements"][0]["replacement_link_id"],
            replacement_link_id,
        )
        self.assertIn(replacement_link_id, durable_states[-1]["active_orders"])

        restored = GridEngine(client, config)
        restored.restore_state(durable_states[-1])
        restored.paused_replacements[0]["replacement_retry_after"] = 0
        restored._resume_paused_replacements()

        self.assertEqual(restored.paused_replacements, [])
        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

    async def test_accepted_counter_replacement_is_not_duplicated_after_pre_dequeue_crash(self):
        client = FakeClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 2,
            "total_investment": 100,
            "leverage": 2,
            "qty_per_grid": 1,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]
        engine.running = True
        engine.grid_ready = True
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
                "order_type": "Limit",
                "time_in_force": "GTC",
            }
        ]
        durable_states = []

        def crash_after_exchange_acceptance_before_dequeue(current_engine):
            snapshot = copy.deepcopy(current_engine.to_state())
            durable_states.append(snapshot)
            replacement_link_id = str(
                snapshot["paused_replacements"][0].get("replacement_link_id") or ""
            )
            replacement = snapshot["active_orders"].get(replacement_link_id)
            if replacement and replacement.get("order_id"):
                raise OSError("simulated counter crash before retry queue dequeue")

        engine.state_callback = crash_after_exchange_acceptance_before_dequeue
        with self.assertRaisesRegex(OSError, "simulated counter crash"):
            engine._resume_paused_replacements()

        replacement_link_id = durable_states[-1]["paused_replacements"][0][
            "replacement_link_id"
        ]
        self.assertIn(replacement_link_id, durable_states[-1]["active_orders"])
        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

        restored = GridEngine(client, config)
        restored.restore_state(durable_states[-1])
        restored._resume_paused_replacements()

        self.assertEqual(restored.paused_replacements, [])
        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

    async def test_fill_and_counter_intent_are_persisted_atomically_before_submission(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 100,
            "lower_price": 90,
            "grid_count": 1,
            "total_investment": 100,
            "leverage": 2,
            "qty_per_grid": 1,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 100]
        durable_states = []

        def crash_on_first_durable_fill(current_engine):
            snapshot = copy.deepcopy(current_engine.to_state())
            durable_states.append(snapshot)
            if snapshot["filled_orders"]:
                raise OSError("simulated crash after durable fill transaction")

        engine.state_callback = crash_on_first_durable_fill
        order = {
            "link_id": "g_0_B_filled",
            "order_id": "filled-order",
            "level_idx": 0,
            "side": "Buy",
            "price": "90",
            "qty": "1",
            "reduce_only": False,
            "order_type": "Limit",
            "time_in_force": "GTC",
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
        }

        with self.assertRaisesRegex(OSError, "durable fill transaction"):
            engine._record_execution_delta(
                order,
                {
                    "price": 90.0,
                    "qty": 1.0,
                    "volume": 90.0,
                    "fee": 0.0,
                    "fee_asset": "USDT",
                    "fee_source": "exchange",
                    "maker_count": 1,
                    "taker_count": 0,
                },
            )

        self.assertEqual(client.orders, [])
        durable = durable_states[-1]
        self.assertEqual(len(durable["filled_orders"]), 1)
        self.assertEqual(len(durable["paused_replacements"]), 1)
        replacement_link_id = durable["paused_replacements"][0]["replacement_link_id"]

        restored = GridEngine(client, config)
        restored.restore_state(durable)
        restored._resume_paused_replacements()

        self.assertEqual(restored.paused_replacements, [])
        self.assertIn(replacement_link_id, restored.active_orders)
        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

    async def test_live_replacement_queue_is_never_truncated_in_persisted_state(self):
        client = FakeClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 2,
            "total_investment": 100,
            "leverage": 2,
        }
        engine = GridEngine(client, config)
        engine.paused_replacements = [
            {
                "link_id": f"g_0_B_filled_{index}",
                "order_id": str(index),
                "level_idx": 0,
                "side": "Buy",
                "price": "90",
                "qty": "1",
                "fill_price": 90.0,
                "reduce_only": False,
            }
            for index in range(225)
        ]

        saved = engine.to_state()
        restored = GridEngine(client, config)
        restored.restore_state(copy.deepcopy(saved))

        self.assertEqual(len(saved["paused_replacements"]), 225)
        self.assertEqual(len(restored.paused_replacements), 225)
        self.assertEqual(restored.paused_replacements[0]["order_id"], "0")
        self.assertEqual(restored.paused_replacements[-1]["order_id"], "224")

    async def test_expired_in_match_order_is_replaced_without_recording_a_fill(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        order = next(item for item in engine.active_orders.values() if not item["reduce_only"])
        before_fills = len(engine.filled_orders)
        client.open_limit_order_ids.discard(order["order_id"])
        original = next(item for item in client.orders if item["orderId"] == order["order_id"])
        original["orderStatus"] = "EXPIRED_IN_MATCH"

        await engine._check_fills()

        self.assertEqual(len(engine.filled_orders), before_fills)
        replacement_orders = [
            item
            for item in engine.active_orders.values()
            if item["side"] == order["side"]
            and item["level_idx"] == order["level_idx"]
            and item["reduce_only"] == order["reduce_only"]
        ]
        self.assertEqual(len(replacement_orders), 1)
        self.assertNotEqual(replacement_orders[0]["order_id"], order["order_id"])

    async def test_short_add_fill_places_reduce_order_even_when_level_already_has_reduce_order(self):
        client = FakeClient("102")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        add_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 2
        )
        existing_reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"] and order["level_idx"] == 2
        ]
        self.assertEqual(len(existing_reduce_orders), 1)

        client.open_limit_order_ids.discard(add_order["order_id"])
        await engine._check_fills()

        reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"] and order["level_idx"] == 2
        ]
        self.assertEqual(len(reduce_orders), 2)
        self.assertAlmostEqual(
            sum(float(order["qty"]) for order in reduce_orders),
            float(existing_reduce_orders[0]["qty"]) + float(add_order["qty"]),
        )

    async def test_instantly_filled_counter_order_is_reconciled_before_protection(self):
        client = FakeClient("102")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        add_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 2
        )
        client.instant_fill_reduce_limits = True
        client.open_limit_order_ids.discard(add_order["order_id"])

        await engine._check_fills()

        instant_reduce_order = next(
            order
            for order in reversed(client.orders)
            if order["side"] == "Buy"
            and order["reduce_only"]
            and order.get("orderStatus") == "FILLED"
            and order["price"] == "100.0"
        )
        self.assertEqual(instant_reduce_order["side"], "Buy")
        self.assertTrue(instant_reduce_order["reduce_only"])
        self.assertTrue(
            any(
                order["side"] == "Buy"
                and order["reduce_only"]
                and order["level_idx"] == add_order["level_idx"]
                and abs(float(order["qty"]) - float(add_order["qty"])) < 1e-9
                for order in engine.filled_orders
            )
        )
        self.assertFalse(
            any(
                order.get("order_id") == instant_reduce_order["orderId"]
                for order in engine.active_orders.values()
            )
        )

    async def test_reduce_fill_does_not_duplicate_existing_short_add_order(self):
        client = FakeClient("102")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        reduce_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"] and order["level_idx"] == 2
        )
        before_add_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 2
        ]
        self.assertEqual(len(before_add_orders), 1)

        placed = engine._place_counter_order(reduce_order)

        after_add_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 2
        ]
        self.assertTrue(placed)
        self.assertEqual(len(after_add_orders), 1)

    async def test_existing_non_reduce_counter_order_counts_as_resumed(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        existing_add_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 2
        ]
        self.assertEqual(len(existing_add_orders), 1)
        engine.paused_replacements = [
            {
                "side": "Buy",
                "price": "100",
                "qty": "1",
                "level_idx": 2,
                "reduce_only": True,
                "fill_price": 100,
            }
        ]

        engine._resume_paused_replacements()

        add_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"] and order["level_idx"] == 2
        ]
        self.assertEqual(len(add_orders), 1)
        self.assertEqual(engine.paused_replacements, [])

    async def test_initial_long_overlap_uses_same_qty_for_add_and_reduce_orders(self):
        client = FakeClient("15.93", tick_size="0.01", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "NOKUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 16.3,
                "lower_price": 15.6,
                "grid_count": 30,
                "total_investment": 40,
                "leverage": 20,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine._fetch_precision()
        engine.grid_levels = engine._calculate_levels()
        engine.initial_entry_price = 15.93
        engine.grid_position_net_qty = 50.18
        engine._prepare_pending_targets(15.93, total_qty_override=50.18)
        engine._deploy_pending_targets()

        reduce_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and order["reduce_only"] and order["level_idx"] == 14
        )
        add_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and not order["reduce_only"] and order["level_idx"] == 14
        )
        self.assertEqual(reduce_order["price"], "15.95")
        self.assertEqual(add_order["price"], "15.92")
        self.assertEqual(reduce_order["qty"], "3.14")
        self.assertEqual(add_order["qty"], "3.14")
        self.assertEqual(engine.target_qty_by_level["14"], 3.14)

    async def test_target_qty_by_level_is_persisted_and_restored(self):
        client = FakeClient("15.93", tick_size="0.01", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "NOKUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 16.3,
                "lower_price": 15.6,
                "grid_count": 30,
                "total_investment": 40,
                "leverage": 20,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine._fetch_precision()
        engine.grid_levels = engine._calculate_levels()
        engine.initial_entry_price = 15.93
        engine.initial_qty = 50.18
        engine._prepare_pending_targets(15.93, total_qty_override=50.18)
        saved = engine.to_state()

        restored = GridEngine(client, saved["config"])
        restored.restore_state(saved)

        self.assertEqual(restored.target_qty_by_level["14"], 3.14)
        self.assertEqual(restored.get_status()["target_qty_by_level"]["14"], 3.14)

    async def test_truncated_pending_allocation_is_rejected_before_any_grid_order(self):
        client = BatchFakeClient("101", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = engine._calculate_levels()
        engine.initial_entry_price = 101
        engine._prepare_pending_targets(101, total_qty_override=3)
        engine._pending_targets["allocated_qtys"].pop()

        with self.assertRaisesRegex(RuntimeError, "target and quantity counts differ"):
            engine._deploy_pending_targets()

        self.assertFalse(engine.grid_ready)
        self.assertEqual(engine.active_orders, {})
        self.assertEqual(client.batch_calls, [])
        self.assertEqual(client.orders, [])

    async def test_fixed_grid_qty_below_limit_minimum_is_rejected_before_market_open(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.05,
                "leverage": 2,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "below exchange quantity precision or minimum"):
            await engine.initialize()

        self.assertFalse(engine.grid_ready)
        self.assertIsNone(engine.opening_order)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])

    async def test_duplicate_exchange_prices_are_rejected_before_market_open(self):
        client = FakeClient("1010", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 1020,
                "lower_price": 1000,
                "grid_count": 40,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "leverage": 2,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "duplicate exchange prices"):
            await engine.initialize()

        self.assertFalse(engine.grid_ready)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.positions, [])

    async def test_duplicate_batch_targets_are_rejected_before_submission(self):
        client = BatchFakeClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "qty_per_grid": 1,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]
        duplicate = {
            "side": "Sell",
            "price": 105,
            "level_idx": 1,
            "reduce_only": False,
            "qty_override": 1,
        }

        with self.assertRaisesRegex(RuntimeError, "Duplicate grid target"):
            engine._place_batch_limit_orders([duplicate, dict(duplicate)])

        self.assertEqual(engine.active_orders, {})
        self.assertEqual(client.batch_calls, [])
        self.assertEqual(client.orders, [])

    async def test_partial_batch_rejection_never_marks_initial_grid_ready(self):
        class RejectOneTargetClient(BatchFakeClient):
            def place_order(self, **kwargs):
                if kwargs.get("side") == "Sell" and str(kwargs.get("price")) == "110":
                    return {"retCode": 400, "retMsg": "simulated target rejection"}
                return super().place_order(**kwargs)

        client = RejectOneTargetClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "Initial grid deployment is incomplete"):
            await engine.initialize()

        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(client.batch_calls), 1)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(len(engine.active_orders), 1)

    async def test_batch_quantity_shrink_stops_grid_and_keeps_order_cancellable(self):
        class ShrinkingBatchClient(BatchFakeClient):
            def place_orders(self, orders):
                response = super().place_orders(orders)
                for index, (request, item) in enumerate(
                    zip(orders, response["result"]["list"], strict=True)
                ):
                    accepted_qty = str(request["qty"])
                    if index == 0:
                        accepted_qty = self.round_to_step(
                            float(request["qty"]) - float(self.qty_step),
                            self.qty_step,
                        )
                    order_id = str(item["result"]["orderId"])
                    accepted = next(order for order in self.orders if order["orderId"] == order_id)
                    accepted["qty"] = accepted_qty
                    item["result"].update(
                        {
                            "orderLinkId": request["order_link_id"],
                            "side": request["side"],
                            "price": request["price"],
                            "qty": accepted_qty,
                            "reduceOnly": request["reduce_only"],
                            "orderStatus": "NEW",
                        }
                    )
                return response

        client = ShrinkingBatchClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 2,
            "total_investment": 100,
            "leverage": 2,
            "qty_per_grid": 1,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]
        engine.initial_entry_price = 100
        engine._prepare_pending_targets(100, total_qty_override=1)

        with self.assertRaisesRegex(RuntimeError, "different exchange shape"):
            engine._deploy_pending_targets()

        mismatches = [
            order
            for order in engine.active_orders.values()
            if order.get("accepted_shape_mismatch")
        ]
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0]["expected_qty"], "1.0")
        self.assertEqual(mismatches[0]["qty"], "0.9")
        self.assertTrue(mismatches[0]["order_id"])
        self.assertTrue(engine.manual_stop_pending)
        self.assertFalse(engine.grid_ready)

        await engine.stop()

        self.assertEqual(engine.active_orders, {})
        self.assertEqual(client.open_limit_order_ids, set())

    async def test_pending_plan_submits_every_target_across_directions_and_prices(self):
        references = (91.0, 95.5, 100.0, 104.5, 109.0)
        for direction in ("long", "short", "neutral"):
            for reference in references:
                with self.subTest(direction=direction, reference=reference):
                    client = BatchFakeClient(
                        str(reference), tick_size="1", qty_step="0.1", min_qty="0.1"
                    )
                    engine = GridEngine(
                        client,
                        {
                            "symbol": "TESTUSDT",
                            "direction": direction,
                            "grid_mode": "arithmetic",
                            "upper_price": 110,
                            "lower_price": 90,
                            "grid_count": 20,
                            "total_investment": 0,
                            "position_sizing_mode": "fixed_grid_qty",
                            "grid_order_qty": 0.2,
                            "leverage": 2,
                            "grid_order_post_only": False,
                        },
                    )
                    engine._fetch_precision()
                    engine.grid_levels = engine._calculate_levels()
                    engine.initial_entry_price = reference
                    engine._prepare_pending_targets(reference)
                    expected_plan = engine._validated_pending_target_plan()

                    engine._deploy_pending_targets()

                    actual_keys = {
                        (
                            order["side"],
                            int(order["level_idx"]),
                            bool(order["reduce_only"]),
                        )
                        for order in engine.active_orders.values()
                    }
                    expected_keys = {
                        (
                            spec["side"],
                            int(spec["level_idx"]),
                            bool(spec["reduce_only"]),
                        )
                        for spec in expected_plan
                    }
                    self.assertTrue(engine.grid_ready)
                    self.assertEqual(len(engine.active_orders), len(expected_plan))
                    self.assertEqual(len(client.orders), len(expected_plan))
                    self.assertEqual(actual_keys, expected_keys)

    async def test_counter_order_tops_up_existing_non_reduce_qty_deficit(self):
        client = FakeClient("15.93", tick_size="0.01", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "NOKUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 16.3,
                "lower_price": 15.6,
                "grid_count": 30,
                "total_investment": 40,
                "leverage": 20,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        engine._fetch_precision()
        engine.grid_levels = engine._calculate_levels()
        engine.active_orders["existing"] = {
            "link_id": "existing",
            "order_id": "existing-order",
            "level_idx": 14,
            "side": "Buy",
            "price": "15.92",
            "qty": "3.13",
            "status": "open",
            "order_type": "Limit",
            "time_in_force": "GTC",
            "reduce_only": False,
            "entry_price": None,
        }

        placed = engine._place_counter_order(
            {
                "side": "Sell",
                "price": "15.95",
                "qty": "3.14",
                "level_idx": 14,
                "reduce_only": True,
                "fill_price": 15.95,
            }
        )

        top_up_orders = [
            order
            for order in client.orders
            if order["side"] == "Buy" and order["price"] == "15.92" and not order["reduce_only"]
        ]
        self.assertTrue(placed)
        self.assertEqual(len(top_up_orders), 1)
        self.assertEqual(top_up_orders[0]["qty"], "0.01")

    async def test_long_add_fill_places_reduce_order_even_when_level_already_has_reduce_order(self):
        client = FakeClient("98")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )

        await engine.initialize()

        add_order = next(
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and not order["reduce_only"] and order["level_idx"] == 1
        )
        existing_reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and order["reduce_only"] and order["level_idx"] == 1
        ]
        self.assertEqual(len(existing_reduce_orders), 1)

        client.open_limit_order_ids.discard(add_order["order_id"])
        await engine._check_fills()

        reduce_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and order["reduce_only"] and order["level_idx"] == 1
        ]
        self.assertEqual(len(reduce_orders), 2)
        self.assertAlmostEqual(
            sum(float(order["qty"]) for order in reduce_orders),
            float(existing_reduce_orders[0]["qty"]) + float(add_order["qty"]),
        )

    async def test_gtc_limit_fallback_fee_uses_taker_rate(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "maker_fee_rate": 0.0002,
                "taker_fee_rate": 0.0005,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        order = next(order for order in engine.active_orders.values() if order["time_in_force"] == "GTC")
        stats = engine._get_trade_stats(
            "",
            fallback_price=100,
            fallback_qty=10,
            liquidity_hint=engine._order_liquidity_hint(order),
        )

        self.assertEqual(engine._order_liquidity_hint(order), "taker")
        self.assertAlmostEqual(stats["fee"], 0.5)

    async def test_pending_targets_are_cleared_after_grid_deployment(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        self.assertTrue(engine.grid_ready)
        self.assertIsNone(engine._pending_targets)
        self.assertIsNone(engine.to_state()["pending_targets"])

    async def test_fee_and_volume_are_counted_in_usdt_equivalent(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "fee_rate": 0.001,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        self.assertAlmostEqual(engine.total_volume, 200)
        self.assertAlmostEqual(engine.total_fee, 0.2)
        self.assertAlmostEqual(engine.total_profit, -0.2)

        take_profit_order = next(
            order for order in engine.active_orders.values() if order["side"] == "Sell" and order["reduce_only"]
        )
        client.open_limit_order_ids.discard(take_profit_order["order_id"])
        await engine._check_fills()
        status = engine.get_status()

        self.assertEqual(status["completed_pairs"], 1)
        self.assertAlmostEqual(status["gross_profit"], 5.0)
        self.assertAlmostEqual(status["total_volume"], 305.0)
        self.assertAlmostEqual(status["total_fee"], 0.305)
        self.assertAlmostEqual(status["total_profit"], 4.695)
        self.assertEqual(engine.filled_orders[0]["fee_asset"], "USDT estimated")

    async def test_post_only_initial_order_waits_before_deploying_grid(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "fee_rate": 0.001,
                "initial_order_type": "post_only",
                "initial_order_price": 99,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        client.trade_details = {}

        def get_order_trades(symbol, order_id):
            return fake_trade_response(client, order_id)

        client.get_order_trades = get_order_trades

        await engine.initialize()

        opening_order = engine.opening_order
        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(engine.active_orders, {})
        self.assertEqual(client.orders[-1]["time_in_force"], "PostOnly")
        self.assertEqual(client.orders[-1]["price"], "99.0")

        client.trade_details[opening_order["order_id"]] = [
            {
                "price": "99",
                "qty": "2.0",
                "volume": "198",
                "feeUsdt": "0.05",
                "feeAsset": "USDT",
            }
        ]
        client.open_limit_order_ids.discard(opening_order["order_id"])
        await engine._check_initial_order()

        self.assertFalse(engine.waiting_initial_order)
        self.assertTrue(engine.grid_ready)
        self.assertGreater(len(engine.active_orders), 0)
        self.assertAlmostEqual(engine.total_volume, 198)
        self.assertAlmostEqual(engine.total_fee, 0.05)

    async def test_short_post_only_recomputes_grid_from_actual_open_fill_price(self):
        client = FakeClient("322.39")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 330,
                "lower_price": 318,
                "grid_count": 30,
                "total_investment": 80,
                "leverage": 10,
                "fee_rate": 0.001,
                "initial_order_type": "post_only",
                "initial_order_price": 322.5,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        client.trade_details = {}

        def get_order_trades(symbol, order_id):
            return fake_trade_response(client, order_id)

        client.get_order_trades = get_order_trades

        await engine.initialize()
        opening_order = engine.opening_order
        fill_qty = opening_order["qty"]
        client.trade_details[opening_order["order_id"]] = [
            {
                "price": "322.5",
                "qty": fill_qty,
                "volume": str(float(fill_qty) * 322.5),
                "feeUsdt": "0.0",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]
        client.open_limit_order_ids.discard(opening_order["order_id"])

        await engine._check_initial_order()

        add_sells = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"]
        ]
        reduce_buys = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertTrue(add_sells)
        self.assertTrue(reduce_buys)
        self.assertTrue(all(float(order["price"]) > 322.5 for order in add_sells))
        self.assertTrue(any(float(order["price"]) == 322.4 for order in reduce_buys))
        self.assertFalse(
            any(float(order["price"]) <= 322.5 for order in add_sells),
            add_sells,
        )

    async def test_post_only_initial_order_without_fill_replaces_safely(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "initial_order_type": "post_only",
                "initial_order_price": 101,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        client.get_order_trades = lambda symbol, order_id: {"retCode": 0, "result": {"list": []}}

        await engine.initialize()
        engine.running = True
        opening_order = engine.opening_order
        original = next(order for order in client.orders if order["orderId"] == opening_order["order_id"])
        original["orderStatus"] = "CANCELED"
        client.open_limit_order_ids.clear()
        await engine._check_initial_order()

        self.assertTrue(engine.running)
        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(client.orders), 2)
        self.assertNotEqual(engine.opening_order["order_id"], opening_order["order_id"])
        self.assertIn("replaced", engine.trigger_message)

    async def test_rejected_opening_replacement_stops_without_ghost_running_state(self):
        class RejectSecondOpeningClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.opening_attempts = 0

            def place_order(self, **kwargs):
                if str(kwargs.get("order_link_id", "")).startswith("open_"):
                    self.opening_attempts += 1
                    if self.opening_attempts == 2:
                        return {"retCode": 400, "retMsg": "replacement rejected"}
                return super().place_order(**kwargs)

        client = RejectSecondOpeningClient("100")
        client.get_order_trades = lambda symbol, order_id: {
            "retCode": 0,
            "result": {"list": []},
        }
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "initial_order_type": "post_only",
                "initial_order_price": 101,
            },
        )

        await engine.initialize()
        engine.running = True
        opening_order = engine.opening_order
        original = next(
            order for order in client.orders if order["orderId"] == opening_order["order_id"]
        )
        original["orderStatus"] = "CANCELED"
        client.open_limit_order_ids.clear()

        await engine._check_initial_order()

        self.assertFalse(engine.running)
        self.assertFalse(engine.grid_ready)
        self.assertFalse(engine.waiting_initial_order)
        self.assertIsNone(engine.opening_order)
        self.assertEqual(len(client.orders), 1)
        self.assertIn("replacement failed", engine.trigger_message)

    async def test_accepted_opening_replacement_survives_post_accept_persist_failure(self):
        client = FakeClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "short",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 4,
            "total_investment": 100,
            "leverage": 2,
            "initial_order_type": "limit",
            "initial_order_price": 101,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        await engine.initialize()
        engine.running = True
        original_link_id = engine.opening_order["link_id"]
        original_order_id = engine.opening_order["order_id"]
        client.open_limit_order_ids.discard(original_order_id)
        original = next(item for item in client.orders if item["orderId"] == original_order_id)
        original["orderStatus"] = "CANCELED"
        replacement_accepted = False

        def fail_after_replacement_acceptance(current_engine):
            nonlocal replacement_accepted
            opening = current_engine.opening_order
            if (
                opening
                and opening.get("link_id") != original_link_id
                and opening.get("order_id")
            ):
                replacement_accepted = True
                raise OSError("simulated persistence failure after replacement acceptance")

        engine.state_callback = fail_after_replacement_acceptance
        await engine._check_initial_order()

        self.assertTrue(replacement_accepted)
        self.assertTrue(engine.running)
        self.assertTrue(engine.waiting_initial_order)
        self.assertIsNotNone(engine.opening_order)
        replacement_link_id = engine.opening_order["link_id"]
        self.assertNotEqual(replacement_link_id, original_link_id)
        self.assertTrue(engine.opening_order["order_id"])
        self.assertIn("retained", engine.trigger_message)
        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

        engine.state_callback = None
        await engine._check_initial_order()

        self.assertEqual(
            sum(1 for item in client.orders if item.get("order_link_id") == replacement_link_id),
            1,
        )

    async def test_filled_opening_is_owned_when_followup_grid_deployment_fails(self):
        client = FakeClient("100")
        client.trade_details = {}
        client.get_order_trades = lambda symbol, order_id: fake_trade_response(client, order_id)
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "initial_order_type": "limit",
                "initial_order_price": 101,
                "grid_order_post_only": False,
            },
        )

        await engine.initialize()
        engine.running = True
        opening_order = dict(engine.opening_order)
        fill_qty = float(opening_order["qty"])
        client.trade_details[opening_order["order_id"]] = [
            {
                "price": "101",
                "qty": str(fill_qty),
                "volume": str(fill_qty * 101),
                "feeUsdt": "0.05",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]
        client.open_limit_order_ids.discard(opening_order["order_id"])
        client.reject_reduce_limit = True

        await engine._check_initial_order()

        self.assertTrue(engine.initialization_failed)
        self.assertTrue(engine.manual_stop_pending)
        self.assertFalse(engine.grid_ready)
        self.assertFalse(engine.waiting_initial_order)
        self.assertIsNone(engine.opening_order)
        self.assertAlmostEqual(engine.grid_position_net_qty, -fill_qty)
        self.assertAlmostEqual(engine.initial_qty, fill_qty)
        self.assertAlmostEqual(engine.initial_entry_price, 101.0)
        self.assertAlmostEqual(engine.total_volume, fill_qty * 101)
        self.assertIn("confirmed position is retained", engine.trigger_message)
        self.assertEqual(
            [
                order
                for order in client.orders
                if order.get("order_type") == "Market" and order.get("reduce_only")
            ],
            [],
        )

    async def test_post_only_initial_order_filled_status_waits_when_trades_lag(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "maker_fee_rate": 0.0002,
                "initial_order_type": "post_only",
                "initial_order_price": 101,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        client.trade_details = {}
        client.get_order_trades = lambda symbol, order_id: {
            "retCode": 0,
            "result": {"list": client.trade_details.get(order_id, [])},
        }

        await engine.initialize()
        engine.running = True
        opening_order = engine.opening_order
        client.open_limit_order_ids.discard(opening_order["order_id"])

        await engine._check_initial_order()

        self.assertTrue(engine.running)
        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(client.orders), 1)
        self.assertIn("waiting for authoritative execution", engine.trigger_message)

        client.trade_details[opening_order["order_id"]] = [
            {
                "price": "101",
                "qty": opening_order["qty"],
                "volume": str(float(opening_order["qty"]) * 101),
                "feeUsdt": "0.04",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]
        await engine._check_initial_order()

        self.assertFalse(engine.waiting_initial_order)
        self.assertTrue(engine.grid_ready)
        self.assertIsNone(engine.opening_order)
        self.assertAlmostEqual(engine.initial_entry_price, 101.0)
        self.assertGreater(len(engine.active_orders), 0)

    async def test_post_only_initial_price_uses_maker_safe_price_when_config_would_take(self):
        client = FakeClient("100", tick_size="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "initial_order_type": "post_only",
                "initial_order_price": 99,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        self.assertTrue(engine.waiting_initial_order)
        self.assertEqual(client.orders[-1]["price"], "100.1")

    async def test_exchange_trade_fee_details_override_estimated_fee(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "fee_rate": 0.001,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        client.trade_details = {}

        def get_order_trades(symbol, order_id):
            return fake_trade_response(client, order_id)

        client.get_order_trades = get_order_trades
        await engine.initialize()

        take_profit_order = next(
            order for order in engine.active_orders.values() if order["side"] == "Sell" and order["reduce_only"]
        )
        client.trade_details[take_profit_order["order_id"]] = [
            {
                "price": "105",
                "qty": "1",
                "volume": "105",
                "feeUsdt": "0.071",
                "feeAsset": "BNB",
                "isMaker": False,
            }
        ]
        client.open_limit_order_ids.discard(take_profit_order["order_id"])
        await engine._check_fills()

        self.assertEqual(engine.filled_orders[0]["fee_asset"], "BNB")
        self.assertEqual(engine.filled_orders[0]["fee_source"], "exchange")
        self.assertEqual(engine.filled_orders[0]["liquidity"], "taker")
        self.assertAlmostEqual(engine.filled_orders[0]["fee"], 0.071)
        self.assertAlmostEqual(engine.filled_orders[0]["profit"], 4.929)

    async def test_pair_profit_uses_actual_entry_and_exit_prices(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "maker_fee_rate": 0.0002,
                "taker_fee_rate": 0.0005,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        client.trade_details = {}

        def get_order_trades(symbol, order_id):
            return fake_trade_response(client, order_id)

        client.get_order_trades = get_order_trades
        await engine.initialize()

        take_profit_order = next(
            order for order in engine.active_orders.values() if order["side"] == "Sell" and order["reduce_only"]
        )
        self.assertAlmostEqual(float(take_profit_order["entry_price"]), 100.0)
        client.trade_details[take_profit_order["order_id"]] = [
            {
                "price": "104",
                "qty": "1",
                "volume": "104",
                "feeUsdt": "0.0208",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]

        client.open_limit_order_ids.discard(take_profit_order["order_id"])
        await engine._check_fills()

        self.assertAlmostEqual(engine.filled_orders[0]["gross_profit"], 4.0)
        self.assertAlmostEqual(engine.filled_orders[0]["profit"], 3.9792)
        self.assertEqual(engine.filled_orders[0]["liquidity"], "maker")

    async def test_stopping_grid_does_not_replace_cancelled_orders(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )

        await engine.initialize()
        order_count = len(client.orders)
        client.open_limit_order_ids.clear()
        engine._stopping = True
        engine.running = False

        await engine._check_fills()

        self.assertEqual(len(client.orders), order_count)
        self.assertEqual(engine.filled_orders, [])

    async def test_cancelled_grid_order_is_not_treated_as_fill_when_trade_details_are_empty(self):
        client = FakeClient("100")
        client.trade_details = {}
        client.get_order_trades = lambda symbol, order_id: fake_trade_response(client, order_id)
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )

        await engine.initialize()
        order = next(iter(engine.active_orders.values()))
        order_count = len(client.orders)
        client.open_limit_order_ids.discard(order["order_id"])

        await engine._check_fills()

        self.assertEqual(engine.filled_orders, [])
        self.assertEqual(len(client.orders), order_count)
        self.assertIn(order["link_id"], engine.active_orders)

    async def test_cancelled_order_snapshot_replaces_only_unfilled_remainder(self):
        class CancelledPartialSnapshotClient(FakeClient):
            trades_ready = False

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                response["result"].update(
                    {
                        "orderStatus": "CANCELED",
                        "executedQty": "0.4",
                        "cumQuote": "40.4",
                        "avgPrice": "101",
                    }
                )
                return response

            def get_order_trades(self, symbol, order_id):
                trades = []
                if self.trades_ready:
                    trades = [
                        {
                            "price": "101",
                            "qty": "0.4",
                            "volume": "40.4",
                            "feeUsdt": "0.00808",
                            "feeAsset": "USDT",
                            "isMaker": True,
                        }
                    ]
                return {"retCode": 0, "result": {"list": trades}}

        client = CancelledPartialSnapshotClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.reduce_lots_complete = True
        original_link = engine._place("Sell", 101, 0, reduce_only=False, qty_override=1)
        original_id = engine.active_orders[original_link]["order_id"]
        client.open_limit_order_ids.discard(original_id)

        await engine._check_fills()

        self.assertIn(original_link, engine.active_orders)
        self.assertEqual(
            engine.active_orders[original_link]["status"],
            "RECONCILING_EXECUTION",
        )
        self.assertEqual(engine.grid_position_net_qty, 0.0)
        self.assertEqual(len(client.orders), 1)

        client.trades_ready = True
        await engine._check_fills()

        self.assertNotIn(original_link, engine.active_orders)
        replacement_sells = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        reduce_buys = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Buy" and order.get("reduce_only")
        ]
        self.assertEqual(len(replacement_sells), 1)
        self.assertEqual(float(replacement_sells[0]["qty"]), 0.6)
        self.assertEqual(len(reduce_buys), 1)
        self.assertEqual(float(reduce_buys[0]["qty"]), 0.4)
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.4)

    async def test_cancelled_order_waits_until_partial_trade_page_matches_snapshot(self):
        class LaggingCancelledTradesClient(FakeClient):
            confirmed_trade_qty = 0.4

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                response["result"].update(
                    {
                        "orderStatus": "CANCELED",
                        "executedQty": "0.6",
                        "cumQuote": "60.6",
                        "avgPrice": "101",
                    }
                )
                return response

            def get_order_trades(self, symbol, order_id):
                qty = self.confirmed_trade_qty
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "price": "101",
                                "qty": str(qty),
                                "volume": str(qty * 101),
                                "feeUsdt": str(qty * 101 * 0.0002),
                                "feeAsset": "USDT",
                                "isMaker": True,
                            }
                        ]
                    },
                }

        client = LaggingCancelledTradesClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.reduce_lots_complete = True
        original_link = engine._place("Sell", 101, 0, reduce_only=False, qty_override=1)
        original_id = engine.active_orders[original_link]["order_id"]
        client.open_limit_order_ids.discard(original_id)

        await engine._check_fills()

        self.assertIn(original_link, engine.active_orders)
        self.assertEqual(
            engine.active_orders[original_link]["status"],
            "RECONCILING_EXECUTION",
        )
        self.assertAlmostEqual(
            engine.active_orders[original_link]["processed_fill_qty"],
            0.4,
        )
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.4)
        self.assertEqual(
            sum(
                float(order["qty"])
                for order in engine.active_orders.values()
                if order.get("side") == "Sell" and not order.get("reduce_only")
            ),
            1.0,
        )

        client.confirmed_trade_qty = 0.6
        await engine._check_fills()

        self.assertNotIn(original_link, engine.active_orders)
        replacement_sells = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        reduce_buys = [
            order
            for order in engine.active_orders.values()
            if order.get("side") == "Buy" and order.get("reduce_only")
        ]
        self.assertEqual(len(replacement_sells), 1)
        self.assertEqual(float(replacement_sells[0]["qty"]), 0.4)
        self.assertAlmostEqual(
            sum(float(order["qty"]) for order in reduce_buys),
            0.6,
        )
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.6)

    async def test_restored_grid_continues_tracking_saved_orders_after_restart(self):
        snapshots = []
        client = FakeClient("100")
        client.trade_details = {}
        client.get_order_trades = lambda symbol, order_id: fake_trade_response(client, order_id)
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
            state_callback=lambda item: snapshots.append(item.to_state()),
        )

        await engine.initialize()
        client.positions = [{"side": "Sell", "size": str(engine.initial_qty), "avgPrice": "100"}]
        saved = engine.to_state()
        restored = GridEngine(client, saved["config"])
        restored.restore_state(saved)
        take_profit = next(
            order for order in restored.active_orders.values() if order["side"] == "Buy" and order["reduce_only"]
        )
        client.trade_details[take_profit["order_id"]] = [
            {
                "price": take_profit["price"],
                "qty": take_profit["qty"],
                "volume": str(float(take_profit["price"]) * float(take_profit["qty"])),
                "feeUsdt": "0.01",
                "feeAsset": "USDT",
                "isMaker": True,
            }
        ]
        client.open_limit_order_ids.discard(take_profit["order_id"])

        await restored._check_fills()

        self.assertGreater(len(snapshots), 0)
        self.assertEqual(restored.completed_pairs, 1)
        self.assertTrue(
            any(order["side"] == "Sell" and not order["reduce_only"] for order in restored.active_orders.values())
        )

    async def test_reduce_order_retries_without_post_only_when_maker_rejected(self):
        client = FakeClient("100")
        client.reject_post_only_reduce = True
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": True,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        filled_sell = next(order for order in engine.active_orders.values() if order["side"] == "Sell")
        engine.active_orders.pop(filled_sell["link_id"])
        engine._place_counter_order(filled_sell)

        reduce_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("order_type") == "Limit"
        ]
        self.assertTrue(reduce_orders)
        self.assertIsNone(reduce_orders[-1].get("time_in_force"))

    async def test_initial_reduce_order_rejection_prevents_grid_ready_without_market_close(self):
        client = FakeClient("100")
        client.reject_post_only_reduce = True
        client.reject_reduce_limit = True
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": True,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        with self.assertRaisesRegex(RuntimeError, "Initial grid deployment is incomplete"):
            await engine.initialize()

        market_reduce_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy"
            and order.get("reduce_only")
            and order.get("order_type") == "Market"
        ]
        self.assertFalse(engine.grid_ready)
        self.assertEqual(market_reduce_orders, [])

    async def test_runtime_reduce_order_rejection_does_not_market_close(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": True,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        client.reject_post_only_reduce = True
        client.reject_reduce_limit = True
        filled_sell = next(order for order in engine.active_orders.values() if order["side"] == "Sell")
        engine.active_orders.pop(filled_sell["link_id"])
        placed = engine._place_counter_order(filled_sell)

        market_reduce_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("order_type") == "Market"
        ]
        self.assertFalse(placed)
        self.assertEqual(market_reduce_orders, [])
        self.assertIn("without market-closing", engine.trigger_message)

    async def test_reduce_counter_order_is_placed_even_outside_grid_range(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        filled_sell = next(order for order in engine.active_orders.values() if order["side"] == "Sell")
        engine.active_orders.pop(filled_sell["link_id"])
        before_reduce_buys = [
            order for order in client.orders if order.get("side") == "Buy" and order.get("reduce_only")
        ]

        engine.current_price = 89
        handled = engine._handle_closed_order(filled_sell)

        after_reduce_buys = [
            order for order in client.orders if order.get("side") == "Buy" and order.get("reduce_only")
        ]
        self.assertTrue(handled)
        self.assertEqual(len(after_reduce_buys), len(before_reduce_buys) + 1)
        self.assertEqual(engine.get_status()["paused_replacements_count"], 0)

    async def test_short_reduce_fill_reopens_passive_sell_below_lower(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        filled_link, filled_buy = next(
            (link_id, order)
            for link_id, order in engine.active_orders.items()
            if order["side"] == "Buy" and order["reduce_only"]
        )
        before_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]

        engine.current_price = 89
        handled = engine._handle_closed_order(filled_buy)

        after_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        self.assertTrue(handled)
        self.assertEqual(len(after_open_sells), len(before_open_sells) + 1)
        self.assertEqual(engine.get_status()["paused_replacements_count"], 0)

    async def test_short_reduce_fill_reopens_even_above_upper_when_marketable(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        _, filled_buy = next(
            (link_id, order)
            for link_id, order in engine.active_orders.items()
            if order["side"] == "Buy" and order["reduce_only"]
        )
        before_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]

        engine.current_price = 111
        handled = engine._handle_closed_order(filled_buy)

        after_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        self.assertTrue(handled)
        self.assertEqual(len(after_open_sells), len(before_open_sells) + 1)
        self.assertEqual(engine.get_status()["paused_replacements_count"], 0)

    async def test_long_reduce_fill_reopens_passive_buy_above_upper(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        filled_sell = next(order for order in engine.active_orders.values() if order["side"] == "Sell")
        before_open_buys = [
            order for order in client.orders if order.get("side") == "Buy" and not order.get("reduce_only")
        ]

        engine.current_price = 111
        handled = engine._handle_closed_order(filled_sell)

        after_open_buys = [
            order for order in client.orders if order.get("side") == "Buy" and not order.get("reduce_only")
        ]
        self.assertTrue(handled)
        self.assertEqual(len(after_open_buys), len(before_open_buys) + 1)
        self.assertEqual(engine.get_status()["paused_replacements_count"], 0)

    async def test_full_20_level_directional_grid_restores_every_order_beyond_boundary(self):
        for direction in ("short", "long"):
            with self.subTest(direction=direction):
                client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
                engine = GridEngine(
                    client,
                    {
                        "symbol": "TESTUSDT",
                        "direction": direction,
                        "grid_mode": "arithmetic",
                        "upper_price": 110,
                        "lower_price": 90,
                        "grid_count": 20,
                        "total_investment": 0,
                        "position_sizing_mode": "fixed_grid_qty",
                        "grid_order_qty": 100,
                        "leverage": 2,
                        "grid_order_post_only": False,
                    },
                )
                await engine.initialize()
                reduce_orders = [
                    order
                    for order in engine.active_orders.values()
                    if order["reduce_only"]
                ]
                self.assertEqual(len(reduce_orders), 10)

                for order in reduce_orders:
                    client.open_limit_order_ids.discard(order["order_id"])
                client.positions = []
                engine.current_price = 89 if direction == "short" else 111

                await engine._check_fills()

                active_orders = list(engine.active_orders.values())
                expected_side = "Sell" if direction == "short" else "Buy"
                self.assertEqual(len(active_orders), 20)
                self.assertTrue(all(not order["reduce_only"] for order in active_orders))
                self.assertTrue(all(order["side"] == expected_side for order in active_orders))
                self.assertEqual({float(order["qty"]) for order in active_orders}, {100.0})
                self.assertEqual(
                    len({(order["level_idx"], order["side"]) for order in active_orders}),
                    20,
                )
                self.assertEqual(engine.paused_replacements, [])
                self.assertAlmostEqual(engine._grid_position_qty(), 0.0)
                self.assertFalse(engine.grid_coverage_snapshot()["has_risk"])

    async def test_paused_passive_reentry_resumes_even_while_price_outside_grid(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        filled_buy = next(order for order in engine.active_orders.values() if order["side"] == "Buy")
        before_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        engine.paused_replacements = [filled_buy]
        engine.current_price = 89

        engine._resume_paused_replacements()

        after_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        self.assertEqual(len(after_open_sells), len(before_open_sells) + 1)
        self.assertEqual(engine.get_status()["paused_replacements_count"], 0)

    async def test_boundary_break_does_not_market_close_by_default(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "2.5"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": True,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        engine.current_price = 89
        engine.active_orders = {
            key: order
            for key, order in engine.active_orders.items()
            if not order["reduce_only"]
        }

        engine._repair_boundary_position()

        repair_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("order_type") == "Market"
        ]
        self.assertFalse(repair_orders)

    async def test_boundary_break_does_not_cancel_stale_reduce_orders_by_default(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "2.5"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        engine.current_price = 89
        engine.active_orders = {
            key: order
            for key, order in engine.active_orders.items()
            if order["reduce_only"] and order["side"] == "Buy"
        }

        engine._repair_boundary_position()

        repair_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("order_type") == "Market"
        ]
        self.assertFalse(client.cancelled_orders)
        self.assertFalse(repair_orders)
        self.assertTrue(engine.active_orders)

    async def test_boundary_market_repair_is_explicit_opt_in(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "grid_order_post_only": False,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
                "boundary_market_repair": True,
            },
        )

        await engine.initialize()
        client.positions = [{"side": "Sell", "size": str(engine.initial_qty), "avgPrice": "100"}]
        engine.current_price = 89
        engine.active_orders = {}

        engine._repair_boundary_position()
        engine._repair_boundary_position()

        repair_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("order_type") == "Market"
        ]
        self.assertEqual(len(repair_orders), 1)
        self.assertAlmostEqual(float(repair_orders[0]["qty"]), engine.initial_qty)
        self.assertGreater(engine._boundary_repair_retry_after, 0)

    async def test_boundary_reduce_fallback_fill_restores_flat_short_grid_without_aggregate_order(self):
        client = FakeClient("89", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 100,
                "boundary_reduce_fallback": True,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_ready = True
        engine.running = True
        engine.grid_position_net_qty = -234
        engine.target_qty_by_level = {str(level): 100 for level in range(4)}

        self.assertTrue(engine._repair_missing_reduce_at_boundary())
        link_id, reduce_order = next(
            (link_id, order)
            for link_id, order in engine.active_orders.items()
            if order["side"] == "Buy" and order["reduce_only"]
        )
        self.assertEqual(reduce_order.get("tag"), "boundary_reduce_fallback")
        client.open_limit_order_ids.discard(str(reduce_order["order_id"]))

        self.assertTrue(engine._handle_confirmed_closed_order(link_id, reduce_order, allow_estimate=True))

        sell_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Sell" and not order["reduce_only"]
        ]
        self.assertEqual(
            sorted((order["level_idx"], order["price"], order["qty"]) for order in sell_orders),
            [(0, "95.0", "100"), (1, "100.0", "100"), (2, "105.0", "100"), (3, "110.0", "100")],
        )
        self.assertFalse(any(order["qty"] == "234" for order in sell_orders))
        self.assertEqual(engine.grid_position_net_qty, 0.0)

    async def test_flat_short_grid_repairs_missing_open_side_remaining_qty(self):
        client = FakeClient("89", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 100,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 95, 100, 105, 110]
        engine.grid_ready = True
        engine.grid_position_net_qty = 0
        engine.target_qty_by_level = {str(level): 100 for level in range(4)}
        engine.active_orders = {
            "level0": {
                "link_id": "level0",
                "order_id": "level0",
                "level_idx": 0,
                "side": "Sell",
                "price": "95.0",
                "qty": "100",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "entry_price": None,
                "processed_fill_qty": 0.0,
            },
            "level1_partial": {
                "link_id": "level1_partial",
                "order_id": "level1_partial",
                "level_idx": 1,
                "side": "Sell",
                "price": "100.0",
                "qty": "100",
                "status": "PARTIALLY_FILLED",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "entry_price": None,
                "processed_fill_qty": 40.0,
            },
            "level2_excess": {
                "link_id": "level2_excess",
                "order_id": "level2_excess",
                "level_idx": 2,
                "side": "Sell",
                "price": "105.0",
                "qty": "150",
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "entry_price": None,
                "processed_fill_qty": 0.0,
            },
        }

        self.assertTrue(engine._repair_flat_open_side_grid())

        placed_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        self.assertEqual(
            sorted((order["price"], order["qty"]) for order in placed_sells),
            [("100.0", "40"), ("110.0", "100")],
        )
        self.assertEqual(client.cancelled_orders, [])

    async def test_neutral_grid_deploys_both_buy_and_sell_limit_orders_without_market_position(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()

        market_orders = [o for o in client.orders if o.get("order_type") == "Market"]
        limit_orders = [o for o in client.orders if o.get("order_type") == "Limit"]
        buy_orders = [o for o in limit_orders if o["side"] == "Buy"]
        sell_orders = [o for o in limit_orders if o["side"] == "Sell"]

        self.assertEqual(len(market_orders), 0)
        self.assertGreater(len(buy_orders), 0)
        self.assertGreater(len(sell_orders), 0)

    async def test_trigger_price_waits_until_hit(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": 105,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        self.assertTrue(engine.waiting_trigger)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(engine.active_orders), 0)

    async def test_geometric_grid_levels_and_profit_rate(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "geometric",
                "upper_price": 121,
                "lower_price": 81,
                "grid_count": 2,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        self.assertEqual(engine.grid_levels, [81.0, 99.0, 121.0])
        self.assertAlmostEqual(engine.grid_profit_pct, 22.222222, places=4)

    async def test_risk_shutdown_closes_position_with_market_reduce_only(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": 95,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        client.positions = [{"side": "Buy", "size": str(engine.initial_qty), "avgPrice": "100"}]
        await engine._shutdown_with_close()

        close_orders = [
            order
            for order in client.orders
            if order.get("order_type") == "Market" and order.get("reduce_only")
        ]
        self.assertEqual(len(close_orders), 1)
        self.assertEqual(close_orders[0]["side"], "Sell")

    async def test_long_grid_keeps_running_when_price_breaks_upper_range(self):
        client = FakeClient("100")
        client.positions = [{"side": "Buy", "size": "4.0"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
            },
        )

        await engine.initialize()
        self.assertFalse(engine._risk_hit(111))
        self.assertGreater(len(engine.active_orders), 0)

    async def test_short_grid_keeps_running_when_price_breaks_lower_range(self):
        client = FakeClient("100")
        client.positions = [{"side": "Sell", "size": "4.0"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 4,
                "total_investment": 100,
                "leverage": 2,
                "trigger_price": None,
                "stop_loss_price": None,
                "take_profit_price": None,
            },
        )

        await engine.initialize()
        self.assertFalse(engine._risk_hit(89))
        self.assertGreater(len(engine.active_orders), 0)

    async def test_qty_step_reduce_remainder_below_min_qty_is_reprotected(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        client.positions = [{"side": "Sell", "size": "0.01", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "qty_per_grid": 0.2,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.grid_ready = True
        engine.grid_position_net_qty = -0.01
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"0": {"qty": 0.01, "entry_value": 1.0}}

        repaired = engine._repair_missing_reduce_protection_from_ledger()

        self.assertTrue(repaired)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["qty"], "0.01")
        self.assertTrue(client.orders[0]["reduce_only"])
        self.assertFalse(engine.reduce_protection_snapshot()["has_risk"])

    async def test_cancelled_qty_step_reduce_remainder_below_min_qty_is_replaced(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "qty_per_grid": 0.2,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        link_id = engine._place(
            "Buy",
            90,
            0,
            reduce_only=True,
            qty_override=0.01,
            entry_price=100,
        )
        order = engine.active_orders[link_id]
        client.open_limit_order_ids.discard(order["order_id"])

        engine._handle_cancelled_order(link_id, order)

        replacements = list(engine.active_orders.values())
        self.assertEqual(len(replacements), 1)
        self.assertNotEqual(replacements[0]["order_id"], order["order_id"])
        self.assertEqual(replacements[0]["qty"], "0.01")
        self.assertTrue(replacements[0]["reduce_only"])

    async def test_failed_counter_order_is_queued_and_retried(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.grid_ready = True
        order = {
            "link_id": "g_0_B_counter",
            "order_id": "filled-1",
            "level_idx": 0,
            "side": "Buy",
            "price": "90",
            "qty": "1",
            "status": "open",
            "order_type": "Limit",
            "time_in_force": "GTC",
            "reduce_only": False,
            "entry_price": None,
            "processed_fill_qty": 0,
            "processed_fill_volume": 0,
            "processed_fill_fee": 0,
        }
        original_place_counter = engine._place_counter_order
        attempts = 0

        def fail_once(filled_order):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return False
            return original_place_counter(filled_order)

        engine._place_counter_order = fail_once

        recorded = engine._record_execution_delta(
            order,
            {
                "price": 90,
                "qty": 1,
                "volume": 90,
                "fee": 0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        self.assertTrue(recorded)
        self.assertEqual(len(engine.paused_replacements), 1)
        self.assertEqual(client.orders, [])

        engine._resume_paused_replacements()

        self.assertEqual(engine.paused_replacements, [])
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["side"], "Sell")
        self.assertEqual(client.orders[0]["qty"], "1")

    async def test_batch_timeout_does_not_duplicate_adopted_reduce_order(self):
        class TimeoutAfterAcceptClient(BatchFakeClient):
            def place_orders(self, orders):
                self.batch_calls.append([dict(order) for order in orders])
                for order in orders:
                    self.place_order(**order)
                raise TimeoutError("response lost after exchange acceptance")

        client = TimeoutAfterAcceptClient("100", tick_size="1", qty_step="0.01", min_qty="0.01")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "qty_per_grid": 0.2,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.grid_ready = True

        links = engine._place_batch_limit_orders(
            [
                {
                    "side": "Buy",
                    "price": 90,
                    "level_idx": 0,
                    "reduce_only": True,
                    "qty_override": 0.2,
                    "entry_price": 100,
                    "allow_duplicate": True,
                }
            ]
        )

        self.assertEqual(len(client.orders), 1)
        self.assertEqual(len(engine.active_orders), 1)
        self.assertEqual(set(links), set(engine.active_orders))

    async def test_risk_close_handles_qty_step_remainder_below_min_qty(self):
        client = FakeClient("100", tick_size="1", qty_step="0.01", min_qty="0.1")
        client.positions = [{"side": "Sell", "size": "0.01", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 0.2,
                "qty_per_grid": 0.2,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_position_net_qty = -0.01

        engine._close_all_positions()

        close_orders = [order for order in client.orders if order.get("order_type") == "Market"]
        self.assertEqual(len(close_orders), 1)
        self.assertEqual(close_orders[0]["side"], "Buy")
        self.assertEqual(close_orders[0]["qty"], "0.01")
        self.assertTrue(close_orders[0]["reduce_only"])

    async def test_risk_close_splits_at_exchange_market_max_without_overclosing(self):
        class FilledMarketClient(FakeClient):
            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                if order.get("order_type") == "Market":
                    qty = float(order["qty"])
                    response["result"].update(
                        {
                            "orderLinkId": order.get("order_link_id", ""),
                            "orderStatus": "FILLED",
                            "executedQty": str(qty),
                            "cumQuote": str(qty * self.ticker_price),
                            "avgPrice": str(self.ticker_price),
                        }
                    )
                return response

            def get_order_trades(self, symbol, order_id):
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                qty = float(order["qty"])
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "qty": str(qty),
                                "price": str(self.ticker_price),
                                "volume": str(qty * self.ticker_price),
                                "feeUsdt": "0",
                                "feeAsset": "USDT",
                                "isMaker": False,
                            }
                        ]
                    },
                }

        client = FilledMarketClient(
            "100",
            tick_size="1",
            qty_step="0.1",
            min_qty="0.1",
            max_market_qty="0.4",
        )
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.current_price = 100
        engine.grid_position_net_qty = -1.0

        self.assertFalse(engine._close_all_positions())
        self.assertFalse(engine._close_all_positions())
        self.assertTrue(engine._close_all_positions())

        close_orders = [order for order in client.orders if order.get("order_type") == "Market"]
        self.assertEqual([order["qty"] for order in close_orders], ["0.4", "0.4", "0.2"])
        self.assertTrue(all(order.get("reduce_only") for order in close_orders))
        self.assertEqual(client.positions, [])
        self.assertEqual(engine.grid_position_net_qty, 0.0)

    async def test_single_order_timeout_is_reconciled_by_original_client_order_id(self):
        class TimeoutAfterAcceptClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submit_calls = 0

            def place_order(self, **kwargs):
                self.submit_calls += 1
                result = super().place_order(**kwargs)
                if self.submit_calls == 1:
                    raise TimeoutError("response lost after exchange acceptance")
                return result

        client = TimeoutAfterAcceptClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]

        link_id = engine._place(
            "Buy",
            90,
            0,
            reduce_only=True,
            qty_override=1,
            entry_price=100,
            allow_duplicate=True,
        )

        self.assertIsNotNone(link_id)
        self.assertEqual(client.submit_calls, 1)
        self.assertEqual(len(client.orders), 1)
        self.assertTrue(engine.active_orders[link_id]["submission_pending"])

        engine._reconcile_exchange_open_orders()

        self.assertEqual(client.submit_calls, 1)
        self.assertEqual(len(client.orders), 1)
        self.assertFalse(engine.active_orders[link_id].get("submission_pending", False))
        self.assertEqual(engine.active_orders[link_id]["order_id"], "1")

    async def test_initial_limit_timeout_adopts_original_opening_order_without_duplicate(self):
        class TimeoutAfterAcceptClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.opening_submit_calls = 0

            def place_order(self, **kwargs):
                result = super().place_order(**kwargs)
                if str(kwargs.get("order_link_id", "")).startswith("open_"):
                    self.opening_submit_calls += 1
                    if self.opening_submit_calls == 1:
                        raise TimeoutError("opening acknowledgement lost")
                return result

        client = TimeoutAfterAcceptClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "initial_order_type": "limit",
                "initial_order_price": 101,
                "grid_order_post_only": False,
            },
        )

        await engine.initialize()

        self.assertTrue(engine.waiting_initial_order)
        self.assertTrue(engine.opening_order["submission_pending"])
        self.assertEqual(client.opening_submit_calls, 1)
        self.assertEqual(
            len(
                [
                    order
                    for order in client.orders
                    if str(order.get("order_link_id", "")).startswith("open_")
                ]
            ),
            1,
        )

        await engine._check_initial_order()

        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.opening_order.get("submission_pending", False))
        self.assertEqual(engine.opening_order["order_id"], "1")
        self.assertEqual(client.opening_submit_calls, 1)
        self.assertEqual(len(client.orders), 1)

    async def test_initial_market_timeout_adopts_fill_and_never_opens_twice(self):
        class MarketTimeoutAfterAcceptClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.market_submit_calls = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Market" and not kwargs.get("reduce_only"):
                    self.market_submit_calls += 1
                    result = super().place_order(**kwargs)
                    if self.market_submit_calls == 1:
                        raise TimeoutError("market acknowledgement lost after fill")
                    return result
                return super().place_order(**kwargs)

            def get_order_by_link(self, symbol, order_link_id):
                response = super().get_order_by_link(symbol, order_link_id)
                if response.get("result", {}).get("orderId"):
                    order = next(
                        item
                        for item in self.orders
                        if str(item.get("order_link_id", "")) == str(order_link_id)
                    )
                    if order.get("order_type") == "Market":
                        response["result"].update(
                            {
                                "orderStatus": "FILLED",
                                "avgPrice": str(self.ticker_price),
                                "executedQty": str(order["qty"]),
                                "cumQuote": str(float(order["qty"]) * self.ticker_price),
                            }
                        )
                return response

        client = MarketTimeoutAfterAcceptClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            },
        )

        await engine.initialize()

        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertTrue(engine.opening_order["submission_pending"])
        self.assertEqual(client.market_submit_calls, 1)
        self.assertEqual(client.positions[0]["side"], "Sell")
        self.assertEqual(float(client.positions[0]["size"]), 1.0)

        await engine._check_initial_order()

        self.assertEqual(client.market_submit_calls, 1)
        self.assertIsNone(engine.opening_order)
        self.assertFalse(engine.waiting_initial_order)
        self.assertTrue(engine.grid_ready)
        self.assertEqual(engine.grid_position_net_qty, -1.0)
        self.assertEqual(
            len(
                [
                    order
                    for order in client.orders
                    if order.get("order_type") == "Market"
                    and not order.get("reduce_only")
                ]
            ),
            1,
        )

    async def test_successful_market_ack_waits_for_actual_execution_before_grid_deploy(self):
        class DelayedMarketExecutionClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.trade_details = {}

            def get_order_trades(self, symbol, order_id):
                return {
                    "retCode": 0,
                    "result": {"list": self.trade_details.get(str(order_id), [])},
                }

        client = DelayedMarketExecutionClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "initial_order_type": "market",
                "grid_order_post_only": False,
            },
        )

        await engine.initialize()

        opening_order = dict(engine.opening_order)
        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(engine.grid_position_net_qty, 0.0)
        self.assertEqual(engine.active_orders, {})
        self.assertEqual(
            len([order for order in client.orders if order.get("order_type") == "Market"]),
            1,
        )

        await engine._check_initial_order()

        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(client.orders), 1)

        qty = float(opening_order["qty"])
        partial_qty = qty / 2
        client.trade_details[opening_order["order_id"]] = [
            {
                "price": "100.2",
                "qty": str(partial_qty),
                "volume": str(partial_qty * 100.2),
                "feeUsdt": "0.025",
                "feeAsset": "USDT",
                "isMaker": False,
            }
        ]
        await engine._check_initial_order()

        self.assertTrue(engine.waiting_initial_order)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(engine.grid_position_net_qty, 0.0)

        client.trade_details[opening_order["order_id"]] = [
            {
                "price": "100.2",
                "qty": str(qty),
                "volume": str(qty * 100.2),
                "feeUsdt": "0.05",
                "feeAsset": "USDT",
                "isMaker": False,
            }
        ]
        await engine._check_initial_order()

        self.assertFalse(engine.waiting_initial_order)
        self.assertTrue(engine.grid_ready)
        self.assertIsNone(engine.opening_order)
        self.assertAlmostEqual(engine.initial_entry_price, 100.2)
        self.assertEqual(engine.grid_position_net_qty, -qty)
        self.assertEqual(
            len([order for order in client.orders if order.get("order_type") == "Market"]),
            1,
        )

    async def test_unaccepted_single_order_retries_with_same_client_order_id(self):
        class TimeoutBeforeAcceptClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submitted_links = []

            def place_order(self, **kwargs):
                self.submitted_links.append(str(kwargs.get("order_link_id", "")))
                if len(self.submitted_links) == 1:
                    raise TimeoutError("connection lost before exchange acceptance")
                return super().place_order(**kwargs)

        client = TimeoutBeforeAcceptClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]

        link_id = engine._place(
            "Buy",
            90,
            0,
            reduce_only=True,
            qty_override=1,
            entry_price=100,
            allow_duplicate=True,
        )

        self.assertIsNotNone(link_id)
        self.assertEqual(client.orders, [])
        self.assertTrue(engine.active_orders[link_id]["submission_pending"])

        for _ in range(5):
            engine.active_orders[link_id]["submission_updated_at"] = 0
            engine.active_orders[link_id]["submission_last_not_found_at"] = 0
            engine._reconcile_exchange_open_orders()

        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.submitted_links, [link_id, link_id])
        self.assertFalse(engine.active_orders[link_id].get("submission_pending", False))

    async def test_delayed_order_visibility_is_adopted_without_retrying_submission(self):
        class DelayedVisibilityClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submit_calls = 0
                self.lookup_calls = 0

            def place_order(self, **kwargs):
                self.submit_calls += 1
                result = super().place_order(**kwargs)
                if self.submit_calls == 1:
                    raise TimeoutError("accepted order is temporarily invisible")
                return result

            def get_open_orders(self, symbol):
                if self.lookup_calls < 5:
                    return {"retCode": 0, "result": {"list": []}}
                return super().get_open_orders(symbol)

            def get_order_by_link(self, symbol, order_link_id):
                self.lookup_calls += 1
                if self.lookup_calls < 5:
                    return {"retCode": 0, "result": {}}
                return super().get_order_by_link(symbol, order_link_id)

            def get_order_history(self, symbol, limit=50):
                return {"retCode": 0, "result": {"list": []}}

        client = DelayedVisibilityClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]

        link_id = engine._place(
            "Buy",
            90,
            0,
            reduce_only=True,
            qty_override=1,
            entry_price=100,
            allow_duplicate=True,
        )
        for _ in range(5):
            engine.active_orders[link_id]["submission_updated_at"] = 0
            engine.active_orders[link_id]["submission_last_not_found_at"] = 0
            engine._reconcile_exchange_open_orders()

        self.assertEqual(client.submit_calls, 1)
        self.assertEqual(len(client.orders), 1)
        self.assertFalse(engine.active_orders[link_id].get("submission_pending", False))

    async def test_unconfirmed_market_open_is_never_automatically_resubmitted(self):
        class MissingMarketClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.market_submit_calls = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Market" and not kwargs.get("reduce_only"):
                    self.market_submit_calls += 1
                    raise TimeoutError("market write result is unknown")
                return super().place_order(**kwargs)

            def get_order_by_link(self, symbol, order_link_id):
                return {"retCode": 0, "result": {}}

            def get_order_history(self, symbol, limit=50):
                return {"retCode": 0, "result": {"list": []}}

        client = MissingMarketClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.current_price = 100

        self.assertFalse(engine._place_market_open("Sell", 1))
        for _ in range(8):
            engine.opening_order["submission_updated_at"] = 0
            engine.opening_order["submission_last_not_found_at"] = 0
            await engine._check_initial_order()

        self.assertEqual(client.market_submit_calls, 1)
        self.assertTrue(engine.waiting_initial_order)
        self.assertTrue(engine.opening_order.get("submission_pending"))
        self.assertTrue(engine.opening_order.get("submission_retry_blocked"))
        self.assertEqual(engine.opening_order.get("submission_attempts"), 1)

    async def test_unconfirmed_market_reduce_is_never_automatically_resubmitted(self):
        class MissingReduceClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reduce_submit_calls = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Market" and kwargs.get("reduce_only"):
                    self.reduce_submit_calls += 1
                    raise TimeoutError("reduce write result is unknown")
                return super().place_order(**kwargs)

            def get_order_by_link(self, symbol, order_link_id):
                return {"retCode": 0, "result": {}}

            def get_order_history(self, symbol, limit=50):
                return {"retCode": 0, "result": {"list": []}}

        client = MissingReduceClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.current_price = 100
        engine.grid_position_net_qty = -1.0

        self.assertFalse(engine._close_all_positions())
        for _ in range(8):
            engine.pending_reduce_action["submission_updated_at"] = 0
            engine.pending_reduce_action["submission_last_not_found_at"] = 0
            self.assertFalse(engine._close_all_positions())

        self.assertEqual(client.reduce_submit_calls, 1)
        self.assertTrue(engine.pending_reduce_action.get("submission_pending"))
        self.assertTrue(engine.pending_reduce_action.get("submission_retry_blocked"))
        self.assertEqual(engine.pending_reduce_action.get("submission_attempts"), 1)
        self.assertEqual(engine.grid_position_net_qty, -1.0)
        self.assertEqual(client.positions[0]["size"], "1")

    async def test_partial_batch_timeout_recovers_each_original_client_order_id(self):
        class PartialBatchTimeoutClient(BatchFakeClient):
            def place_orders(self, orders):
                self.batch_calls.append([dict(order) for order in orders])
                self.place_order(**orders[0])
                raise TimeoutError("batch response lost after first order acceptance")

        client = PartialBatchTimeoutClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "qty_per_grid": 1,
                "leverage": 2,
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]

        links = engine._place_batch_limit_orders(
            [
                {
                    "side": "Buy",
                    "price": 90,
                    "level_idx": 0,
                    "reduce_only": True,
                    "qty_override": 1,
                    "entry_price": 100,
                    "allow_duplicate": True,
                },
                {
                    "side": "Buy",
                    "price": 100,
                    "level_idx": 1,
                    "reduce_only": True,
                    "qty_override": 1,
                    "entry_price": 105,
                    "allow_duplicate": True,
                },
            ]
        )

        self.assertEqual(len(links), 2)
        self.assertEqual(len(engine.active_orders), 2)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(
            sum(bool(order.get("submission_pending")) for order in engine.active_orders.values()),
            1,
        )

        pending_link = next(
            link for link, order in engine.active_orders.items() if order.get("submission_pending")
        )
        for _ in range(5):
            engine.active_orders[pending_link]["submission_updated_at"] = 0
            engine.active_orders[pending_link]["submission_last_not_found_at"] = 0
            engine._reconcile_exchange_open_orders()

        self.assertEqual(len(client.orders), 2)
        self.assertEqual(len({order["order_link_id"] for order in client.orders}), 2)
        self.assertEqual(set(links), {order["order_link_id"] for order in client.orders})
        self.assertFalse(
            any(order.get("submission_pending") for order in engine.active_orders.values())
        )

    async def test_pending_submission_survives_state_round_trip_without_new_id(self):
        class AlwaysTimeoutClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submitted_links = []

            def place_order(self, **kwargs):
                self.submitted_links.append(str(kwargs.get("order_link_id", "")))
                raise TimeoutError("submission outcome unknown")

        client = AlwaysTimeoutClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        config = {
            "symbol": "TESTUSDT",
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
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 110]

        link_id = engine._place(
            "Buy",
            90,
            0,
            reduce_only=True,
            qty_override=1,
            entry_price=100,
            allow_duplicate=True,
        )
        state = engine.to_state()

        restored = GridEngine(client, config)
        restored.restore_state(state)

        self.assertIn(link_id, restored.active_orders)
        self.assertTrue(restored.active_orders[link_id]["submission_pending"])
        self.assertEqual(restored.active_orders[link_id]["link_id"], link_id)
        self.assertEqual(client.submitted_links, [link_id])

    async def test_pending_submission_rejects_same_link_with_different_order_shape(self):
        client = FakeClient("100", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})
        engine._fetch_precision()
        wrong_qty_order = engine._pending_limit_order_state(
            link_id="g_1_S_shape",
            level_idx=1,
            side="Sell",
            price="101.0",
            qty="1.0",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )

        wrong_qty = engine._confirm_pending_submission(
            wrong_qty_order,
            {
                "orderId": "10",
                "orderLinkId": "g_1_S_shape",
                "side": "Sell",
                "price": "101.0",
                "qty": "1.1",
                "reduceOnly": False,
            },
        )
        wrong_price_order = engine._pending_limit_order_state(
            link_id="g_1_S_price",
            level_idx=1,
            side="Sell",
            price="101.0",
            qty="1.0",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )
        wrong_price = engine._confirm_pending_submission(
            wrong_price_order,
            {
                "orderId": "11",
                "orderLinkId": "g_1_S_price",
                "side": "Sell",
                "price": "101.1",
                "qty": "1.0",
                "reduceOnly": False,
            },
        )

        self.assertFalse(wrong_qty)
        self.assertFalse(wrong_price)
        self.assertFalse(wrong_qty_order.get("submission_pending", False))
        self.assertFalse(wrong_price_order.get("submission_pending", False))
        self.assertEqual(wrong_qty_order["order_id"], "10")
        self.assertEqual(wrong_price_order["order_id"], "11")
        self.assertEqual(wrong_qty_order["expected_qty"], "1.0")
        self.assertEqual(wrong_qty_order["qty"], "1.1")
        self.assertEqual(wrong_price_order["expected_price"], "101.0")
        self.assertEqual(wrong_price_order["price"], "101.1")
        self.assertTrue(engine.manual_stop_pending)
        self.assertFalse(engine.grid_ready)

        engine._record_execution_delta(
            wrong_qty_order,
            {
                "price": 101.0,
                "qty": 1.1,
                "volume": 111.1,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
            place_counter=False,
        )
        self.assertAlmostEqual(engine.grid_position_net_qty, -1.1)

        correct_order = engine._pending_limit_order_state(
            link_id="g_1_S_correct",
            level_idx=1,
            side="Sell",
            price="101.0",
            qty="1.0",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )
        confirmed = engine._confirm_pending_submission(
            correct_order,
            {
                "orderId": "12",
                "orderLinkId": "g_1_S_correct",
                "side": "Sell",
                "price": "101.0",
                "qty": "1.0",
                "reduceOnly": False,
                "orderStatus": "NEW",
            },
        )

        self.assertTrue(confirmed)
        self.assertEqual(correct_order["order_id"], "12")

    async def test_confirmed_order_shape_change_is_never_silently_adopted(self):
        client = FakeClient("100", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})
        engine._fetch_precision()
        link_id = engine._place(
            "Sell",
            101,
            1,
            reduce_only=False,
            qty_override=1,
        )
        self.assertIsNotNone(link_id)
        client.orders[0]["qty"] = "0.7"

        changed = engine._reconcile_exchange_open_orders()

        order = engine.active_orders[link_id]
        self.assertTrue(changed)
        self.assertEqual(order["order_id"], "1")
        self.assertEqual(order["expected_qty"], "1.0")
        self.assertEqual(order["exchange_accepted_qty"], "0.7")
        self.assertEqual(order["qty"], "0.7")
        self.assertIn("expected=1.0 actual=0.7", order["accepted_shape_mismatch"])
        self.assertTrue(engine.manual_stop_pending)
        self.assertFalse(engine.grid_ready)
        self.assertEqual(len(client.orders), 1)

    async def test_generated_grid_client_order_id_uses_exchange_safe_entropy(self):
        client = FakeClient("100")
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "neutral"})
        engine._fetch_precision()

        link_id = engine._place(
            "Buy",
            90,
            0,
            reduce_only=False,
            qty_override=1,
        )

        self.assertRegex(link_id, r"^g_0_B_[0-9a-f]{16}$")
        self.assertLessEqual(len(link_id), 36)

    async def test_stop_cancels_only_managed_orders_and_preserves_manual_order(self):
        client = FakeClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        grid_link = engine._place(
            "Sell",
            110,
            0,
            reduce_only=False,
            qty_override=1,
        )
        manual = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="0.5",
            price="95",
            order_type="Limit",
            reduce_only=False,
            order_link_id="manual_order",
        )
        manual_id = str(manual["result"]["orderId"])
        grid_id = str(engine.active_orders[grid_link]["order_id"])
        engine.running = True

        await engine.stop()

        self.assertNotIn(grid_id, client.open_limit_order_ids)
        self.assertIn(manual_id, client.open_limit_order_ids)

    async def test_terminal_stop_clears_restore_refresh_retry_state(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
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
        engine.running = True
        engine.grid_ready = True
        engine.restore_refresh_pending = True
        engine.restore_refresh_error = "exchangeInfo unavailable"
        engine.restore_refresh_retry_after = time.time() + 60
        engine.restore_refresh_attempts = 4
        engine._restore_saved_running = True
        engine._restore_legacy_bootstrap_pending = True
        engine._restore_previous_trigger_message = "previous status"
        engine.trigger_message = "Restore refresh paused: exchangeInfo unavailable"

        await engine.stop()

        durable = engine.to_state()
        self.assertFalse(durable["running"])
        self.assertFalse(durable["restore_refresh_pending"])
        self.assertEqual(durable["restore_refresh_error"], "")
        self.assertEqual(durable["restore_refresh_retry_after"], 0.0)
        self.assertEqual(durable["restore_refresh_attempts"], 0)
        self.assertFalse(durable["restore_saved_running"])
        self.assertFalse(durable["restore_legacy_bootstrap_pending"])
        self.assertEqual(durable["restore_previous_trigger_message"], "")
        self.assertEqual(durable["trigger_message"], "")

    async def test_stop_records_cancel_race_fill_without_placing_counter_order(self):
        class FillWhileCancellingClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.fill_order_id = ""

            def cancel_order(self, symbol, order_id):
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                self.open_limit_order_ids.discard(str(order_id))
                order["orderStatus"] = "CANCELED"
                self.fill_order_id = str(order_id)
                self.positions = [{"side": "Sell", "size": "0.4", "avgPrice": "101"}]
                return {"retCode": 0}

            def get_order_trades(self, symbol, order_id):
                trades = []
                if str(order_id) == self.fill_order_id:
                    trades = [
                        {
                            "qty": "0.4",
                            "price": "101",
                            "volume": "40.4",
                            "feeUsdt": "0.00808",
                            "feeAsset": "USDT",
                            "isMaker": True,
                        }
                    ]
                return {"retCode": 0, "result": {"list": trades}}

        client = FillWhileCancellingClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.reduce_lots_complete = True
        engine._place("Sell", 101, 0, reduce_only=False, qty_override=1)
        engine.running = True

        await engine.stop()

        self.assertAlmostEqual(engine.grid_position_net_qty, -0.4)
        self.assertEqual(len(engine.filled_orders), 1)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(engine.paused_replacements, [])

    async def test_stop_accounts_partial_opening_fill_without_touching_baseline_position(self):
        class PartialOpeningFillOnCancelClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.partial_order_id = ""

            def cancel_order(self, symbol, order_id):
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                self.open_limit_order_ids.discard(str(order_id))
                order["orderStatus"] = "CANCELED"
                self.partial_order_id = str(order_id)
                self.positions = [{"side": "Sell", "size": "3.4", "avgPrice": "100.5"}]
                return {"retCode": 0}

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                if str(order_id) == self.partial_order_id:
                    response["result"].update(
                        {
                            "orderStatus": "CANCELED",
                            "executedQty": "0.4",
                            "cumQuote": "40.4",
                            "avgPrice": "101",
                        }
                    )
                return response

            def get_order_trades(self, symbol, order_id):
                trades = []
                if str(order_id) == self.partial_order_id:
                    trades = [
                        {
                            "qty": "0.4",
                            "price": "101",
                            "volume": "40.4",
                            "feeUsdt": "0.00808",
                            "feeAsset": "USDT",
                            "isMaker": True,
                        }
                    ]
                return {"retCode": 0, "result": {"list": trades}}

        client = PartialOpeningFillOnCancelClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        client.positions = [{"side": "Sell", "size": "3", "avgPrice": "100"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "leverage": 2,
                "initial_order_type": "limit",
                "initial_order_price": 101,
                "grid_order_post_only": False,
            },
        )

        await engine.initialize()
        engine.running = True
        await engine.stop()

        self.assertFalse(engine.running)
        self.assertIsNone(engine.opening_order)
        self.assertFalse(engine.waiting_initial_order)
        self.assertEqual(engine.baseline_position_side, "Sell")
        self.assertAlmostEqual(engine.baseline_position_qty, 3.0)
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.4)
        self.assertAlmostEqual(engine.initial_qty, 0.4)
        self.assertAlmostEqual(engine.initial_entry_price, 101.0)
        self.assertAlmostEqual(engine.total_volume, 40.4)
        self.assertEqual(client.positions[0]["size"], "3.4")
        self.assertEqual(
            [order for order in client.orders if order.get("order_type") == "Market"],
            [],
        )

    async def test_reduce_market_timeout_adopts_original_fill_without_duplicate(self):
        class ReduceTimeoutAfterAcceptClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reduce_market_calls = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Market" and kwargs.get("reduce_only"):
                    self.reduce_market_calls += 1
                    result = super().place_order(**kwargs)
                    if self.reduce_market_calls == 1:
                        raise TimeoutError("reduce acknowledgement lost after fill")
                    return result
                return super().place_order(**kwargs)

            def get_order_by_link(self, symbol, order_link_id):
                response = super().get_order_by_link(symbol, order_link_id)
                if response.get("result", {}).get("orderId"):
                    order = next(
                        item
                        for item in self.orders
                        if str(item.get("order_link_id", "")) == str(order_link_id)
                    )
                    response["result"].update(
                        {
                            "orderStatus": "FILLED",
                            "avgPrice": str(self.ticker_price),
                            "executedQty": str(order["qty"]),
                            "cumQuote": str(float(order["qty"]) * self.ticker_price),
                        }
                    )
                return response

            def get_order_trades(self, symbol, order_id):
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                qty = float(order["qty"])
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "qty": str(qty),
                                "price": str(self.ticker_price),
                                "volume": str(qty * self.ticker_price),
                                "feeUsdt": str(qty * self.ticker_price * 0.0005),
                                "feeAsset": "USDT",
                                "isMaker": False,
                            }
                        ]
                    },
                }

        client = ReduceTimeoutAfterAcceptClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
                "grid_order_post_only": False,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.grid_position_net_qty = -1.0

        engine._place_reduce_market("Buy", 1.0, "test timeout recovery")
        engine._resolve_pending_reduce_action()

        self.assertEqual(client.reduce_market_calls, 1)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(engine.grid_position_net_qty, 0.0)
        self.assertIsNone(engine.pending_reduce_action)

    async def test_fill_delta_never_creates_negative_fee_when_snapshot_regresses(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
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
        order = {
            "qty": "1",
            "price": "100",
            "processed_fill_qty": 0.5,
            "processed_fill_volume": 50.0,
            "processed_fill_fee": 0.05,
        }

        delta = engine._fill_delta_stats(
            order,
            {
                "price": 100.0,
                "qty": 0.8,
                "volume": 80.0,
                "fee": 0.04,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        self.assertIsNotNone(delta)
        self.assertEqual(delta["fee"], 0.0)

    async def test_risk_close_accounts_partial_market_fill_then_closes_remainder(self):
        class PartialReduceClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reduce_fill_qty_by_order = {}

            def place_order(self, **kwargs):
                if kwargs.get("order_type") != "Market" or not kwargs.get("reduce_only"):
                    return super().place_order(**kwargs)
                self.order_seq += 1
                order = dict(kwargs)
                order["orderId"] = str(self.order_seq)
                requested = float(kwargs["qty"])
                executed = min(requested, 0.4) if not self.reduce_fill_qty_by_order else requested
                order["orderStatus"] = "CANCELED" if executed < requested else "FILLED"
                order["executedQty"] = str(executed)
                order["cumQuote"] = str(executed * self.ticker_price)
                order["avgPrice"] = str(self.ticker_price)
                self.orders.append(order)
                self.reduce_fill_qty_by_order[order["orderId"]] = executed
                position = next(item for item in self.positions if item["side"] == "Sell")
                remaining = float(position["size"]) - executed
                if remaining > 0:
                    position["size"] = str(remaining)
                else:
                    self.positions.remove(position)
                return {"retCode": 0, "result": {"orderId": order["orderId"]}}

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                response["result"].update(
                    {
                        "orderLinkId": order.get("order_link_id", ""),
                        "orderStatus": order["orderStatus"],
                        "executedQty": order["executedQty"],
                        "cumQuote": order["cumQuote"],
                        "avgPrice": order["avgPrice"],
                    }
                )
                return response

            def get_order_trades(self, symbol, order_id):
                qty = self.reduce_fill_qty_by_order.get(str(order_id), 0)
                trades = []
                if qty:
                    trades = [
                        {
                            "qty": str(qty),
                            "price": str(self.ticker_price),
                            "volume": str(qty * self.ticker_price),
                            "feeUsdt": str(qty * self.ticker_price * 0.0005),
                            "feeAsset": "USDT",
                            "isMaker": False,
                        }
                    ]
                return {"retCode": 0, "result": {"list": trades}}

        client = PartialReduceClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.current_price = 100
        engine.grid_position_net_qty = -1.0

        self.assertFalse(engine._close_all_positions())
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.6)
        self.assertAlmostEqual(float(client.positions[0]["size"]), 0.6)

        self.assertTrue(engine._close_all_positions())
        self.assertEqual(engine.grid_position_net_qty, 0.0)
        self.assertEqual(client.positions, [])
        self.assertEqual(len(client.reduce_fill_qty_by_order), 2)

    async def test_risk_shutdown_cancels_only_managed_orders_and_preserves_manual_order(self):
        class FilledReduceClient(FakeClient):
            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                if order.get("order_type") == "Market":
                    qty = float(order["qty"])
                    response["result"].update(
                        {
                            "orderLinkId": order.get("order_link_id", ""),
                            "orderStatus": "FILLED",
                            "executedQty": str(qty),
                            "cumQuote": str(qty * self.ticker_price),
                            "avgPrice": str(self.ticker_price),
                        }
                    )
                return response

            def get_order_trades(self, symbol, order_id):
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                if order.get("order_type") != "Market":
                    return {"retCode": 0, "result": {"list": []}}
                qty = float(order["qty"])
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "qty": str(qty),
                                "price": str(self.ticker_price),
                                "volume": str(qty * self.ticker_price),
                                "feeUsdt": str(qty * self.ticker_price * 0.0005),
                                "feeAsset": "USDT",
                                "isMaker": False,
                            }
                        ]
                    },
                }

        client = FilledReduceClient("100", tick_size="1", qty_step="0.1", min_qty="0.1")
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.grid_levels = [90, 110]
        engine.grid_position_net_qty = -1.0
        engine._place("Sell", 110, 0, reduce_only=False, qty_override=1)
        manual = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="0.5",
            price="95",
            order_type="Limit",
            reduce_only=False,
            order_link_id="manual_order",
        )
        manual_id = str(manual["result"]["orderId"])
        engine.running = True
        engine.grid_ready = True

        completed = await engine._shutdown_with_close()

        self.assertTrue(completed)
        self.assertFalse(engine.running)
        self.assertIn(manual_id, client.open_limit_order_ids)
        self.assertEqual(engine.grid_position_net_qty, 0.0)

    async def test_pending_risk_shutdown_does_not_depend_on_ticker_availability(self):
        class TickerUnavailableCloseClient(FakeClient):
            def get_ticker(self, symbol):
                raise AssertionError("ticker must not gate an already-pending risk shutdown")

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
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
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
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

        client = TickerUnavailableCloseClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.current_price = 100
        engine.grid_position_net_qty = -1.0
        engine.risk_shutdown_pending = True
        engine.running = True

        await asyncio.wait_for(engine._run_loop(), timeout=1)

        self.assertFalse(engine.running)
        self.assertFalse(engine.risk_shutdown_pending)
        self.assertEqual(engine.grid_position_net_qty, 0.0)
        self.assertEqual(client.positions, [])

    async def test_partial_execution_snapshots_count_one_completed_pair_per_reduce_order(self):
        client = FakeClient("100")
        engine = GridEngine(
            client,
            {
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
        engine.grid_levels = [90, 110]
        engine.grid_position_net_qty = -1.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {"0": {"qty": 1.0, "entry_value": 100.0}}
        order = {
            "link_id": "g_0_B_partial",
            "order_id": "1",
            "level_idx": 0,
            "side": "Buy",
            "price": "99",
            "qty": "1",
            "reduce_only": True,
            "entry_price": 100,
            "order_type": "Limit",
            "time_in_force": "GTC",
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
        }

        engine._record_execution_delta(
            order,
            {
                "price": 99.0,
                "qty": 0.4,
                "volume": 39.6,
                "fee": 0.01,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
            place_counter=False,
        )
        engine._record_execution_delta(
            order,
            {
                "price": 99.0,
                "qty": 0.7,
                "volume": 69.3,
                "fee": 0.02,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 2,
                "taker_count": 0,
            },
            place_counter=False,
        )

        self.assertEqual(engine.completed_pairs, 1)
        self.assertEqual([round(item["qty"], 8) for item in engine.filled_orders], [0.4, 0.3])
        self.assertAlmostEqual(engine.total_volume, 69.3)
        self.assertAlmostEqual(engine.total_fee, 0.02)

    async def test_unknown_managed_order_is_retained_during_stop_reconciliation(self):
        client = FakeClient("100")
        engine = GridEngine(client, {"symbol": "TESTUSDT", "direction": "short"})
        engine._fetch_precision()
        engine.active_orders = {
            "g_0_S_unknown": {
                "link_id": "g_0_S_unknown",
                "order_id": "missing-order",
                "level_idx": 0,
                "side": "Sell",
                "price": "101",
                "qty": "1",
                "reduce_only": False,
                "order_type": "Limit",
                "time_in_force": "GTC",
            }
        }

        completed = engine._cancel_managed_orders_once()

        self.assertFalse(completed)
        self.assertIn("g_0_S_unknown", engine.active_orders)

    async def test_pending_reduce_action_survives_restart_and_reuses_original_client_id(self):
        class ReduceTimeoutAfterAcceptClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.reduce_market_calls = 0

            def place_order(self, **kwargs):
                if kwargs.get("order_type") == "Market" and kwargs.get("reduce_only"):
                    self.reduce_market_calls += 1
                    result = super().place_order(**kwargs)
                    if self.reduce_market_calls == 1:
                        raise TimeoutError("close response lost")
                    return result
                return super().place_order(**kwargs)

            def get_order_by_link(self, symbol, order_link_id):
                response = super().get_order_by_link(symbol, order_link_id)
                if response.get("result", {}).get("orderId"):
                    order = next(
                        item
                        for item in self.orders
                        if str(item.get("order_link_id", "")) == str(order_link_id)
                    )
                    response["result"].update(
                        {
                            "orderStatus": "FILLED",
                            "executedQty": str(order["qty"]),
                            "avgPrice": "100",
                            "cumQuote": str(float(order["qty"]) * 100),
                        }
                    )
                return response

            def get_order_trades(self, symbol, order_id):
                order = next(item for item in self.orders if str(item["orderId"]) == str(order_id))
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "qty": order["qty"],
                                "price": "100",
                                "volume": str(float(order["qty"]) * 100),
                                "feeUsdt": "0.05",
                                "feeAsset": "USDT",
                                "isMaker": False,
                            }
                        ]
                    },
                }

        client = ReduceTimeoutAfterAcceptClient("100")
        client.positions = [{"side": "Sell", "size": "1", "avgPrice": "101"}]
        config = {
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
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.current_price = 100
        engine.grid_position_net_qty = -1.0
        engine._place_reduce_market("Buy", 1.0, "restart test")
        original_link = engine.pending_reduce_action["link_id"]
        state = engine.to_state()

        restored = GridEngine(client, config)
        restored.restore_state(state)
        restored._resolve_pending_reduce_action()

        self.assertEqual(client.reduce_market_calls, 1)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["order_link_id"], original_link)
        self.assertIsNone(restored.pending_reduce_action)
        self.assertEqual(restored.grid_position_net_qty, 0.0)

    async def test_risk_shutdown_restore_does_not_place_counter_before_cancel_reconciliation(self):
        client = FakeClient("100")
        placed = client.place_order(
            symbol="TESTUSDT",
            side="Sell",
            qty="1",
            price="101",
            order_type="Limit",
            reduce_only=False,
            order_link_id="g_0_S_restore",
        )
        order_id = str(placed["result"]["orderId"])
        seed_order_count = len(client.orders)
        state = {
            "running": True,
            "config": {
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
            "grid_levels": [90, 110],
            "grid_ready": True,
            "risk_shutdown_pending": True,
            "active_orders": {
                "g_0_S_restore": {
                    "link_id": "g_0_S_restore",
                    "order_id": order_id,
                    "level_idx": 0,
                    "side": "Sell",
                    "price": "101",
                    "qty": "1",
                    "reduce_only": False,
                    "order_type": "Limit",
                    "time_in_force": "GTC",
                }
            },
            "reduce_lots_complete": True,
            "reduce_lots_by_level": {},
            "grid_position_net_qty": 0.0,
        }

        restored = GridEngine(client, state["config"])
        restored.restore_state(state)

        self.assertTrue(restored.risk_shutdown_pending)
        self.assertIn("g_0_S_restore", restored.active_orders)
        self.assertEqual(len(client.orders), seed_order_count)

    async def test_stop_cancels_order_found_by_single_query_when_open_list_omits_it(self):
        class OmittedOpenListClient(FakeClient):
            def get_open_orders(self, symbol):
                return {"retCode": 0, "result": {"list": []}}

        client = OmittedOpenListClient("100")
        engine = GridEngine(
            client,
            {
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
        link_id = engine._place("Sell", 110, 0, reduce_only=False, qty_override=1)
        order_id = engine.active_orders[link_id]["order_id"]
        engine.running = True

        await engine.stop()

        self.assertIn(order_id, client.cancelled_orders)
        self.assertEqual(engine.active_orders, {})

    async def test_unconfirmed_manual_stop_persists_and_restart_only_continues_cleanup(self):
        class RecoveringCancelClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.allow_cancel = False

            def cancel_order(self, symbol, order_id):
                if not self.allow_cancel:
                    raise TimeoutError("cancel response and terminal status unavailable")
                return super().cancel_order(symbol, order_id)

        client = RecoveringCancelClient("100")
        config = {
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
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        engine.grid_ready = True
        engine._place("Sell", 110, 0, reduce_only=False, qty_override=1)
        engine.running = True

        with self.assertRaises(RuntimeError):
            await engine.stop()

        state = engine.to_state()
        self.assertTrue(state["running"])
        self.assertTrue(state["manual_stop_pending"])
        self.assertEqual(len(state["active_orders"]), 1)

        restored = GridEngine(client, config)
        restored.restore_state(state)
        client.allow_cancel = True
        restored.start()
        await asyncio.sleep(0.1)

        self.assertFalse(restored.running)
        self.assertFalse(restored.manual_stop_pending)
        self.assertEqual(restored.active_orders, {})
        self.assertEqual(len(client.orders), 1)

    async def test_manual_stop_cleanup_finishes_while_restore_rules_are_unavailable(self):
        class RecoveringCancelClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.fail_rules = False

            def get_instrument_info(self, symbol):
                if self.fail_rules:
                    raise RuntimeError("exchangeInfo unavailable during stop recovery")
                return super().get_instrument_info(symbol)

        client = RecoveringCancelClient("100")
        config = {
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
        }
        source = GridEngine(client, config)
        source._fetch_precision()
        source.grid_levels = [90, 110]
        source._place("Sell", 110, 0, reduce_only=False, qty_override=1)
        source.grid_ready = False
        source.manual_stop_pending = True
        source.running = True
        state = source.to_state()

        client.fail_rules = True
        restored = GridEngine(client, config)
        restored.restore_state(state)

        self.assertTrue(restored.restore_refresh_pending)
        self.assertTrue(restored.manual_stop_pending)
        self.assertEqual(len(client.orders), 1)

        restored.start()
        await asyncio.wait_for(restored._task, timeout=1)

        durable = restored.to_state()
        self.assertFalse(restored.running)
        self.assertFalse(restored.manual_stop_pending)
        self.assertFalse(restored.restore_refresh_pending)
        self.assertFalse(durable["restore_saved_running"])
        self.assertEqual(restored.active_orders, {})
        self.assertEqual(len(client.orders), 1)

    async def test_exchange_unknown_status_result_preserves_single_order_write_ahead_record(self):
        class UnknownResultClient(FakeClient):
            def place_order(self, **kwargs):
                return {
                    "retCode": -1007,
                    "retMsg": "Timeout waiting for backend; execution status unknown",
                    "result": {},
                }

        client = UnknownResultClient("100")
        engine = GridEngine(
            client,
            {
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

        link_id = engine._place("Sell", 110, 0, reduce_only=False, qty_override=1)

        self.assertIsNotNone(link_id)
        self.assertIn(link_id, engine.active_orders)
        self.assertTrue(engine.active_orders[link_id]["submission_pending"])
        self.assertEqual(engine.active_orders[link_id]["status"], "SUBMIT_UNKNOWN")

    async def test_exchange_unknown_batch_items_do_not_fall_back_to_new_client_ids(self):
        class UnknownBatchClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.batch_calls = 0
                self.single_calls = 0

            def place_orders(self, orders):
                self.batch_calls += 1
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "retCode": -1007,
                                "retMsg": "TIMEOUT",
                                "result": {},
                            }
                            for _ in orders
                        ]
                    },
                }

            def place_order(self, **kwargs):
                self.single_calls += 1
                return super().place_order(**kwargs)

        client = UnknownBatchClient("100")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 1,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]

        links = engine._place_batch_limit_orders(
            [
                {
                    "side": "Sell",
                    "price": 100,
                    "level_idx": 0,
                    "reduce_only": False,
                    "qty_override": 1,
                },
                {
                    "side": "Sell",
                    "price": 110,
                    "level_idx": 1,
                    "reduce_only": False,
                    "qty_override": 1,
                },
            ]
        )

        self.assertEqual(len(links), 2)
        self.assertEqual(client.batch_calls, 1)
        self.assertEqual(client.single_calls, 0)
        self.assertEqual(len(engine.active_orders), 2)
        self.assertTrue(all(order["submission_pending"] for order in engine.active_orders.values()))

    async def test_definitively_rejected_pending_limit_becomes_durable_exact_replacement(self):
        class UnknownThenRejectedClient(FakeClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.first_submission = True
                self.accept_replacement = False

            def place_order(self, **kwargs):
                if self.first_submission:
                    self.first_submission = False
                    raise ConnectionError("response lost before exchange acceptance")
                if not self.accept_replacement:
                    return {"retCode": 400, "retMsg": "definitive retry rejection"}
                return super().place_order(**kwargs)

            def get_order_by_link(self, symbol, order_link_id):
                return {"retCode": 0, "result": {}}

            def get_order_history(self, symbol, limit=100):
                return {"retCode": 0, "result": {"list": []}}

        client = UnknownThenRejectedClient("100")
        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 2,
            "total_investment": 100,
            "leverage": 2,
            "qty_per_grid": 1,
            "grid_order_post_only": False,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [90, 100, 110]
        source_link_id = engine._place("Sell", 110, 1, reduce_only=False, qty_override=1)
        pending_order = engine.active_orders[source_link_id]
        pending_order["submission_updated_at"] = 0
        pending_order["submission_not_found_count"] = 99

        engine._resolve_pending_submissions([])

        self.assertNotIn(source_link_id, engine.active_orders)
        self.assertEqual(len(engine.paused_replacements), 1)
        queued = engine.paused_replacements[0]
        self.assertEqual(queued["replacement_mode"], "same_order")
        self.assertEqual(queued["replacement_source_link_id"], source_link_id)
        self.assertEqual((queued["side"], queued["price"], queued["qty"]), ("Sell", "110.0", "1.0"))
        self.assertNotIn("submission_pending", queued)

        client.accept_replacement = True
        queued["replacement_retry_after"] = 0
        replacement_link_id = queued["replacement_link_id"]
        engine._resume_paused_replacements()

        self.assertEqual(engine.paused_replacements, [])
        self.assertIn(replacement_link_id, engine.active_orders)
        self.assertEqual(engine.active_orders[replacement_link_id]["price"], "110.0")
        self.assertEqual(engine.active_orders[replacement_link_id]["qty"], "1.0")

    async def test_short_profit_and_unrealized_follow_exact_lot_cashflow(self):
        client = FakeClient("100", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
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
        engine.reduce_lots_complete = True

        engine._record_fill(
            {
                "level_idx": 0,
                "side": "Sell",
                "price": "101.3",
                "qty": "1",
                "order_id": "open",
                "reduce_only": False,
            },
            {
                "price": 101.3,
                "qty": 1.0,
                "volume": 101.3,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )
        engine._record_fill(
            {
                "level_idx": 0,
                "side": "Buy",
                "price": "100.2",
                "qty": "0.4",
                "order_id": "reduce",
                "reduce_only": True,
                # This stale order field must not override the fill lot.
                "entry_price": 999.0,
            },
            {
                "price": 100.2,
                "qty": 0.4,
                "volume": 40.08,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        mark = 99.8
        cashflow_total = Decimal("101.3") - Decimal("40.08") - Decimal("0.6") * Decimal(str(mark))
        engine_total = Decimal(str(engine.gross_profit)) + Decimal(
            str(engine.estimate_grid_unrealized_pnl(mark))
        )

        self.assertAlmostEqual(engine.gross_profit, 0.44)
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.6)
        self.assertEqual(engine_total, cashflow_total)

    async def test_long_profit_and_unrealized_follow_exact_lot_cashflow(self):
        client = FakeClient("100", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
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
        engine.reduce_lots_complete = True

        engine._record_fill(
            {
                "level_idx": 0,
                "side": "Buy",
                "price": "98.7",
                "qty": "1",
                "order_id": "open",
                "reduce_only": False,
            },
            {
                "price": 98.7,
                "qty": 1.0,
                "volume": 98.7,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )
        engine._record_fill(
            {
                "level_idx": 0,
                "side": "Sell",
                "price": "99.8",
                "qty": "0.4",
                "order_id": "reduce",
                "reduce_only": True,
                "entry_price": 1.0,
            },
            {
                "price": 99.8,
                "qty": 0.4,
                "volume": 39.92,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        mark = 100.2
        cashflow_total = -Decimal("98.7") + Decimal("39.92") + Decimal("0.6") * Decimal(str(mark))
        engine_total = Decimal(str(engine.gross_profit)) + Decimal(
            str(engine.estimate_grid_unrealized_pnl(mark))
        )

        self.assertAlmostEqual(engine.gross_profit, 0.44)
        self.assertAlmostEqual(engine.grid_position_net_qty, 0.6)
        self.assertEqual(engine_total, cashflow_total)

    async def test_market_reduce_uses_grid_lots_instead_of_blended_position_entry(self):
        client = FakeClient("100", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.grid_levels = [90, 100, 110]
        engine.grid_position_net_qty = -1.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 0.4, "entry_value": 40.4},
            "1": {"qty": 0.6, "entry_value": 61.8},
        }
        action = {
            "side": "Buy",
            "qty": "0.5",
            "price": "100",
            "entry_price": 999.0,
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
            "reason": "test",
            "tag": "market_reduce",
        }

        recorded = engine._record_market_reduce_execution(
            action,
            {
                "price": 100.0,
                "qty": 0.5,
                "volume": 50.0,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 0,
                "taker_count": 1,
            },
        )

        self.assertTrue(recorded)
        self.assertTrue(engine.reduce_lots_complete)
        self.assertAlmostEqual(engine.grid_position_net_qty, -0.5)
        self.assertAlmostEqual(engine.gross_profit, 0.7)
        self.assertEqual(
            engine.reduce_lots_by_level,
            {"1": {"qty": 0.5, "entry_value": 51.5}},
        )
        cashflow_total = (
            Decimal("0.4") * Decimal("101")
            + Decimal("0.6") * Decimal("103")
            - Decimal("0.5") * Decimal("100")
            - Decimal("0.5") * Decimal("99")
        )
        engine_total = Decimal(str(engine.gross_profit)) + Decimal(
            str(engine.estimate_grid_unrealized_pnl(99))
        )
        self.assertEqual(engine_total, cashflow_total)

    async def test_long_market_reduce_uses_grid_lots(self):
        client = FakeClient("100", tick_size="0.1", qty_step="0.1", min_qty="0.1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "long",
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
        engine.grid_levels = [90, 100, 110]
        engine.grid_position_net_qty = 1.0
        engine.reduce_lots_complete = True
        engine.reduce_lots_by_level = {
            "0": {"qty": 0.4, "entry_value": 39.6},
            "1": {"qty": 0.6, "entry_value": 58.2},
        }
        action = {
            "side": "Sell",
            "qty": "0.5",
            "price": "100",
            "entry_price": 1.0,
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
            "reason": "test",
            "tag": "market_reduce",
        }

        engine._record_market_reduce_execution(
            action,
            {
                "price": 100.0,
                "qty": 0.5,
                "volume": 50.0,
                "fee": 0.0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 0,
                "taker_count": 1,
            },
        )

        self.assertTrue(engine.reduce_lots_complete)
        self.assertAlmostEqual(engine.grid_position_net_qty, 0.5)
        self.assertAlmostEqual(engine.gross_profit, 0.7)
        self.assertEqual(
            engine.reduce_lots_by_level,
            {"1": {"qty": 0.5, "entry_value": 48.5}},
        )

    async def test_fill_waits_until_nonquote_fee_conversion_is_exact(self):
        class DelayedFeeConversionClient(FakeClient):
            fee_ready = False

            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                response["result"].update(
                    {
                        "orderStatus": "FILLED",
                        "executedQty": "1",
                        "cumQuote": "101",
                        "avgPrice": "101",
                    }
                )
                return response

            def get_order_trades(self, symbol, order_id):
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "price": "101",
                                "qty": "1",
                                "volume": "101",
                                "feeUsdt": "0.01818" if self.fee_ready else "",
                                "feeAsset": "BNB",
                                "feeUsdtSource": (
                                    "historical_minute_open"
                                    if self.fee_ready
                                    else "historical_price_unavailable"
                                ),
                                "isMaker": True,
                            }
                        ]
                    },
                }

        client = DelayedFeeConversionClient(
            "100", tick_size="1", qty_step="0.1", min_qty="0.1"
        )
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.grid_levels = [90, 110]
        engine.reduce_lots_complete = True
        link_id = engine._place("Sell", 101, 0, reduce_only=False, qty_override=1)
        client.open_limit_order_ids.clear()

        await engine._check_fills()

        self.assertIn(link_id, engine.active_orders)
        self.assertEqual(engine.grid_position_net_qty, 0.0)
        self.assertEqual(engine.total_fee, 0.0)
        self.assertIn("exact exchange fee conversion", engine.trigger_message)

        client.fee_ready = True
        await engine._check_fills()

        self.assertNotIn(link_id, engine.active_orders)
        self.assertAlmostEqual(engine.grid_position_net_qty, -1.0)
        self.assertAlmostEqual(engine.total_fee, 0.01818)
        self.assertNotIn("exact exchange fee conversion", engine.trigger_message)

    async def test_total_fill_count_survives_truncated_state_history(self):
        client = FakeClient("100")
        config = {
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
        }
        engine = GridEngine(client, config)
        engine.filled_orders = [
            {"side": "Sell", "qty": 1, "price": 100, "reduce_only": False}
            for _ in range(250)
        ]
        engine.filled_count = 250

        state = engine.to_state()

        self.assertEqual(len(state["filled_orders"]), 200)
        self.assertEqual(state["filled_count"], 250)

        restored = GridEngine(client, config)
        restored.restore_state({**state, "running": False})

        self.assertEqual(len(restored.filled_orders), 200)
        self.assertEqual(restored.filled_count, 250)
        self.assertEqual(restored.get_status()["filled_count"], 250)

    async def test_random_lot_sequences_match_independent_cashflow_every_step(self):
        import random

        for direction, seed in (("short", 73129), ("long", 91871)):
            rng = random.Random(seed)
            client = FakeClient(
                "100",
                tick_size="0.01",
                qty_step="0.1",
                min_qty="0.1",
            )
            engine = GridEngine(
                client,
                {
                    "symbol": "TESTUSDT",
                    "direction": direction,
                    "grid_mode": "arithmetic",
                    "upper_price": 110,
                    "lower_price": 90,
                    "grid_count": 5,
                    "total_investment": 0,
                    "position_sizing_mode": "fixed_grid_qty",
                    "grid_order_qty": 1,
                    "leverage": 2,
                },
            )
            engine._fetch_precision()
            engine.grid_levels = [90, 94, 98, 102, 106, 110]
            engine.reduce_lots_complete = True
            cashflow = Decimal("0")

            for index in range(400):
                lots = engine._reduce_lot_decimal_map()
                should_open = not lots or rng.random() < 0.56
                if should_open:
                    level_idx = rng.randrange(5)
                    qty = Decimal(rng.randint(1, 10)) * Decimal("0.1")
                    side = "Sell" if direction == "short" else "Buy"
                    reduce_only = False
                else:
                    level_idx = rng.choice(sorted(lots))
                    available_steps = int(lots[level_idx]["qty"] / Decimal("0.1"))
                    qty = Decimal(rng.randint(1, available_steps)) * Decimal("0.1")
                    side = "Buy" if direction == "short" else "Sell"
                    reduce_only = True

                price = (
                    Decimal("91")
                    + Decimal(level_idx * 4)
                    + Decimal(rng.randint(-75, 75)) * Decimal("0.01")
                )
                volume = price * qty
                engine._record_fill(
                    {
                        "level_idx": level_idx,
                        "side": side,
                        "price": str(price),
                        "qty": str(qty),
                        "order_id": f"{direction}-{index}",
                        "reduce_only": reduce_only,
                        "entry_price": 999.0 if direction == "short" else 1.0,
                    },
                    {
                        "price": float(price),
                        "qty": float(qty),
                        "volume": float(volume),
                        "fee": 0.0,
                        "fee_asset": "USDT",
                        "fee_source": "exchange",
                        "maker_count": 1,
                        "taker_count": 0,
                    },
                )
                cashflow += volume if side == "Sell" else -volume

                mark = Decimal("100") + Decimal(rng.randint(-500, 500)) * Decimal("0.01")
                position_value = Decimal(str(engine.grid_position_net_qty)) * mark
                independent_total = cashflow + position_value
                engine_total = Decimal(str(engine.gross_profit)) + Decimal(
                    str(engine.estimate_grid_unrealized_pnl(float(mark)))
                )
                self.assertAlmostEqual(
                    float(engine_total),
                    float(independent_total),
                    places=8,
                    msg=f"direction={direction} step={index}",
                )
                self.assertTrue(engine.reduce_lots_complete)

    async def test_partial_counter_waits_for_source_terminal_and_places_one_full_order(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 0.30,
                "lower_price": 0.26,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 20,
                "qty_per_grid": 20,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.26, 0.30]
        source = {
            "link_id": "g_0_B_source",
            "order_id": "source-1",
            "level_idx": 0,
            "side": "Buy",
            "price": "0.26",
            "qty": "20",
            "status": "PARTIALLY_FILLED",
            "order_type": "Limit",
            "time_in_force": "GTC",
            "reduce_only": False,
            "processed_fill_qty": 0,
            "processed_fill_volume": 0,
            "processed_fill_fee": 0,
        }
        engine.active_orders[source["link_id"]] = source

        first = engine._record_execution_delta(
            source,
            {
                "price": 0.26,
                "qty": 8,
                "volume": 2.08,
                "fee": 0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        self.assertTrue(first)
        self.assertEqual(client.orders, [])
        self.assertEqual(len(engine.paused_replacements), 1)

        second = engine._record_execution_delta(
            source,
            {
                "price": 0.26,
                "qty": 20,
                "volume": 5.2,
                "fee": 0,
                "fee_asset": "USDT",
                "fee_source": "exchange",
                "maker_count": 1,
                "taker_count": 0,
            },
        )

        self.assertTrue(second)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["side"], "Sell")
        self.assertEqual(client.orders[0]["price"], "0.30000")
        self.assertEqual(client.orders[0]["qty"], "20")
        self.assertEqual(engine.paused_replacements, [])

    async def test_sub_minimum_counter_tasks_coalesce_before_exchange_submission(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 0.30,
                "lower_price": 0.26,
                "grid_count": 1,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 20,
                "qty_per_grid": 20,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.26, 0.30]

        for index, qty in enumerate((8, 9), start=1):
            source = {
                "link_id": f"g_0_B_source_{index}",
                "order_id": f"source-{index}",
                "level_idx": 0,
                "side": "Buy",
                "price": "0.26",
                "qty": str(qty),
                "status": "FILLED",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "processed_fill_qty": 0,
                "processed_fill_volume": 0,
                "processed_fill_fee": 0,
            }
            engine._record_execution_delta(
                source,
                {
                    "price": 0.26,
                    "qty": qty,
                    "volume": 0.26 * qty,
                    "fee": 0,
                    "fee_asset": "USDT",
                    "fee_source": "exchange",
                    "maker_count": 1,
                    "taker_count": 0,
                },
            )

        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["side"], "Sell")
        self.assertEqual(client.orders[0]["qty"], "17")
        self.assertEqual(engine.paused_replacements, [])

    async def test_coalesced_counter_survives_restart_and_places_once(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        config = {
            "symbol": "ANSEMUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 0.30,
            "lower_price": 0.26,
            "grid_count": 1,
            "total_investment": 0,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 20,
            "qty_per_grid": 20,
            "leverage": 2,
        }
        engine = GridEngine(client, config)
        engine._fetch_precision()
        engine.grid_levels = [0.26, 0.30]
        engine._exchange_rate_limit_until = 10**12

        for index, qty in enumerate((8, 9), start=1):
            source = {
                "link_id": f"g_0_B_restart_source_{index}",
                "order_id": f"restart-source-{index}",
                "level_idx": 0,
                "side": "Buy",
                "price": "0.26",
                "qty": str(qty),
                "status": "FILLED",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "processed_fill_qty": 0,
                "processed_fill_volume": 0,
                "processed_fill_fee": 0,
            }
            engine._record_execution_delta(
                source,
                {
                    "price": 0.26,
                    "qty": qty,
                    "volume": 0.26 * qty,
                    "fee": 0,
                    "fee_asset": "USDT",
                    "fee_source": "exchange",
                    "maker_count": 1,
                    "taker_count": 0,
                },
            )

        self.assertEqual(client.orders, [])
        self.assertEqual(len(engine.paused_replacements), 1)
        self.assertEqual(engine.paused_replacements[0]["qty"], "17")
        self.assertEqual(
            set(engine.paused_replacements[0]["replacement_source_links"]),
            {"g_0_B_restart_source_1", "g_0_B_restart_source_2"},
        )
        durable_state = engine.to_state()

        restored = GridEngine(client, config)
        restored.restore_state({**durable_state, "running": False})
        restored._exchange_rate_limit_until = 0
        restored._resume_paused_replacements()

        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["side"], "Sell")
        self.assertEqual(client.orders[0]["price"], "0.30000")
        self.assertEqual(client.orders[0]["qty"], "17")
        self.assertEqual(restored.paused_replacements, [])

    async def test_accepted_coalesced_counter_is_adopted_after_persist_failure(self):
        client = FakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        config = {
            "symbol": "ANSEMUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 0.30,
            "lower_price": 0.26,
            "grid_count": 1,
            "total_investment": 0,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 20,
            "qty_per_grid": 20,
            "leverage": 2,
        }
        durable_state = {}

        def persist_until_exchange_accepts(current):
            if client.orders and any(
                order.get("order_id") for order in current.active_orders.values()
            ):
                raise OSError("simulated state write failure after exchange acceptance")
            durable_state.clear()
            durable_state.update(copy.deepcopy(current.to_state()))

        engine = GridEngine(client, config, state_callback=persist_until_exchange_accepts)
        engine._fetch_precision()
        engine.grid_levels = [0.26, 0.30]
        engine.paused_replacements = [
            {
                "link_id": "g_0_B_aggregate_source",
                "order_id": "aggregate-source",
                "level_idx": 0,
                "side": "Buy",
                "price": "0.26",
                "qty": "17",
                "fill_price": 0.26,
                "status": "FILLED",
                "order_type": "Limit",
                "time_in_force": "GTC",
                "reduce_only": False,
                "replacement_mode": "counter_order",
                "replacement_source_links": ["source-a", "source-b"],
            }
        ]

        with self.assertRaisesRegex(OSError, "after exchange acceptance"):
            engine._resume_paused_replacements()

        self.assertEqual(len(client.orders), 1)
        self.assertEqual(len(durable_state["paused_replacements"]), 1)
        pending = next(iter(durable_state["active_orders"].values()))
        self.assertTrue(pending["submission_pending"])
        self.assertEqual(pending["order_id"], "")

        restored = GridEngine(client, config)
        restored.restore_state({**durable_state, "running": False})
        restored._reconcile_exchange_open_orders()
        restored._resume_paused_replacements()

        self.assertEqual(len(client.orders), 1)
        self.assertEqual(restored.paused_replacements, [])
        adopted = next(iter(restored.active_orders.values()))
        self.assertFalse(adopted.get("submission_pending", False))
        self.assertEqual(adopted["order_id"], client.orders[0]["orderId"])

    async def test_batch_rejects_sub_minimum_nonreduce_order_before_exchange(self):
        client = BatchFakeClient(
            "0.28",
            tick_size="0.00001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "ANSEMUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 0.30,
                "lower_price": 0.26,
                "grid_count": 1,
                "total_investment": 0,
                "qty_per_grid": 10,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [0.26, 0.30]

        placed = engine._place_batch_limit_orders(
            [
                {
                    "side": "Sell",
                    "price": 0.30,
                    "level_idx": 0,
                    "reduce_only": False,
                    "qty_override": 10,
                }
            ]
        )

        self.assertEqual(placed, [])
        self.assertEqual(client.batch_calls, [])
        self.assertEqual(client.orders, [])
        self.assertEqual(engine.active_orders, {})
        self.assertIn("minimum notional", engine.trigger_message)

    async def test_run_loop_persists_read_rate_limit_without_fast_retry(self):
        client = FakeClient("100")
        persisted = []
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
            },
            state_callback=lambda current: persisted.append(current.to_state()),
        )
        calls = 0

        def rate_limited_price():
            nonlocal calls
            calls += 1
            raise ExchangeRateLimitError("Too many requests", retry_after=17)

        async def stop_after_backoff():
            engine.running = False

        engine._get_current_price = rate_limited_price
        engine._sleep_until_next_poll = stop_after_backoff
        engine.running = True

        await engine._run_loop()

        self.assertEqual(calls, 1)
        self.assertGreater(engine._rate_limit_remaining(), 16)
        self.assertIn("Exchange rate limit reached", engine.trigger_message)
        self.assertTrue(persisted)
        self.assertGreater(persisted[-1]["exchange_rate_limit_until"], 0)

    async def test_definitive_order_rejection_uses_persistent_shape_backoff(self):
        class RejectingClient(FakeClient):
            def __init__(self):
                super().__init__("100")
                self.place_calls = 0

            def place_order(self, **kwargs):
                self.place_calls += 1
                return {"retCode": 400, "retMsg": "ReduceOnly Order is rejected."}

        client = RejectingClient()
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
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
        engine.grid_levels = [90, 110]

        with patch("grid_engine.time.time", return_value=1000.0):
            self.assertIsNone(
                engine._place("Buy", 90, 0, reduce_only=True, qty_override=1)
            )
            self.assertIsNone(
                engine._place("Buy", 90, 0, reduce_only=True, qty_override=1)
            )
        self.assertEqual(client.place_calls, 1)

        durable = engine.to_state()
        restored = GridEngine(client, engine.config)
        with patch("grid_engine.time.time", return_value=1001.0):
            restored.restore_state({**durable, "running": False})
            self.assertIsNone(
                restored._place("Buy", 90, 0, reduce_only=True, qty_override=1)
            )
        self.assertEqual(client.place_calls, 1)

        with patch("grid_engine.time.time", return_value=1004.0):
            self.assertIsNone(
                restored._place("Buy", 90, 0, reduce_only=True, qty_override=1)
            )
        self.assertEqual(client.place_calls, 2)

    async def test_batch_rate_limit_never_falls_back_to_single_orders(self):
        class RateLimitedBatchClient(BatchFakeClient):
            def __init__(self):
                super().__init__("100")
                self.single_calls = 0

            def place_orders(self, orders):
                self.batch_calls.append([dict(order) for order in orders])
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "retCode": -1015,
                                "retMsg": "Too many new orders; current limit is 1200 orders per MINUTE.",
                                "result": {},
                            }
                            for _ in orders
                        ]
                    },
                }

            def place_order(self, **kwargs):
                self.single_calls += 1
                return super().place_order(**kwargs)

        client = RateLimitedBatchClient()
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 0,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]

        placed = engine._place_batch_limit_orders(
            [
                {
                    "side": "Buy",
                    "price": 90,
                    "level_idx": 0,
                    "reduce_only": False,
                    "qty_override": 1,
                }
            ]
        )

        self.assertEqual(placed, [])
        self.assertEqual(len(client.batch_calls), 1)
        self.assertEqual(client.single_calls, 0)
        self.assertGreater(engine._rate_limit_remaining(), 0)
        self.assertEqual(engine.active_orders, {})

    async def test_manual_stop_discovers_and_cancels_pending_submission_by_client_id(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        link_id = "g_0_B_pending_stop"
        accepted = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="1",
            price="90",
            order_type="Limit",
            reduce_only=False,
            order_link_id=link_id,
        )
        order_id = str(accepted["result"]["orderId"])
        engine.active_orders[link_id] = engine._pending_limit_order_state(
            link_id=link_id,
            level_idx=0,
            side="Buy",
            price="90",
            qty="1",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )
        engine.running = True
        engine.grid_ready = True

        await engine.stop()

        self.assertEqual(client.cancelled_orders, [order_id])
        self.assertEqual(engine.active_orders, {})
        self.assertFalse(engine.running)
        self.assertFalse(engine.manual_stop_pending)

    async def test_manual_stop_accounts_pending_fill_without_counter_order(self):
        class FilledPendingClient(FakeClient):
            def get_order(self, symbol, order_id):
                response = super().get_order(symbol, order_id)
                order = next(
                    item for item in self.orders if str(item["orderId"]) == str(order_id)
                )
                response["result"].update(
                    {
                        "orderLinkId": order.get("order_link_id", ""),
                        "orderStatus": "FILLED",
                        "executedQty": order["qty"],
                        "cumQuote": str(float(order["qty"]) * float(order["price"])),
                        "avgPrice": order["price"],
                    }
                )
                return response

            def get_order_trades(self, symbol, order_id):
                order = next(
                    item for item in self.orders if str(item["orderId"]) == str(order_id)
                )
                qty = float(order["qty"])
                price = float(order["price"])
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "qty": str(qty),
                                "price": str(price),
                                "volume": str(qty * price),
                                "feeUsdt": "0",
                                "feeAsset": "USDT",
                                "isMaker": True,
                            }
                        ]
                    },
                }

        client = FilledPendingClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        link_id = "g_0_B_pending_fill"
        accepted = client.place_order(
            symbol="TESTUSDT",
            side="Buy",
            qty="1",
            price="90",
            order_type="Limit",
            reduce_only=False,
            order_link_id=link_id,
        )
        order_id = str(accepted["result"]["orderId"])
        client.open_limit_order_ids.discard(order_id)
        next(item for item in client.orders if item["orderId"] == order_id)[
            "orderStatus"
        ] = "FILLED"
        engine.active_orders[link_id] = engine._pending_limit_order_state(
            link_id=link_id,
            level_idx=0,
            side="Buy",
            price="90",
            qty="1",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )
        engine.running = True
        engine.grid_ready = True

        await engine.stop()

        self.assertEqual(client.cancelled_orders, [])
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(engine.filled_count, 1)
        self.assertEqual(engine.grid_position_net_qty, 1.0)
        self.assertEqual(engine.active_orders, {})
        self.assertFalse(engine.running)

    async def test_manual_stop_retains_pending_submission_when_lookup_fails(self):
        class LookupFailureClient(FakeClient):
            def get_order_by_link(self, symbol, order_link_id):
                raise ConnectionError("exchange lookup unavailable")

        client = LookupFailureClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        engine._fetch_precision()
        engine.grid_levels = [90, 110]
        link_id = "g_0_B_lookup_failure"
        engine.active_orders[link_id] = engine._pending_limit_order_state(
            link_id=link_id,
            level_idx=0,
            side="Buy",
            price="90",
            qty="1",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )
        engine.running = True
        engine.grid_ready = True

        with patch("grid_engine.asyncio.sleep", new=unittest.mock.AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                await engine.stop()

        self.assertIn(link_id, engine.active_orders)
        self.assertTrue(engine.active_orders[link_id]["submission_pending"])
        self.assertTrue(engine.running)
        self.assertTrue(engine.manual_stop_pending)
        self.assertEqual(client.orders, [])

    async def test_pending_cancel_requires_repeated_authoritative_absence(self):
        client = FakeClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        order = engine._pending_limit_order_state(
            link_id="g_0_B_absent",
            level_idx=0,
            side="Buy",
            price="90",
            qty="1",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )

        outcomes = []
        for checked_at in (1.0, 1.5, 2.0, 2.5, 3.0):
            with patch("grid_engine.time.time", return_value=checked_at):
                outcomes.append(engine._resolve_pending_for_cancel(order, []))

        self.assertEqual(outcomes, ["pending", "pending", "pending", "pending", "absent"])
        self.assertEqual(order["submission_not_found_count"], 5)

    async def test_pending_cancel_can_confirm_from_history_only(self):
        class HistoryOnlyClient(FakeClient):
            def get_order_by_link(self, symbol, order_link_id):
                raise ConnectionError("direct lookup unavailable")

            def get_order_history(self, symbol, limit=100):
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "orderId": "77",
                                "orderLinkId": "g_0_B_history",
                                "side": "Buy",
                                "price": "90",
                                "qty": "1",
                                "orderStatus": "NEW",
                                "reduceOnly": False,
                            }
                        ]
                    },
                }

        client = HistoryOnlyClient("100", tick_size="1", qty_step="1", min_qty="1")
        engine = GridEngine(
            client,
            {
                "symbol": "TESTUSDT",
                "direction": "neutral",
                "grid_mode": "arithmetic",
                "upper_price": 110,
                "lower_price": 90,
                "grid_count": 1,
                "total_investment": 100,
                "qty_per_grid": 1,
                "leverage": 2,
            },
        )
        order = engine._pending_limit_order_state(
            link_id="g_0_B_history",
            level_idx=0,
            side="Buy",
            price="90",
            qty="1",
            reduce_only=False,
            entry_price=None,
            time_in_force="GTC",
        )

        outcome = engine._resolve_pending_for_cancel(order, [])

        self.assertEqual(outcome, "confirmed")
        self.assertEqual(order["order_id"], "77")
        self.assertFalse(order.get("submission_pending", False))

    async def test_initial_market_min_notional_uses_exchange_mark_price(self):
        class MarkPriceClient(FakeClient):
            def get_ticker(self, symbol):
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "lastPrice": "0.241",
                                "markPrice": "0.26",
                            }
                        ]
                    },
                }

        client = MarkPriceClient(
            "0.241",
            tick_size="0.001",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        engine = GridEngine(
            client,
            {
                "symbol": "EDGEUSDT",
                "direction": "short",
                "grid_mode": "arithmetic",
                "upper_price": 0.28,
                "lower_price": 0.24,
                "grid_count": 2,
                "total_investment": 0,
                "position_sizing_mode": "fixed_grid_qty",
                "grid_order_qty": 20,
                "leverage": 2,
                "initial_order_type": "market",
            },
        )

        await engine.initialize()

        market_orders = [
            order for order in client.orders if order.get("order_type") == "Market"
        ]
        self.assertEqual(engine.current_price, 0.241)
        self.assertEqual(engine.current_mark_price, 0.26)
        self.assertEqual(len(market_orders), 1)
        self.assertEqual(market_orders[0]["qty"], "20")

    async def test_restore_rule_refresh_failure_blocks_orders_until_rules_recover(self):
        class RuleRefreshClient(FakeClient):
            def __init__(self):
                super().__init__(
                    "100",
                    tick_size="1",
                    qty_step="1",
                    min_qty="1",
                    min_notional="5",
                )
                self.fail_rules = True
                self.rule_calls = 0
                self.open_order_reads = 0

            def get_instrument_info(self, symbol):
                self.rule_calls += 1
                if self.fail_rules:
                    raise RuntimeError("exchangeInfo temporarily unavailable")
                return super().get_instrument_info(symbol)

            def get_open_orders(self, symbol):
                self.open_order_reads += 1
                return super().get_open_orders(symbol)

        config = {
            "symbol": "TESTUSDT",
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
        }
        source_client = FakeClient(
            "100",
            tick_size="1",
            qty_step="1",
            min_qty="1",
            min_notional="5",
        )
        source = GridEngine(source_client, config)
        source._fetch_precision()
        source.grid_levels = [90, 110]
        source.target_qty_by_level = {"0": 1.0}
        source.grid_ready = True
        source.running = True
        legacy_state = source.to_state()
        for key in (
            "min_notional",
            "restore_refresh_pending",
            "restore_refresh_error",
            "restore_refresh_retry_after",
            "restore_refresh_attempts",
            "restore_saved_running",
            "restore_legacy_bootstrap_pending",
            "restore_previous_trigger_message",
        ):
            legacy_state.pop(key, None)

        client = RuleRefreshClient()
        restored = GridEngine(client, config)
        restored.restore_state(legacy_state)

        self.assertTrue(restored.restore_refresh_pending)
        self.assertIn("exchangeInfo temporarily unavailable", restored.restore_refresh_error)
        self.assertIn("normal grid placement is disabled", restored.trigger_message)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.open_order_reads, 0)

        async def stop_after_poll():
            restored.running = False

        restored._sleep_until_next_poll = stop_after_poll
        restored.restore_refresh_retry_after = 0
        restored.running = True
        await restored._run_loop()

        self.assertTrue(restored.restore_refresh_pending)
        self.assertEqual(client.orders, [])
        self.assertEqual(client.open_order_reads, 0)

        client.fail_rules = False
        restored.restore_refresh_retry_after = 0
        restored.running = True
        await restored._run_loop()

        self.assertFalse(restored.restore_refresh_pending)
        self.assertEqual(restored.restore_refresh_error, "")
        self.assertEqual(restored.min_notional, 5.0)
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["side"], "Sell")
        self.assertEqual(client.orders[0]["price"], "110")
        self.assertEqual(client.orders[0]["qty"], "1")

    async def test_restore_rule_rate_limit_stays_registered_for_retry(self):
        class RateLimitedRulesClient(FakeClient):
            def get_instrument_info(self, symbol):
                raise ExchangeRateLimitError(
                    "Too many requests while loading exchangeInfo",
                    retry_after=12,
                )

        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 1,
            "total_investment": 100,
            "qty_per_grid": 1,
            "leverage": 2,
        }
        source = GridEngine(FakeClient("100"), config)
        source.grid_levels = [90, 110]
        source.grid_ready = True
        source.running = True
        state = source.to_state()

        restored = GridEngine(RateLimitedRulesClient("100"), config)
        restored.restore_state(state)

        self.assertTrue(restored.restore_refresh_pending)
        self.assertTrue(restored._restore_saved_running)
        self.assertGreater(restored._rate_limit_remaining(), 11)
        self.assertGreater(restored.restore_refresh_retry_after, time.time() + 11)
        self.assertEqual(restored.active_orders, {})

    async def test_legacy_lot_bootstrap_intent_survives_failed_restore_refresh(self):
        class FailingRulesClient(FakeClient):
            def get_instrument_info(self, symbol):
                raise RuntimeError("exchangeInfo unavailable during migration")

        class TrackingEngine(GridEngine):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.bootstrap_calls = 0

            def _bootstrap_reduce_lots_from_legacy_state(self):
                self.bootstrap_calls += 1
                return super()._bootstrap_reduce_lots_from_legacy_state()

        config = {
            "symbol": "TESTUSDT",
            "direction": "neutral",
            "grid_mode": "arithmetic",
            "upper_price": 110,
            "lower_price": 90,
            "grid_count": 1,
            "total_investment": 100,
            "qty_per_grid": 1,
            "leverage": 2,
        }
        source = GridEngine(FakeClient("100"), config)
        source.grid_levels = [90, 110]
        source.grid_ready = True
        source.running = True
        legacy_state = source.to_state()
        legacy_state.pop("reduce_lots_complete", None)
        legacy_state.pop("reduce_lots_by_level", None)
        legacy_state.pop("restore_legacy_bootstrap_pending", None)

        failed = GridEngine(FailingRulesClient("100"), config)
        failed.restore_state(legacy_state)
        durable = failed.to_state()

        self.assertTrue(durable["restore_refresh_pending"])
        self.assertTrue(durable["restore_legacy_bootstrap_pending"])

        recovered = TrackingEngine(FakeClient("100"), config)
        recovered.restore_state(durable)

        self.assertEqual(recovered.bootstrap_calls, 1)
        self.assertFalse(recovered.restore_refresh_pending)
        self.assertFalse(recovered._restore_legacy_bootstrap_pending)


if __name__ == "__main__":
    asyncio.run(unittest.main())
