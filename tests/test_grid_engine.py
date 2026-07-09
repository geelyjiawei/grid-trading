import asyncio
import logging
import random
import sys
import unittest
from decimal import Decimal, ROUND_DOWN
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from grid_engine import GridEngine  # noqa: E402


class FakeClient:
    def __init__(self, ticker_price="100", tick_size="0.1", qty_step="0.1", min_qty="0.1"):
        self.orders = []
        self.order_seq = 0
        self.ticker_price = float(ticker_price)
        self.tick_size = str(tick_size)
        self.qty_step = str(qty_step)
        self.min_qty = str(min_qty)
        self.open_limit_order_ids = set()
        self.positions = []
        self.reject_post_only_reduce = False
        self.reject_reduce_limit = False
        self.cancelled_orders = []
        self.instant_fill_reduce_limits = False

    def get_instrument_info(self, symbol):
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "priceFilter": {"tickSize": self.tick_size},
                        "lotSizeFilter": {"qtyStep": self.qty_step, "minOrderQty": self.min_qty},
                    }
                ]
            },
        }

    def set_leverage(self, symbol, leverage):
        return {"retCode": 0}

    def get_ticker(self, symbol):
        return {"retCode": 0, "result": {"list": [{"lastPrice": str(self.ticker_price)}]}}

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
        engine.active_orders = {
            "wrong": {
                "link_id": "wrong",
                "order_id": "old-1",
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
        self.assertIn("old-1", client.cancelled_orders)

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
        self.assertEqual(status["grid_position_net_qty"], -528)
        repair_orders = [
            order
            for order in engine.active_orders.values()
            if order["side"] == "Buy" and order["reduce_only"]
        ]
        self.assertEqual(repair_orders, [])

    async def test_position_sync_clears_stale_reduce_lots(self):
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

        self.assertEqual(engine.grid_position_net_qty, -3)
        self.assertFalse(engine.reduce_lots_complete)
        self.assertEqual(engine.reduce_lots_by_level, {})

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

        engine._reconcile_grid_position_protection()

        self.assertEqual(engine.trigger_message, "")

    async def test_reconcile_clears_grid_position_when_exchange_is_flat(self):
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

        self.assertEqual(engine.get_status()["grid_position_net_qty"], 0)

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
            return {"retCode": 0, "result": {"list": client.trade_details.get(order_id, [])}}

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
            return {"retCode": 0, "result": {"list": client.trade_details.get(order_id, [])}}

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

    async def test_post_only_initial_order_filled_status_deploys_when_trades_lag(self):
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
        client.get_order_trades = lambda symbol, order_id: {"retCode": 0, "result": {"list": []}}

        await engine.initialize()
        engine.running = True
        opening_order = engine.opening_order
        client.open_limit_order_ids.discard(opening_order["order_id"])

        await engine._check_initial_order()

        self.assertTrue(engine.running)
        self.assertFalse(engine.waiting_initial_order)
        self.assertTrue(engine.grid_ready)
        self.assertEqual(engine.opening_order, None)
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
            return {"retCode": 0, "result": {"list": client.trade_details.get(order_id, [])}}

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
            return {"retCode": 0, "result": {"list": client.trade_details.get(order_id, [])}}

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
        client.get_order_trades = lambda symbol, order_id: {"retCode": 0, "result": {"list": []}}
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

    async def test_restored_grid_continues_tracking_saved_orders_after_restart(self):
        snapshots = []
        client = FakeClient("100")
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

    async def test_reduce_order_limit_rejection_does_not_market_close(self):
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

        await engine.initialize()
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


if __name__ == "__main__":
    asyncio.run(unittest.main())
