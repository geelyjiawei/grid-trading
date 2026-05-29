import asyncio
import sys
import unittest
from decimal import Decimal, ROUND_DOWN
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from grid_engine import GridEngine  # noqa: E402


class FakeClient:
    def __init__(self, ticker_price="100"):
        self.orders = []
        self.order_seq = 0
        self.ticker_price = float(ticker_price)
        self.open_limit_order_ids = set()
        self.positions = []
        self.reject_post_only_reduce = False
        self.reject_reduce_limit = False
        self.cancelled_orders = []

    def get_instrument_info(self, symbol):
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "priceFilter": {"tickSize": "0.1"},
                        "lotSizeFilter": {"qtyStep": "0.1", "minOrderQty": "0.1"},
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
        if kwargs.get("order_type") == "Limit":
            self.open_limit_order_ids.add(order["orderId"])
        return {"retCode": 0, "result": {"orderId": order["orderId"]}}

    def cancel_all_orders(self, symbol):
        self.open_limit_order_ids.clear()
        return {"retCode": 0}

    def cancel_order(self, symbol, order_id):
        self.cancelled_orders.append(str(order_id))
        self.open_limit_order_ids.discard(str(order_id))
        return {"retCode": 0}

    def get_open_orders(self, symbol):
        by_id = {str(order["orderId"]): order for order in self.orders}
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


class GridEngineTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_reconcile_repairs_missing_short_reduce_protection_from_exchange_position(self):
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
        self.assertEqual(len(repair_orders), 1)
        self.assertEqual(float(repair_orders[0]["qty"]), 528)

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

    async def test_post_only_initial_order_without_fill_stops_safely(self):
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
        client.open_limit_order_ids.clear()
        await engine._check_initial_order()

        self.assertFalse(engine.running)
        self.assertFalse(engine.grid_ready)
        self.assertIn("without fills", engine.trigger_message)

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

    async def test_reduce_order_market_repairs_when_limit_retry_is_rejected(self):
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
        engine._place_counter_order(filled_sell)

        repair_orders = [
            order
            for order in client.orders
            if order.get("side") == "Buy" and order.get("reduce_only") and order.get("order_type") == "Market"
        ]
        self.assertTrue(repair_orders)
        self.assertIn("Safety repair", engine.trigger_message)

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

    async def test_non_reduce_counter_order_is_queued_while_price_is_outside_grid_range(self):
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
        filled_buy = next(
            order for order in engine.active_orders.values() if order["side"] == "Buy" and order["reduce_only"]
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
        self.assertEqual(len(after_open_sells), len(before_open_sells))
        self.assertEqual(engine.get_status()["paused_replacements_count"], 1)

        engine.current_price = 100
        engine._resume_paused_replacements()
        resumed_open_sells = [
            order for order in client.orders if order.get("side") == "Sell" and not order.get("reduce_only")
        ]
        self.assertEqual(len(resumed_open_sells), len(before_open_sells) + 1)
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
                "boundary_market_repair": True,
            },
        )

        await engine.initialize()
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
        client.positions = [{"side": "Buy", "size": "3.0"}]
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
