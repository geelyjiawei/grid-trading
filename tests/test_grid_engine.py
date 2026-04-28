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

    def get_open_orders(self, symbol):
        return {
            "retCode": 0,
            "result": {"list": [{"orderId": oid} for oid in sorted(self.open_limit_order_ids)]},
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

        market_qty = sum(float(o["qty"]) for o in market_orders)
        reduce_qty = sum(float(o["qty"]) for o in buy_reduce_orders)
        self.assertAlmostEqual(market_qty, reduce_qty)

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
