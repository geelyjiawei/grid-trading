import asyncio
import logging
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Optional


logger = logging.getLogger(__name__)


class GridEngine:
    def __init__(self, client, config: dict):
        self.client = client
        self.config = config
        self.running = False
        self.grid_levels: list[float] = []
        self.active_orders: dict[str, dict] = {}
        self.filled_orders: list[dict] = []
        self.completed_pairs = 0
        self.total_profit = 0.0
        self.start_time: float | None = None
        self.tick_size = "0.1"
        self.qty_step = "0.001"
        self.min_qty = 0.001
        self.current_price = 0.0
        self.initial_side = ""
        self.initial_qty = 0.0
        self.grid_profit_pct = 0.0
        self.waiting_trigger = False
        self.trigger_message = ""
        self.grid_ready = False
        self._stopping = False
        self._task: Optional[asyncio.Task] = None

    async def initialize(self):
        symbol = self.config["symbol"]
        leverage = str(self.config["leverage"])

        self._fetch_precision()
        leverage_resp = self.client.set_leverage(symbol, leverage)
        if leverage_resp.get("retCode") not in (0, 110043):
            raise RuntimeError(leverage_resp.get("retMsg", "Failed to set leverage"))

        self.grid_levels = self._calculate_levels()
        self.current_price = self._get_current_price()
        self.grid_profit_pct = self._calculate_grid_profit_pct(self.current_price)

        trigger_price = self.config.get("trigger_price")
        if trigger_price is not None and not self._is_trigger_hit(self.current_price):
            self.waiting_trigger = True
            self.trigger_message = f"Waiting for trigger price {trigger_price}"
            return

        self._deploy_initial_grid(self.current_price)

    def start(self):
        if self.running:
            return
        self._stopping = False
        self.running = True
        self.start_time = time.time()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._stopping = True
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        try:
            self.client.cancel_all_orders(self.config["symbol"])
        finally:
            self.active_orders.clear()

    def get_status(self) -> dict:
        return {
            "running": self.running,
            "grid_ready": self.grid_ready,
            "waiting_trigger": self.waiting_trigger,
            "trigger_message": self.trigger_message,
            "symbol": self.config.get("symbol", ""),
            "direction": self.config.get("direction", ""),
            "grid_mode": self.config.get("grid_mode", "arithmetic"),
            "grid_levels": self.grid_levels,
            "active_orders": list(self.active_orders.values()),
            "completed_pairs": self.completed_pairs,
            "filled_count": len(self.filled_orders),
            "filled_orders": self.filled_orders[-50:],
            "total_profit": round(self.total_profit, 4),
            "start_time": self.start_time,
            "current_price": self.current_price,
            "initial_side": self.initial_side,
            "initial_qty": round(self.initial_qty, 8),
            "grid_profit_pct": round(self.grid_profit_pct, 6),
            "config": self.config,
        }

    def _fetch_precision(self):
        resp = self.client.get_instrument_info(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch instrument info"))

        info = resp["result"]["list"][0]
        self.tick_size = info["priceFilter"]["tickSize"]
        self.qty_step = info["lotSizeFilter"]["qtyStep"]
        self.min_qty = float(info["lotSizeFilter"]["minOrderQty"])

    def _get_current_price(self) -> float:
        ticker = self.client.get_ticker(self.config["symbol"])
        if ticker.get("retCode") != 0:
            raise RuntimeError(ticker.get("retMsg", "Failed to fetch current price"))
        return float(ticker["result"]["list"][0]["lastPrice"])

    def _calculate_levels(self) -> list[float]:
        lower = float(self.config["lower_price"])
        upper = float(self.config["upper_price"])
        count = int(self.config["grid_count"])
        grid_mode = self.config.get("grid_mode", "arithmetic")

        if grid_mode == "geometric":
            ratio = (upper / lower) ** (1 / count)
            return [round(lower * (ratio ** idx), 10) for idx in range(count + 1)]

        step = (upper - lower) / count
        return [round(lower + (step * idx), 10) for idx in range(count + 1)]

    def _calculate_grid_profit_pct(self, reference_price: float) -> float:
        if reference_price <= 0 or len(self.grid_levels) < 2:
            return 0.0

        if self.config.get("grid_mode", "arithmetic") == "geometric":
            return ((self.grid_levels[1] / self.grid_levels[0]) - 1) * 100

        step = self.grid_levels[1] - self.grid_levels[0]
        return (step / reference_price) * 100

    def _calc_total_qty(self, reference_price: float) -> float:
        total_investment = float(self.config["total_investment"])
        leverage = float(self.config["leverage"])
        return (total_investment * leverage) / reference_price

    def _qty_to_steps(self, qty: float) -> int:
        qty_decimal = Decimal(str(qty))
        step_decimal = Decimal(self.qty_step)
        return int((qty_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN))

    def _steps_to_qty(self, steps: int) -> float:
        return float(Decimal(self.qty_step) * Decimal(steps))

    def _fp(self, value: float) -> str:
        return self.client.round_to_step(value, self.tick_size)

    def _fq(self, value: float) -> str:
        normalized = max(value, self.min_qty)
        return self.client.round_to_step(normalized, self.qty_step)

    def _has_active_order(self, side: str, level_idx: int, reduce_only: bool) -> bool:
        for order in self.active_orders.values():
            if (
                order["side"] == side
                and order["level_idx"] == level_idx
                and order["reduce_only"] == reduce_only
            ):
                return True
        return False

    def _place_market_open(self, side: str, qty: float):
        qty_text = self._fq(qty)
        result = self.client.place_order(
            symbol=self.config["symbol"],
            side=side,
            qty=qty_text,
            order_type="Market",
            reduce_only=False,
            order_link_id=f"init_{side[0]}_{uuid.uuid4().hex[:6]}",
        )
        if result.get("retCode") != 0:
            raise RuntimeError(result.get("retMsg", "Failed to place initial market order"))

        self.initial_side = side
        self.initial_qty = float(qty_text)

    def _place(
        self,
        side: str,
        price: float,
        level_idx: int,
        reduce_only: bool,
        qty_override: float | None = None,
    ) -> Optional[str]:
        raw_qty = float(qty_override) if qty_override is not None else float(self.config["qty_per_grid"])
        qty = self._fq(raw_qty)
        price_text = self._fp(price)
        link_id = f"g_{level_idx}_{side[0]}_{uuid.uuid4().hex[:6]}"

        if self._has_active_order(side, level_idx, reduce_only):
            return None
        if self._stopping:
            return None

        result = self.client.place_order(
            symbol=self.config["symbol"],
            side=side,
            qty=qty,
            price=price_text,
            order_type="Limit",
            reduce_only=reduce_only,
            order_link_id=link_id,
        )

        if result.get("retCode") != 0:
            logger.warning(
                "Place order failed side=%s price=%s reduce_only=%s msg=%s",
                side,
                price_text,
                reduce_only,
                result.get("retMsg"),
            )
            return None

        self.active_orders[link_id] = {
            "link_id": link_id,
            "order_id": result["result"]["orderId"],
            "level_idx": level_idx,
            "side": side,
            "price": price_text,
            "qty": qty,
            "status": "open",
            "reduce_only": reduce_only,
        }
        return link_id

    def _is_trigger_hit(self, current_price: float) -> bool:
        trigger_price = self.config.get("trigger_price")
        if trigger_price is None:
            return True

        direction = self.config["direction"]
        trigger_price = float(trigger_price)
        if direction == "long":
            return current_price <= trigger_price
        if direction == "short":
            return current_price >= trigger_price
        return self.config["lower_price"] <= current_price <= self.config["upper_price"]

    def _deploy_initial_grid(self, current_price: float):
        levels = self.grid_levels
        direction = self.config["direction"]

        if not (levels[0] < current_price < levels[-1]):
            raise RuntimeError("Current price must stay inside the configured range")

        self.waiting_trigger = False
        self.trigger_message = ""

        if direction == "long":
            open_side = "Buy"
            profit_targets = [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > current_price
            ]
            add_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < current_price
            ]
            self._place_market_open(open_side, self._calc_total_qty(current_price))
        elif direction == "short":
            open_side = "Sell"
            profit_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < current_price
            ]
            add_targets = [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > current_price
            ]
            self._place_market_open(open_side, self._calc_total_qty(current_price))
        else:
            profit_targets = []
            add_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < current_price
            ] + [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > current_price
            ]

        target_count = len(profit_targets) if direction in {"long", "short"} else len(add_targets)
        if target_count <= 0:
            raise RuntimeError("No valid grid targets were found around current price")

        raw_total_qty = self._calc_total_qty(current_price)
        total_steps = self._qty_to_steps(raw_total_qty)
        if total_steps < target_count:
            raise RuntimeError("Total investment is too small for this symbol and grid count")

        base_steps = total_steps // target_count
        remainder_steps = total_steps % target_count
        per_grid_steps = [
            base_steps + (1 if index < remainder_steps else 0)
            for index in range(target_count)
        ]
        total_qty = self._steps_to_qty(total_steps)
        allocated_qtys = [self._steps_to_qty(steps) for steps in per_grid_steps]
        qty_per_grid = total_qty / target_count

        self.config["active_grid_count"] = target_count
        self.config["derived_total_qty"] = total_qty
        self.config["qty_per_grid"] = qty_per_grid

        if direction in {"long", "short"}:
            for target, allocated_qty in zip(profit_targets, allocated_qtys):
                idx, target_price, target_side = target
                self._place(target_side, target_price, idx, reduce_only=True, qty_override=allocated_qty)

            for idx, target_price, target_side in add_targets:
                self._place(target_side, target_price, idx, reduce_only=False, qty_override=qty_per_grid)
        else:
            for target, allocated_qty in zip(add_targets, allocated_qtys):
                idx, target_price, target_side = target
                self._place(target_side, target_price, idx, reduce_only=False, qty_override=allocated_qty)

        self.grid_ready = True

    def _risk_hit(self, current_price: float) -> bool:
        direction = self.config["direction"]
        stop_loss = self.config.get("stop_loss_price")
        take_profit = self.config.get("take_profit_price")

        if direction == "long":
            if stop_loss is not None and current_price <= float(stop_loss):
                self.trigger_message = f"Stop loss hit at {current_price}"
                return True
            if take_profit is not None and current_price >= float(take_profit):
                self.trigger_message = f"Take profit hit at {current_price}"
                return True
        elif direction == "short":
            if stop_loss is not None and current_price >= float(stop_loss):
                self.trigger_message = f"Stop loss hit at {current_price}"
                return True
            if take_profit is not None and current_price <= float(take_profit):
                self.trigger_message = f"Take profit hit at {current_price}"
                return True
        else:
            if stop_loss is not None and current_price <= float(stop_loss):
                self.trigger_message = f"Stop loss hit at {current_price}"
                return True
            if take_profit is not None and current_price >= float(take_profit):
                self.trigger_message = f"Take profit hit at {current_price}"
                return True

        return False

    def _close_all_positions(self):
        resp = self.client.get_positions(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch positions"))

        for position in resp["result"].get("list", []):
            size = float(position.get("size", 0))
            if size <= 0:
                continue

            close_side = "Sell" if position.get("side") == "Buy" else "Buy"
            result = self.client.place_order(
                symbol=self.config["symbol"],
                side=close_side,
                qty=self._fq(size),
                order_type="Market",
                reduce_only=True,
                order_link_id=f"close_{close_side[0]}_{uuid.uuid4().hex[:6]}",
            )
            if result.get("retCode") != 0:
                raise RuntimeError(result.get("retMsg", "Failed to close position"))

    async def _shutdown_with_close(self):
        self._stopping = True
        self.running = False
        try:
            self.client.cancel_all_orders(self.config["symbol"])
            self.active_orders.clear()
            self._close_all_positions()
        except Exception as exc:
            logger.exception("Risk shutdown failed: %s", exc)

    async def _run_loop(self):
        while self.running:
            try:
                self.current_price = self._get_current_price()

                if self.waiting_trigger and self._is_trigger_hit(self.current_price):
                    self._deploy_initial_grid(self.current_price)

                if self.grid_ready and self._risk_hit(self.current_price):
                    await self._shutdown_with_close()
                    break

                if self.grid_ready:
                    await self._check_fills()

                await asyncio.sleep(3)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Grid polling failed: %s", exc)
                await asyncio.sleep(5)

    async def _check_fills(self):
        resp = self.client.get_open_orders(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch open orders"))

        open_order_ids = {item["orderId"] for item in resp["result"].get("list", [])}
        if self._stopping:
            return

        filled_links = [
            link_id
            for link_id, order in list(self.active_orders.items())
            if order["order_id"] not in open_order_ids
        ]

        for link_id in filled_links:
            if self._stopping:
                break
            order = self.active_orders.pop(link_id)
            self._record_fill(order)
            self._place_counter_order(order)

    def _record_fill(self, order: dict):
        level_idx = order["level_idx"]
        qty = float(order["qty"])
        profit = 0.0

        if order["reduce_only"] and level_idx + 1 < len(self.grid_levels):
            profit = (self.grid_levels[level_idx + 1] - self.grid_levels[level_idx]) * qty
            self.total_profit += profit
            self.completed_pairs += 1

        self.filled_orders.append(
            {
                "side": order["side"],
                "price": float(order["price"]),
                "qty": qty,
                "level_idx": level_idx,
                "profit": round(profit, 4),
                "time": time.time(),
                "reduce_only": order["reduce_only"],
            }
        )

    def _place_counter_order(self, order: dict):
        direction = self.config["direction"]
        side = order["side"]
        level_idx = order["level_idx"]
        qty = float(order["qty"])

        if direction == "long":
            if side == "Buy" and level_idx + 1 < len(self.grid_levels):
                self._place("Sell", self.grid_levels[level_idx + 1], level_idx, reduce_only=True, qty_override=qty)
            elif side == "Sell":
                self._place("Buy", self.grid_levels[level_idx], level_idx, reduce_only=False, qty_override=qty)
        elif direction == "short":
            if side == "Sell":
                self._place("Buy", self.grid_levels[level_idx], level_idx, reduce_only=True, qty_override=qty)
            elif level_idx + 1 < len(self.grid_levels):
                self._place("Sell", self.grid_levels[level_idx + 1], level_idx, reduce_only=False, qty_override=qty)
        else:
            if side == "Buy" and level_idx + 1 < len(self.grid_levels):
                self._place("Sell", self.grid_levels[level_idx + 1], level_idx, reduce_only=False, qty_override=qty)
            elif side == "Sell":
                self._place("Buy", self.grid_levels[level_idx], level_idx, reduce_only=False, qty_override=qty)
