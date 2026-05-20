import asyncio
import logging
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable, Optional


logger = logging.getLogger(__name__)


class GridEngine:
    def __init__(self, client, config: dict, state_callback: Callable[["GridEngine"], None] | None = None):
        self.client = client
        self.config = config
        self.state_callback = state_callback
        self.running = False
        self.grid_levels: list[float] = []
        self.active_orders: dict[str, dict] = {}
        self.filled_orders: list[dict] = []
        self.completed_pairs = 0
        self.gross_profit = 0.0
        self.total_profit = 0.0
        self.total_fee = 0.0
        self.total_volume = 0.0
        self.start_time: float | None = None
        self.tick_size = "0.1"
        self.qty_step = "0.001"
        self.min_qty = 0.001
        self.current_price = 0.0
        self.initial_side = ""
        self.initial_qty = 0.0
        self.initial_entry_price = 0.0
        self.grid_profit_pct = 0.0
        self.waiting_trigger = False
        self.trigger_message = ""
        self.grid_ready = False
        self.waiting_initial_order = False
        self.opening_order: dict | None = None
        self._pending_targets: dict | None = None
        self.paused_replacements: list[dict] = []
        self._stopping = False
        self._boundary_repair_in_progress = False
        self._boundary_repair_retry_after = 0.0
        self._task: Optional[asyncio.Task] = None

    def _persist_state(self):
        if self.state_callback:
            self.state_callback(self)

    def to_state(self) -> dict[str, Any]:
        return {
            "version": 1,
            "config": self.config,
            "running": self.running,
            "grid_levels": self.grid_levels,
            "active_orders": self.active_orders,
            "filled_orders": self.filled_orders[-200:],
            "completed_pairs": self.completed_pairs,
            "gross_profit": self.gross_profit,
            "total_profit": self.total_profit,
            "total_fee": self.total_fee,
            "total_volume": self.total_volume,
            "start_time": self.start_time,
            "tick_size": self.tick_size,
            "qty_step": self.qty_step,
            "min_qty": self.min_qty,
            "current_price": self.current_price,
            "initial_side": self.initial_side,
            "initial_qty": self.initial_qty,
            "initial_entry_price": self.initial_entry_price,
            "grid_profit_pct": self.grid_profit_pct,
            "waiting_trigger": self.waiting_trigger,
            "trigger_message": self.trigger_message,
            "grid_ready": self.grid_ready,
            "waiting_initial_order": self.waiting_initial_order,
            "opening_order": self.opening_order,
            "pending_targets": self._pending_targets,
            "paused_replacements": self.paused_replacements[-200:],
            "saved_at": time.time(),
        }

    def restore_state(self, state: dict[str, Any]):
        self.config = dict(state.get("config") or self.config)
        self.grid_levels = list(state.get("grid_levels") or [])
        self.active_orders = dict(state.get("active_orders") or {})
        self.filled_orders = list(state.get("filled_orders") or [])
        self.completed_pairs = int(state.get("completed_pairs") or 0)
        self.gross_profit = float(state.get("gross_profit") or 0)
        self.total_profit = float(state.get("total_profit") or 0)
        self.total_fee = float(state.get("total_fee") or 0)
        self.total_volume = float(state.get("total_volume") or 0)
        self.start_time = state.get("start_time")
        self.tick_size = str(state.get("tick_size") or self.tick_size)
        self.qty_step = str(state.get("qty_step") or self.qty_step)
        self.min_qty = float(state.get("min_qty") or self.min_qty)
        self.current_price = float(state.get("current_price") or 0)
        self.initial_side = str(state.get("initial_side") or "")
        self.initial_qty = float(state.get("initial_qty") or 0)
        self.initial_entry_price = float(state.get("initial_entry_price") or 0)
        self.grid_profit_pct = float(state.get("grid_profit_pct") or 0)
        self.waiting_trigger = bool(state.get("waiting_trigger", False))
        self.trigger_message = str(state.get("trigger_message") or "")
        self.grid_ready = bool(state.get("grid_ready", False))
        self.waiting_initial_order = bool(state.get("waiting_initial_order", False))
        self.opening_order = state.get("opening_order")
        self._pending_targets = state.get("pending_targets")
        self.paused_replacements = list(state.get("paused_replacements") or [])

        if not self.grid_levels:
            self.grid_levels = self._calculate_levels()
        try:
            self._fetch_precision()
            self.current_price = self._get_current_price()
        except Exception as exc:
            logger.warning("Restore refresh failed symbol=%s msg=%s", self.config.get("symbol"), exc)
        self._persist_state()

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
        if self.start_time is None:
            self.start_time = time.time()
        self._task = asyncio.create_task(self._run_loop())
        self._persist_state()

    async def stop(self):
        self._stopping = True
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        resp = self.client.cancel_all_orders(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to cancel open orders"))

        self.active_orders.clear()
        self.opening_order = None
        self.waiting_initial_order = False
        self.grid_ready = False
        self._persist_state()

    async def suspend(self):
        """Stop the local polling task without touching exchange orders."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._persist_state()

    def get_status(self) -> dict:
        return {
            "running": self.running,
            "grid_ready": self.grid_ready,
            "waiting_trigger": self.waiting_trigger,
            "waiting_initial_order": self.waiting_initial_order,
            "trigger_message": self.trigger_message,
            "symbol": self.config.get("symbol", ""),
            "direction": self.config.get("direction", ""),
            "grid_mode": self.config.get("grid_mode", "arithmetic"),
            "grid_levels": self.grid_levels,
            "active_orders": list(self.active_orders.values()),
            "paused_replacements": self.paused_replacements,
            "paused_replacements_count": len(self.paused_replacements),
            "completed_pairs": self.completed_pairs,
            "filled_count": len(self.filled_orders),
            "filled_orders": self.filled_orders[-50:],
            "gross_profit": round(self.gross_profit, 4),
            "total_profit": round(self.total_profit, 4),
            "realized_net_profit": round(self.total_profit, 4),
            "total_fee": round(self.total_fee, 4),
            "total_volume": round(self.total_volume, 4),
            "fee_rate": self._fee_rate(),
            "maker_fee_rate": self._maker_fee_rate(),
            "taker_fee_rate": self._taker_fee_rate(),
            "start_time": self.start_time,
            "current_price": self.current_price,
            "initial_side": self.initial_side,
            "initial_qty": round(self.initial_qty, 8),
            "initial_entry_price": round(self.initial_entry_price, 10),
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

    def _in_grid_range(self, price: float | None = None) -> bool:
        current = self.current_price if price is None else float(price)
        return float(self.config["lower_price"]) <= current <= float(self.config["upper_price"])

    def _calc_total_qty(self, reference_price: float) -> float:
        total_investment = float(self.config["total_investment"])
        leverage = float(self.config["leverage"])
        return (total_investment * leverage) / reference_price

    def _fee_rate(self) -> float:
        return max(0.0, float(self.config.get("fee_rate", 0.0005) or 0))

    def _maker_fee_rate(self) -> float:
        if self.config.get("maker_fee_rate") is not None:
            return max(0.0, float(self.config.get("maker_fee_rate") or 0))
        return self._fee_rate()

    def _taker_fee_rate(self) -> float:
        if self.config.get("taker_fee_rate") is not None:
            return max(0.0, float(self.config.get("taker_fee_rate") or 0))
        return self._fee_rate()

    def _estimate_fee(self, volume: float, liquidity: str = "taker") -> float:
        rate = self._maker_fee_rate() if liquidity == "maker" else self._taker_fee_rate()
        return volume * rate

    def _record_trade_value(
        self,
        price: float,
        qty: float,
        gross_profit: float = 0.0,
        *,
        volume: float | None = None,
        fee: float | None = None,
        fee_asset: str = "USDT",
        fee_source: str = "estimated",
    ) -> dict:
        notional = volume if volume is not None else price * qty
        fee = fee if fee is not None else self._estimate_fee(notional)
        net_profit = gross_profit - fee

        self.total_volume += notional
        self.total_fee += fee
        self.total_profit += net_profit
        if gross_profit:
            self.gross_profit += gross_profit

        return {
            "volume": notional,
            "fee": fee,
            "gross_profit": gross_profit,
            "net_profit": net_profit,
            "fee_asset": fee_asset,
            "fee_source": fee_source,
        }

    def _get_trade_stats(
        self,
        order_id: str,
        fallback_price: float,
        fallback_qty: float,
        *,
        allow_estimate: bool = True,
        liquidity_hint: str = "taker",
    ) -> dict | None:
        fallback_volume = fallback_price * fallback_qty
        fallback_fee = self._estimate_fee(fallback_volume, liquidity_hint)
        stats = {
            "price": fallback_price,
            "qty": fallback_qty,
            "volume": fallback_volume,
            "fee": fallback_fee,
            "fee_asset": "USDT estimated",
            "fee_source": "estimated",
            "maker_count": 1 if liquidity_hint == "maker" else 0,
            "taker_count": 1 if liquidity_hint != "maker" else 0,
        }

        if not order_id or not hasattr(self.client, "get_order_trades"):
            if not allow_estimate:
                return None
            return stats

        try:
            resp = self.client.get_order_trades(self.config["symbol"], order_id)
        except Exception as exc:
            logger.warning("Fetch trade details failed order_id=%s msg=%s", order_id, exc)
            if not allow_estimate:
                return None
            return stats

        if resp.get("retCode") != 0:
            logger.warning(
                "Fetch trade details rejected order_id=%s msg=%s",
                order_id,
                resp.get("retMsg"),
            )
            if not allow_estimate:
                return None
            return stats

        trades = resp.get("result", {}).get("list", [])
        if not trades:
            if not allow_estimate:
                return None
            return stats

        total_qty = 0.0
        total_volume = 0.0
        total_fee = 0.0
        fee_assets: set[str] = set()
        converted_all = True
        maker_count = 0
        taker_count = 0

        for trade in trades:
            qty = float(trade.get("qty", 0) or 0)
            price = float(trade.get("price", fallback_price) or fallback_price)
            volume = float(trade.get("volume", 0) or (price * qty))
            fee_usdt_text = trade.get("feeUsdt", "")
            fee_asset = str(trade.get("feeAsset", "USDT") or "USDT")
            fee_assets.add(fee_asset)
            if trade.get("isMaker"):
                maker_count += 1
            else:
                taker_count += 1

            total_qty += qty
            total_volume += volume
            if fee_usdt_text != "":
                total_fee += float(fee_usdt_text)
            else:
                converted_all = False
                total_fee += self._estimate_fee(volume, "maker" if trade.get("isMaker") else "taker")

        if total_qty <= 0 or total_volume <= 0:
            if not allow_estimate:
                return None
            return stats

        return {
            "price": total_volume / total_qty,
            "qty": total_qty,
            "volume": total_volume,
            "fee": total_fee,
            "fee_asset": ",".join(sorted(fee_assets)) if fee_assets else "USDT",
            "fee_source": "exchange" if converted_all else "mixed",
            "maker_count": maker_count,
            "taker_count": taker_count,
        }

    @staticmethod
    def _liquidity_label(stats: dict) -> str:
        maker_count = int(stats.get("maker_count", 0) or 0)
        taker_count = int(stats.get("taker_count", 0) or 0)
        if maker_count and not taker_count:
            return "maker"
        if taker_count and not maker_count:
            return "taker"
        if maker_count or taker_count:
            return "mixed"
        return "unknown"

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

    def _active_reduce_qty(self, side: str) -> float:
        return sum(
            float(order.get("qty", 0) or 0)
            for order in self.active_orders.values()
            if order.get("side") == side and order.get("reduce_only")
        )

    def _position_size(self, side: str) -> float:
        resp = self.client.get_positions(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch positions"))

        total = 0.0
        for position in resp["result"].get("list", []):
            if position.get("side") != side:
                continue
            try:
                total += float(position.get("size", 0) or 0)
            except (TypeError, ValueError):
                continue
        return total

    def _place_reduce_market(self, side: str, qty: float, reason: str) -> str:
        qty_text = self._fq(qty)
        result = self.client.place_order(
            symbol=self.config["symbol"],
            side=side,
            qty=qty_text,
            order_type="Market",
            reduce_only=True,
            order_link_id=f"repair_{side[0]}_{uuid.uuid4().hex[:6]}",
        )
        if result.get("retCode") != 0:
            raise RuntimeError(result.get("retMsg", f"Failed to place reduce-only repair order: {reason}"))
        self.trigger_message = f"Safety repair placed {side} reduce-only {qty_text}: {reason}"
        logger.warning(self.trigger_message)
        self._persist_state()
        return str(result["result"].get("orderId") or "")

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
        order_id = str(result.get("result", {}).get("orderId", ""))
        stats = self._get_trade_stats(order_id, self.current_price, self.initial_qty, liquidity_hint="taker")
        if stats is None:
            volume = self.current_price * self.initial_qty
            stats = {
                "price": self.current_price,
                "qty": self.initial_qty,
                "volume": volume,
                "fee": self._estimate_fee(volume, "taker"),
                "fee_asset": "USDT estimated",
                "fee_source": "estimated",
            }
        self.initial_entry_price = stats["price"]
        self._record_trade_value(
            stats["price"],
            stats["qty"],
            volume=stats["volume"],
            fee=stats["fee"],
            fee_asset=stats["fee_asset"],
            fee_source=stats["fee_source"],
        )
        self._persist_state()

    def _place_limit_open(self, side: str, qty: float, price: float):
        qty_text = self._fq(qty)
        price_text = self._fp(price)
        link_id = f"open_{side[0]}_{uuid.uuid4().hex[:6]}"
        result = self.client.place_order(
            symbol=self.config["symbol"],
            side=side,
            qty=qty_text,
            price=price_text,
            order_type="Limit",
            reduce_only=False,
            order_link_id=link_id,
            time_in_force="PostOnly",
        )
        if result.get("retCode") != 0:
            raise RuntimeError(result.get("retMsg", "Failed to place initial post-only order"))

        self.initial_side = side
        self.initial_qty = float(qty_text)
        self.initial_entry_price = 0.0
        self.waiting_initial_order = True
        self.trigger_message = f"Waiting for post-only opening order at {price_text}"
        self.opening_order = {
            "link_id": link_id,
            "order_id": result["result"]["orderId"],
            "side": side,
            "price": price_text,
            "qty": qty_text,
        }
        self._persist_state()

    def _initial_limit_price(self, side: str, current_price: float) -> float:
        configured_price = self.config.get("initial_order_price")
        if configured_price is not None:
            return float(configured_price)

        tick = float(self.tick_size)
        return current_price - tick if side == "Buy" else current_price + tick

    def _place(
        self,
        side: str,
        price: float,
        level_idx: int,
        reduce_only: bool,
        qty_override: float | None = None,
        entry_price: float | None = None,
    ) -> Optional[str]:
        raw_qty = float(qty_override) if qty_override is not None else float(self.config["qty_per_grid"])
        qty = self._fq(raw_qty)
        price_text = self._fp(price)
        link_id = f"g_{level_idx}_{side[0]}_{uuid.uuid4().hex[:6]}"

        if self._has_active_order(side, level_idx, reduce_only):
            return None
        if self._stopping:
            return None

        def submit_limit(use_post_only: bool):
            return self.client.place_order(
                    symbol=self.config["symbol"],
                    side=side,
                    qty=qty,
                    price=price_text,
                    order_type="Limit",
                    reduce_only=reduce_only,
                    order_link_id=link_id,
                    time_in_force="PostOnly" if use_post_only else None,
                )

        # Reduce-only orders are safety exits; never let maker-only rules prevent them.
        use_post_only = bool(self.config.get("grid_order_post_only", False)) and not reduce_only
        try:
            result = submit_limit(use_post_only)
        except Exception as exc:
            if reduce_only and use_post_only:
                logger.warning(
                    "Post-only reduce order failed; retrying as normal reduce-only limit side=%s price=%s msg=%s",
                    side,
                    price_text,
                    exc,
                )
                try:
                    result = submit_limit(False)
                except Exception as retry_exc:
                    logger.warning(
                        "Reduce-only limit retry failed; placing market repair side=%s price=%s msg=%s",
                        side,
                        price_text,
                        retry_exc,
                    )
                    return self._place_reduce_market(side, float(qty), "reduce limit placement failed")
            else:
                logger.warning(
                    "Place order failed side=%s price=%s reduce_only=%s msg=%s",
                    side,
                    price_text,
                    reduce_only,
                    exc,
                )
                return None

        if result.get("retCode") != 0 and reduce_only and use_post_only:
            logger.warning(
                "Post-only reduce order rejected; retrying as normal reduce-only limit side=%s price=%s msg=%s",
                side,
                price_text,
                result.get("retMsg"),
            )
            try:
                result = submit_limit(False)
            except Exception as retry_exc:
                logger.warning(
                    "Reduce-only limit retry failed; placing market repair side=%s price=%s msg=%s",
                    side,
                    price_text,
                    retry_exc,
                )
                self._place_reduce_market(side, float(qty), "reduce limit placement failed")
                return None

        if result.get("retCode") != 0 and reduce_only:
            logger.warning(
                "Reduce-only limit rejected; placing market repair side=%s price=%s msg=%s",
                side,
                price_text,
                result.get("retMsg"),
            )
            return self._place_reduce_market(side, float(qty), "reduce limit rejected")

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
            "order_type": "Limit",
            "time_in_force": "PostOnly" if use_post_only else "GTC",
            "reduce_only": reduce_only,
            "entry_price": entry_price,
        }
        self._persist_state()
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
            raise RuntimeError(
                f"Current price {current_price} must stay inside the configured range "
                f"{levels[0]} - {levels[-1]}"
            )

        self.waiting_trigger = False
        self.trigger_message = ""

        open_side = ""
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

        self._pending_targets = {
            "profit_targets": profit_targets,
            "add_targets": add_targets,
            "allocated_qtys": allocated_qtys,
            "qty_per_grid": qty_per_grid,
        }
        self._persist_state()

        if direction in {"long", "short"}:
            if self.config.get("initial_order_type", "market") == "post_only":
                self._place_limit_open(
                    open_side,
                    total_qty,
                    self._initial_limit_price(open_side, current_price),
                )
                return
            self._place_market_open(open_side, total_qty)
            self._deploy_pending_targets()

        if direction == "neutral":
            self._deploy_pending_targets()

        self.grid_ready = True
        self._persist_state()

    def _deploy_pending_targets(self, qty_scale: float = 1.0):
        if not self._pending_targets:
            raise RuntimeError("No pending grid targets were prepared")

        direction = self.config["direction"]
        profit_targets = self._pending_targets["profit_targets"]
        add_targets = self._pending_targets["add_targets"]
        allocated_qtys = [qty * qty_scale for qty in self._pending_targets["allocated_qtys"]]
        qty_per_grid = self._pending_targets["qty_per_grid"] * qty_scale

        if direction in {"long", "short"}:
            for target, allocated_qty in zip(profit_targets, allocated_qtys):
                idx, target_price, target_side = target
                self._place(
                    target_side,
                    target_price,
                    idx,
                    reduce_only=True,
                    qty_override=allocated_qty,
                    entry_price=self.initial_entry_price,
                )

            for idx, target_price, target_side in add_targets:
                self._place(target_side, target_price, idx, reduce_only=False, qty_override=qty_per_grid)
        else:
            for target, allocated_qty in zip(add_targets, allocated_qtys):
                idx, target_price, target_side = target
                self._place(target_side, target_price, idx, reduce_only=False, qty_override=allocated_qty)

        self.grid_ready = True
        self.waiting_initial_order = False
        self.trigger_message = ""
        self._persist_state()

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
            self._close_all_positions()
            self.client.cancel_all_orders(self.config["symbol"])
            self.active_orders.clear()
        except Exception as exc:
            logger.exception("Risk shutdown failed: %s", exc)
        finally:
            self._persist_state()

    async def _run_loop(self):
        while self.running:
            try:
                self.current_price = self._get_current_price()

                if self.waiting_trigger and self._is_trigger_hit(self.current_price):
                    self._deploy_initial_grid(self.current_price)

                if self.waiting_initial_order:
                    await self._check_initial_order()

                if self.grid_ready and self._risk_hit(self.current_price):
                    await self._shutdown_with_close()
                    break

                if self.grid_ready:
                    await self._check_fills()
                    self._repair_boundary_position()
                    self._resume_paused_replacements()

                await asyncio.sleep(3)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Grid polling failed: %s", exc)
                await asyncio.sleep(5)

    def _repair_boundary_position(self):
        now = time.time()
        if self._boundary_repair_in_progress or now < self._boundary_repair_retry_after:
            return

        direction = self.config["direction"]
        lower = float(self.config["lower_price"])
        upper = float(self.config["upper_price"])

        if direction == "short" and self.current_price <= lower:
            position_side = "Sell"
            close_side = "Buy"
            reason = f"short grid below lower boundary {lower}"
        elif direction == "long" and self.current_price >= upper:
            position_side = "Buy"
            close_side = "Sell"
            reason = f"long grid above upper boundary {upper}"
        else:
            return

        position_qty = self._position_size(position_side)
        if position_qty < self.min_qty:
            return

        self._boundary_repair_in_progress = True
        try:
            self._cancel_stale_reduce_orders(close_side)
            refreshed_qty = self._position_size(position_side)
            if refreshed_qty >= self.min_qty:
                self._place_reduce_market(close_side, refreshed_qty, reason)
                self._boundary_repair_retry_after = time.time() + 2
        finally:
            self._boundary_repair_in_progress = False

    def _cancel_stale_reduce_orders(self, side: str):
        for link_id, order in list(self.active_orders.items()):
            if order.get("side") != side or not order.get("reduce_only"):
                continue

            order_id = str(order.get("order_id", ""))
            try:
                result = {"retCode": 0}
                if order_id:
                    result = self.client.cancel_order(self.config["symbol"], order_id)
                if result.get("retCode") != 0:
                    raise RuntimeError(result.get("retMsg", "Failed to cancel stale reduce order"))
                self.active_orders.pop(link_id, None)
            except Exception as exc:
                logger.warning(
                    "Failed to cancel stale reduce order before boundary repair symbol=%s order_id=%s msg=%s",
                    self.config.get("symbol"),
                    order_id,
                    exc,
                )
        self._persist_state()

    async def _check_initial_order(self):
        if not self.opening_order:
            return

        resp = self.client.get_open_orders(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch open orders"))

        open_order_ids = {item["orderId"] for item in resp["result"].get("list", [])}
        order_id = self.opening_order["order_id"]
        if order_id in open_order_ids:
            return

        fallback_price = float(self.opening_order["price"])
        planned_qty = float(self.opening_order["qty"])
        stats = self._get_trade_stats(
            order_id,
            fallback_price,
            planned_qty,
            allow_estimate=False,
            liquidity_hint="maker",
        )
        if not stats or stats["qty"] <= 0:
            self.waiting_initial_order = False
            self.opening_order = None
            self.running = False
            self.trigger_message = "Post-only opening order closed without fills; please restart the grid."
            return

        qty_scale = stats["qty"] / planned_qty if planned_qty > 0 else 0
        if qty_scale <= 0:
            self.waiting_initial_order = False
            self.running = False
            self.trigger_message = "Opening order fill quantity is too small; please restart the grid."
            return

        allocated_qtys = self._pending_targets["allocated_qtys"] if self._pending_targets else []
        if allocated_qtys and min(qty * qty_scale for qty in allocated_qtys) < self.min_qty:
            self.waiting_initial_order = False
            self.running = False
            self.trigger_message = "Opening order partial fill is too small for grid allocation."
            return

        self.initial_qty = stats["qty"]
        self.initial_entry_price = stats["price"]
        self._record_trade_value(
            stats["price"],
            stats["qty"],
            volume=stats["volume"],
            fee=stats["fee"],
            fee_asset=stats["fee_asset"],
            fee_source=stats["fee_source"],
        )
        self.opening_order = None
        self._deploy_pending_targets(qty_scale)
        self._persist_state()

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
            handled = self._handle_closed_order(order)
            if not handled:
                logger.info(
                    "Grid order closed without confirmed fill symbol=%s order_id=%s link_id=%s",
                    self.config.get("symbol"),
                    order.get("order_id"),
                    link_id,
                )
            self._persist_state()

    def _handle_closed_order(self, order: dict) -> bool:
        fallback_qty = float(order["qty"])
        fallback_price = float(order["price"])
        allow_estimate = not hasattr(self.client, "get_order_trades")
        stats = self._get_trade_stats(
            order["order_id"],
            fallback_price,
            fallback_qty,
            allow_estimate=allow_estimate,
            liquidity_hint=self._order_liquidity_hint(order),
        )
        if not stats or stats["qty"] <= 0:
            return False

        filled_order = {**order, "qty": str(stats["qty"]), "fill_price": stats["price"]}
        self._record_fill(filled_order, stats)
        if self._in_grid_range():
            self._place_counter_order(filled_order)
        else:
            self.paused_replacements.append(filled_order)
            self.trigger_message = (
                f"Price {self.current_price} is outside grid range; "
                "counter order is queued until price returns."
            )
            self._persist_state()
        return True

    def _resume_paused_replacements(self):
        if not self.paused_replacements or not self._in_grid_range() or self._stopping:
            return

        pending = list(self.paused_replacements)
        remaining = []
        self.paused_replacements.clear()
        for order in pending:
            if not self._place_counter_order(order):
                remaining.append(order)
        self.paused_replacements = remaining
        self.trigger_message = (
            ""
            if not remaining
            else f"{len(remaining)} counter order(s) are still queued; retrying next poll."
        )
        self._persist_state()

    def _order_liquidity_hint(self, order: dict) -> str:
        if str(order.get("order_type", "")).lower() == "market":
            return "taker"
        return "maker" if order.get("time_in_force") == "PostOnly" else "maker"

    def _record_fill(self, order: dict, stats: dict | None = None):
        level_idx = order["level_idx"]
        fallback_qty = float(order["qty"])
        fallback_price = float(order["price"])
        stats = stats or self._get_trade_stats(
            order["order_id"],
            fallback_price,
            fallback_qty,
            liquidity_hint=self._order_liquidity_hint(order),
        )
        qty = stats["qty"]
        price = stats["price"]
        gross_profit = 0.0

        if order["reduce_only"]:
            entry_price = float(order.get("entry_price") or 0)
            if entry_price > 0:
                if self.config["direction"] == "long" and order["side"] == "Sell":
                    gross_profit = (price - entry_price) * qty
                elif self.config["direction"] == "short" and order["side"] == "Buy":
                    gross_profit = (entry_price - price) * qty
            elif level_idx + 1 < len(self.grid_levels):
                gross_profit = (self.grid_levels[level_idx + 1] - self.grid_levels[level_idx]) * qty
            self.completed_pairs += 1
        recorded = self._record_trade_value(
            price,
            qty,
            gross_profit,
            volume=stats["volume"],
            fee=stats["fee"],
            fee_asset=stats["fee_asset"],
            fee_source=stats["fee_source"],
        )

        self.filled_orders.append(
            {
                "side": order["side"],
                "price": price,
                "qty": qty,
                "level_idx": level_idx,
                "volume": round(recorded["volume"], 4),
                "fee": round(recorded["fee"], 4),
                "fee_asset": recorded["fee_asset"],
                "fee_source": recorded["fee_source"],
                "maker_count": stats.get("maker_count", 0),
                "taker_count": stats.get("taker_count", 0),
                "liquidity": self._liquidity_label(stats),
                "gross_profit": round(recorded["gross_profit"], 4),
                "profit": round(recorded["net_profit"], 4),
                "time": time.time(),
                "reduce_only": order["reduce_only"],
            }
        )
        self._persist_state()

    def _place_counter_leg(
        self,
        side: str,
        price: float,
        level_idx: int,
        *,
        reduce_only: bool,
        qty: float,
        entry_price: float | None = None,
    ) -> bool:
        if self._has_active_order(side, level_idx, reduce_only):
            return True
        return (
            self._place(
                side,
                price,
                level_idx,
                reduce_only=reduce_only,
                qty_override=qty,
                entry_price=entry_price,
            )
            is not None
        )

    def _place_counter_order(self, order: dict) -> bool:
        direction = self.config["direction"]
        side = order["side"]
        level_idx = order["level_idx"]
        qty = float(order["qty"])

        if direction == "long":
            if side == "Buy" and level_idx + 1 < len(self.grid_levels):
                return self._place_counter_leg(
                    "Sell",
                    self.grid_levels[level_idx + 1],
                    level_idx,
                    reduce_only=True,
                    qty=qty,
                    entry_price=float(order.get("fill_price") or order.get("price") or 0),
                )
            elif side == "Sell":
                return self._place_counter_leg(
                    "Buy", self.grid_levels[level_idx], level_idx, reduce_only=False, qty=qty
                )
        elif direction == "short":
            if side == "Sell":
                return self._place_counter_leg(
                    "Buy",
                    self.grid_levels[level_idx],
                    level_idx,
                    reduce_only=True,
                    qty=qty,
                    entry_price=float(order.get("fill_price") or order.get("price") or 0),
                )
            elif level_idx + 1 < len(self.grid_levels):
                return self._place_counter_leg(
                    "Sell", self.grid_levels[level_idx + 1], level_idx, reduce_only=False, qty=qty
                )
        else:
            if side == "Buy" and level_idx + 1 < len(self.grid_levels):
                return self._place_counter_leg(
                    "Sell", self.grid_levels[level_idx + 1], level_idx, reduce_only=False, qty=qty
                )
            elif side == "Sell":
                return self._place_counter_leg(
                    "Buy", self.grid_levels[level_idx], level_idx, reduce_only=False, qty=qty
                )
        return True
