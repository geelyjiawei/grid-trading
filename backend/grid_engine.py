import asyncio
import contextlib
import json
import logging
import re
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable, Optional


logger = logging.getLogger(__name__)

NORMAL_POLL_SECONDS = 3.0
FAST_POLL_SECONDS = 0.3
FAST_POLL_WINDOW_SECONDS = 15.0
USER_STREAM_KEEPALIVE_SECONDS = 30 * 60
BATCH_ORDER_CHUNK_SIZE = 5
POSITION_SYNC_GRACE_SECONDS = 2.0


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
        self.reduce_lots_by_level: dict[str, dict[str, float]] = {}
        self.reduce_lots_complete = False
        self.target_qty_by_level: dict[str, float] = {}
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
        self.baseline_position_side = ""
        self.baseline_position_qty = 0.0
        self.baseline_position_entry_price = 0.0
        self.grid_position_net_qty = 0.0
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
        self._allow_restore_baseline_migration = False
        self._task: Optional[asyncio.Task] = None
        self._wake_event: asyncio.Event | None = None
        self._fast_poll_until = 0.0
        self._user_stream_task: Optional[asyncio.Task] = None
        self._user_stream_listen_key = ""
        self._position_mismatch_seen_at = 0.0
        self._position_mismatch_signature: tuple[float, float, int] | None = None
        self._reduce_warning_at_by_signature: dict[tuple, float] = {}

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
            "reduce_lots_by_level": self.reduce_lots_by_level,
            "reduce_lots_complete": self.reduce_lots_complete,
            "target_qty_by_level": self.target_qty_by_level,
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
            "baseline_position_side": self.baseline_position_side,
            "baseline_position_qty": self.baseline_position_qty,
            "baseline_position_entry_price": self.baseline_position_entry_price,
            "grid_position_net_qty": self.grid_position_net_qty,
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
        saved_running = bool(state.get("running", False))
        self.config = dict(state.get("config") or self.config)
        self.grid_levels = list(state.get("grid_levels") or [])
        self.active_orders = dict(state.get("active_orders") or {})
        self.filled_orders = list(state.get("filled_orders") or [])
        self.completed_pairs = int(state.get("completed_pairs") or 0)
        self.reduce_lots_by_level = self._normalize_reduce_lots(state.get("reduce_lots_by_level") or {})
        self.reduce_lots_complete = bool(state.get("reduce_lots_complete", bool(self.reduce_lots_by_level)))
        self.target_qty_by_level = self._normalize_level_qtys(state.get("target_qty_by_level") or {})
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
        self.baseline_position_side = str(state.get("baseline_position_side") or "")
        self.baseline_position_qty = float(state.get("baseline_position_qty") or 0)
        self.baseline_position_entry_price = float(state.get("baseline_position_entry_price") or 0)
        self._allow_restore_baseline_migration = "grid_position_net_qty" not in state
        if "grid_position_net_qty" in state:
            self.grid_position_net_qty = float(state.get("grid_position_net_qty") or 0)
        else:
            self.grid_position_net_qty = self._derive_grid_position_net_qty()
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
            self._bootstrap_reduce_lots_from_legacy_state()
            if saved_running:
                self._migrate_baseline_position_from_exchange()
                self._reconcile_exchange_open_orders()
                self._reconcile_grid_position_protection()
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
        self._wake_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop())
        if self._supports_user_stream():
            self._user_stream_task = asyncio.create_task(self._user_stream_loop())
        self._persist_state()

    async def stop(self):
        self._stopping = True
        self.running = False
        await self._stop_user_stream()
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
        await self._stop_user_stream()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._persist_state()

    def _mark_fast_poll(self, seconds: float = FAST_POLL_WINDOW_SECONDS):
        self._fast_poll_until = max(self._fast_poll_until, time.time() + seconds)
        if self._wake_event:
            self._wake_event.set()

    def _poll_interval(self) -> float:
        if self.waiting_initial_order or time.time() < self._fast_poll_until:
            return FAST_POLL_SECONDS
        return NORMAL_POLL_SECONDS

    async def _sleep_until_next_poll(self):
        interval = self._poll_interval()
        if not self._wake_event:
            await asyncio.sleep(interval)
            return

        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        self._wake_event.clear()

    def _supports_user_stream(self) -> bool:
        return all(
            hasattr(self.client, name)
            for name in (
                "start_user_stream",
                "keepalive_user_stream",
                "close_user_stream",
                "user_stream_url",
            )
        )

    async def _stop_user_stream(self):
        task = self._user_stream_task
        self._user_stream_task = None
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        listen_key = self._user_stream_listen_key
        self._user_stream_listen_key = ""
        if listen_key and hasattr(self.client, "close_user_stream"):
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self.client.close_user_stream, listen_key)

    async def _keepalive_user_stream(self, listen_key: str):
        while self.running and not self._stopping:
            await asyncio.sleep(USER_STREAM_KEEPALIVE_SECONDS)
            if not self.running or self._stopping:
                return
            await asyncio.to_thread(self.client.keepalive_user_stream, listen_key)

    def _is_relevant_user_stream_event(self, event: dict[str, Any]) -> bool:
        event_type = str(event.get("e") or "")
        if event_type not in {"ORDER_TRADE_UPDATE", "TRADE_LITE"}:
            return False
        symbol = str(event.get("s") or event.get("o", {}).get("s") or "").upper()
        return symbol == str(self.config.get("symbol", "")).upper()

    async def _user_stream_loop(self):
        try:
            import websockets
        except Exception as exc:
            logger.warning("Binance user stream unavailable symbol=%s msg=%s", self.config.get("symbol"), exc)
            return

        while self.running and not self._stopping:
            keepalive_task: asyncio.Task | None = None
            try:
                listen_key = await asyncio.to_thread(self.client.start_user_stream)
                if not listen_key:
                    raise RuntimeError("Empty listen key")
                self._user_stream_listen_key = listen_key
                keepalive_task = asyncio.create_task(self._keepalive_user_stream(listen_key))
                async with websockets.connect(
                    self.client.user_stream_url(listen_key),
                    ping_interval=20,
                    close_timeout=5,
                ) as websocket:
                    async for raw_message in websocket:
                        if not self.running or self._stopping:
                            break
                        try:
                            event = json.loads(raw_message)
                        except (TypeError, ValueError):
                            continue
                        if self._is_relevant_user_stream_event(event):
                            self._mark_fast_poll()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.running and not self._stopping:
                    logger.warning(
                        "Binance user stream disconnected symbol=%s msg=%s",
                        self.config.get("symbol"),
                        exc,
                    )
                    await asyncio.sleep(5)
            finally:
                if keepalive_task:
                    keepalive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await keepalive_task

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
            "reduce_lots_complete": self.reduce_lots_complete,
            "reduce_lots_by_level": self.reduce_lots_by_level,
            "reduce_protection": self.reduce_protection_snapshot(),
            "target_qty_by_level": self.target_qty_by_level,
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
            "baseline_position": {
                "side": self.baseline_position_side,
                "qty": round(self.baseline_position_qty, 8),
                "entry_price": round(self.baseline_position_entry_price, 10),
            },
            "grid_position_net_qty": round(self._grid_position_net_qty(), 8),
            "grid_position_qty": round(self._grid_position_qty(), 8),
            "expected_position_net_qty": round(self._expected_position_net_qty(), 8),
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

    def _position_sizing_mode(self) -> str:
        return str(self.config.get("position_sizing_mode") or "investment").lower().strip()

    def _fixed_grid_qty_for_level(self, level_idx: int, fallback_qty: float | None = None) -> float:
        if self._position_sizing_mode() != "fixed_grid_qty":
            return float(fallback_qty or 0)

        raw_qty = (
            self.target_qty_by_level.get(str(level_idx))
            or self.config.get("grid_order_qty")
            or self.config.get("qty_per_grid")
            or fallback_qty
            or 0
        )
        steps = self._qty_to_steps(float(raw_qty))
        if steps <= 0:
            return float(fallback_qty or 0)
        return self._steps_to_qty(steps)

    def _counter_qty_for_order(self, order: dict) -> float:
        qty = float(order["qty"])
        if self._position_sizing_mode() != "fixed_grid_qty":
            return qty
        return self._fixed_grid_qty_for_level(int(order.get("level_idx", 0) or 0), qty)

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

    def _fill_delta_stats(self, order: dict, stats: dict) -> dict | None:
        planned_qty = Decimal(str(order.get("qty", 0) or 0))
        total_qty = min(Decimal(str(stats.get("qty", 0) or 0)), planned_qty)
        processed_qty = Decimal(str(order.get("processed_fill_qty", 0) or 0))
        delta_qty = total_qty - processed_qty
        if delta_qty < Decimal(str(self.min_qty)):
            return None

        total_volume = Decimal(str(stats.get("volume", 0) or 0))
        total_fee = Decimal(str(stats.get("fee", 0) or 0))
        processed_volume = Decimal(str(order.get("processed_fill_volume", 0) or 0))
        processed_fee = Decimal(str(order.get("processed_fill_fee", 0) or 0))
        delta_volume = total_volume - processed_volume
        if delta_volume <= 0:
            delta_volume = Decimal(str(stats.get("price", order.get("price", 0)) or 0)) * delta_qty
        delta_fee = total_fee - processed_fee
        delta_price = delta_volume / delta_qty if delta_qty > 0 else Decimal(str(stats.get("price", 0) or 0))

        return {
            **stats,
            "price": float(delta_price),
            "qty": float(delta_qty),
            "volume": float(delta_volume),
            "fee": float(delta_fee),
        }

    def _mark_order_fill_processed(self, order: dict, stats: dict):
        planned_qty = Decimal(str(order.get("qty", 0) or 0))
        processed_qty = min(Decimal(str(stats.get("qty", 0) or 0)), planned_qty)
        order["processed_fill_qty"] = float(processed_qty)
        order["processed_fill_volume"] = float(Decimal(str(stats.get("volume", 0) or 0)))
        order["processed_fill_fee"] = float(Decimal(str(stats.get("fee", 0) or 0)))

    def _record_execution_delta(self, order: dict, stats: dict) -> bool:
        delta_stats = self._fill_delta_stats(order, stats)
        self._mark_order_fill_processed(order, stats)
        if not delta_stats:
            return False

        filled_order = {**order, "qty": str(delta_stats["qty"]), "fill_price": delta_stats["price"]}
        self._record_fill(filled_order, delta_stats)
        if self._in_grid_range() or self._counter_order_reduces_grid_position(filled_order):
            self._place_counter_order(filled_order)
        else:
            self.paused_replacements.append(filled_order)
            self.trigger_message = (
                f"Price {self.current_price} is outside grid range; "
                "counter order is queued until price returns."
            )
            self._persist_state()
        return True

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

    def _active_order_qty(self, side: str, level_idx: int, reduce_only: bool) -> float:
        total = Decimal("0")
        for order in self.active_orders.values():
            if (
                order.get("side") == side
                and int(order.get("level_idx", 0) or 0) == level_idx
                and bool(order.get("reduce_only")) == reduce_only
            ):
                total += Decimal(str(order.get("qty", 0) or 0))
        return float(total)

    def _active_order_qty_deficit(self, side: str, level_idx: int, reduce_only: bool, qty: float) -> float:
        requested = Decimal(str(qty))
        active = Decimal(str(self._active_order_qty(side, level_idx, reduce_only)))
        deficit = requested - active
        minimum_order_qty = max(Decimal(self.qty_step), Decimal(str(self.min_qty)))
        if deficit < minimum_order_qty:
            return 0.0
        return float(deficit)

    def _active_reduce_qty(self, side: str) -> float:
        return sum(
            float(order.get("qty", 0) or 0)
            for order in self.active_orders.values()
            if order.get("side") == side and order.get("reduce_only")
        )

    def reduce_protection_snapshot(self) -> dict:
        direction = self.config.get("direction")
        reduce_side = self._reduce_side()
        grid_qty = Decimal(str(self._grid_position_qty()))
        min_qty = Decimal(str(self.min_qty))
        active_total = Decimal(str(self._active_reduce_qty(reduce_side))) if reduce_side else Decimal("0")

        snapshot = {
            "enabled": direction in {"long", "short"} and bool(reduce_side),
            "ledger_ok": True,
            "ledger_reason": "",
            "grid_qty": float(grid_qty),
            "active_reduce_qty": float(active_total),
            "expected_reduce_qty": 0.0,
            "missing_by_level": [],
            "excess_by_level": [],
            "has_level_gap": False,
            "has_risk": False,
        }
        if not snapshot["enabled"] or grid_qty < min_qty:
            return snapshot

        lots, reason = self._reduce_lots_for_repair()
        if lots is None:
            snapshot["ledger_ok"] = False
            snapshot["ledger_reason"] = reason or "reduce protection ledger is incomplete"
            stored_lots = self._reduce_lot_decimal_map()
            if stored_lots:
                lots = stored_lots
            else:
                snapshot["has_risk"] = active_total + min_qty < grid_qty
                return snapshot

        expected_total = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        snapshot["expected_reduce_qty"] = float(expected_total)
        if abs(expected_total - grid_qty) >= min_qty:
            snapshot["ledger_ok"] = False
            snapshot["ledger_reason"] = (
                f"reduce protection ledger qty {float(expected_total)} "
                f"does not match grid qty {float(grid_qty)}"
            )
            snapshot["has_risk"] = True

        level_indexes = set(lots)
        for order in self.active_orders.values():
            if order.get("side") == reduce_side and order.get("reduce_only"):
                level_indexes.add(int(order.get("level_idx", 0) or 0))

        for level_idx in sorted(level_indexes):
            expected_qty = Decimal("0")
            if level_idx in lots:
                expected_qty = lots[level_idx]["qty"]
            active_qty = Decimal(str(self._active_order_qty(reduce_side, level_idx, True)))
            diff = expected_qty - active_qty
            if diff >= min_qty:
                snapshot["missing_by_level"].append(
                    {
                        "level": level_idx,
                        "price": self._fp(self.grid_levels[level_idx])
                        if 0 <= level_idx < len(self.grid_levels)
                        else "",
                        "expected_qty": float(expected_qty),
                        "active_qty": float(active_qty),
                        "missing_qty": float(diff),
                    }
                )
            elif -diff >= min_qty:
                snapshot["excess_by_level"].append(
                    {
                        "level": level_idx,
                        "price": self._fp(self.grid_levels[level_idx])
                        if 0 <= level_idx < len(self.grid_levels)
                        else "",
                        "expected_qty": float(expected_qty),
                        "active_qty": float(active_qty),
                        "excess_qty": float(-diff),
                    }
                )

        snapshot["has_level_gap"] = bool(snapshot["missing_by_level"] or snapshot["excess_by_level"])
        snapshot["has_risk"] = bool(snapshot["has_risk"] or snapshot["has_level_gap"])
        return snapshot

    def _reduce_side(self) -> str:
        direction = self.config["direction"]
        if direction == "long":
            return "Sell"
        if direction == "short":
            return "Buy"
        return ""

    def _is_grid_reduce_side(self, side: str, reduce_only: bool) -> bool:
        return bool(reduce_only) and side == self._reduce_side()

    def _available_reduce_qty(self, side: str) -> float:
        if not self._is_grid_reduce_side(side, True):
            return float("inf")
        available = Decimal(str(self._grid_position_qty())) - Decimal(str(self._active_reduce_qty(side)))
        if available <= 0:
            return 0.0
        return float(available)

    def _cap_reduce_order_qty(self, side: str, qty: float) -> float:
        if not self._is_grid_reduce_side(side, True):
            return qty
        available = self._available_reduce_qty(side)
        return min(qty, available)

    def _allocate_qtys(self, raw_total_qty: float, target_count: int) -> list[float]:
        if target_count <= 0:
            return []
        total_steps = self._qty_to_steps(raw_total_qty)
        if total_steps < target_count:
            return []
        base_steps = total_steps // target_count
        remainder_steps = total_steps % target_count
        return [
            self._steps_to_qty(base_steps + (1 if index < remainder_steps else 0))
            for index in range(target_count)
        ]

    def _sort_reduce_orders_for_trim(self, reduce_side: str) -> list[tuple[str, dict]]:
        orders = [
            (link_id, order)
            for link_id, order in self.active_orders.items()
            if order.get("side") == reduce_side and order.get("reduce_only")
        ]
        reverse = reduce_side == "Buy"
        return sorted(
            orders,
            key=lambda item: float(item[1].get("price", 0) or 0),
            reverse=reverse,
        )

    def _trim_reduce_overcommit(self) -> bool:
        reduce_side = self._reduce_side()
        if not reduce_side:
            return False

        grid_qty = self._grid_position_qty()
        active_reduce_qty = self._active_reduce_qty(reduce_side)
        excess = active_reduce_qty - grid_qty
        if excess < self.min_qty:
            return False

        logger.warning(
            "Reduce-only overcommitted; trimming excess symbol=%s reduce_side=%s active_reduce_qty=%s grid_qty=%s excess=%s",
            self.config.get("symbol"),
            reduce_side,
            active_reduce_qty,
            grid_qty,
            excess,
        )

        remaining_excess = Decimal(str(excess))
        changed = False
        for link_id, order in self._sort_reduce_orders_for_trim(reduce_side):
            if remaining_excess < Decimal(str(self.min_qty)):
                break

            order_qty = Decimal(str(order.get("qty", 0) or 0))
            replacement_qty = Decimal("0")
            if order_qty > remaining_excess:
                replacement_qty = order_qty - remaining_excess
            order_id = str(order.get("order_id", "") or "")
            try:
                result = {"retCode": 0}
                if order_id:
                    result = self.client.cancel_order(self.config["symbol"], order_id)
                if result.get("retCode") != 0:
                    raise RuntimeError(result.get("retMsg", "Failed to cancel excess reduce order"))
                self.active_orders.pop(link_id, None)
                remaining_excess -= min(order_qty, remaining_excess)
                changed = True
                logger.warning(
                    "Cancelled excess reduce-only order symbol=%s order_id=%s link_id=%s qty=%s",
                    self.config.get("symbol"),
                    order_id,
                    link_id,
                    order.get("qty"),
                )
                if replacement_qty >= Decimal(str(self.min_qty)):
                    self._place(
                        reduce_side,
                        float(order.get("price", 0) or 0),
                        int(order.get("level_idx", 0) or 0),
                        reduce_only=True,
                        qty_override=float(replacement_qty),
                        entry_price=order.get("entry_price"),
                        allow_duplicate=True,
                    )
                    logger.warning(
                        "Replaced trimmed reduce-only remainder symbol=%s old_order_id=%s price=%s qty=%s",
                        self.config.get("symbol"),
                        order_id,
                        order.get("price"),
                        self._fq(float(replacement_qty)),
                    )
            except Exception as exc:
                status = self._get_order_status(order)
                if self._is_cancelled_status(status):
                    self.active_orders.pop(link_id, None)
                    remaining_excess -= min(order_qty, remaining_excess)
                    changed = True
                    continue
                logger.warning(
                    "Failed to cancel excess reduce-only order symbol=%s order_id=%s link_id=%s status=%s msg=%s",
                    self.config.get("symbol"),
                    order_id,
                    link_id,
                    status,
                    exc,
                )
                self._mark_fast_poll()
                break

        if changed:
            self.trigger_message = (
                f"Trimmed excess reduce-only protection: {reduce_side} {float(excess):g}"
            )
            self._persist_state()
            return True
        else:
            self._mark_fast_poll()
        return True

    def _normalize_fixed_reduce_protection(self) -> bool:
        if self._position_sizing_mode() != "fixed_grid_qty":
            return False

        reduce_side = self._reduce_side()
        if not reduce_side:
            return False

        grid_qty = Decimal(str(self._grid_position_qty()))
        min_qty = Decimal(str(self.min_qty))
        if grid_qty < min_qty:
            return False

        orders = self._sort_reduce_orders_for_trim(reduce_side)
        if not orders:
            return False

        minimum_diff = max(Decimal(str(self.qty_step)), min_qty)
        remaining = grid_qty
        replacements: list[tuple[str, dict, Decimal]] = []

        for link_id, order in orders:
            level_idx = int(order.get("level_idx", 0) or 0)
            target_qty = Decimal(str(self._fixed_grid_qty_for_level(level_idx, float(order.get("qty", 0) or 0))))
            if target_qty < min_qty:
                target_qty = Decimal(str(order.get("qty", 0) or 0))

            desired_qty = Decimal("0")
            if remaining >= min_qty:
                desired_qty = target_qty if remaining >= target_qty else remaining
                remaining -= desired_qty

            if desired_qty >= min_qty:
                desired_qty = Decimal(self._fq(float(desired_qty)))

            current_qty = Decimal(str(order.get("qty", 0) or 0))
            if abs(current_qty - desired_qty) >= minimum_diff:
                replacements.append((link_id, order, desired_qty))

        if not replacements:
            return False

        planned_replacements: list[tuple[dict, Decimal]] = []
        for link_id, order, desired_qty in replacements:
            order_id = str(order.get("order_id", "") or "")
            try:
                result = {"retCode": 0}
                if order_id:
                    result = self.client.cancel_order(self.config["symbol"], order_id)
                if result.get("retCode") != 0:
                    raise RuntimeError(result.get("retMsg", "Failed to cancel fixed-grid reduce order"))
                self.active_orders.pop(link_id, None)
                if desired_qty >= min_qty:
                    planned_replacements.append((order, desired_qty))
            except Exception as exc:
                status = self._get_order_status(order)
                if self._is_cancelled_status(status):
                    self.active_orders.pop(link_id, None)
                    if desired_qty >= min_qty:
                        planned_replacements.append((order, desired_qty))
                    continue
                logger.warning(
                    "Failed to normalize fixed-grid reduce order symbol=%s order_id=%s link_id=%s status=%s msg=%s",
                    self.config.get("symbol"),
                    order_id,
                    link_id,
                    status,
                    exc,
                )
                self._mark_fast_poll()
                self._persist_state()
                return True

        placed_qty = Decimal("0")
        for order, desired_qty in planned_replacements:
            placed = self._place(
                reduce_side,
                float(order.get("price", 0) or 0),
                int(order.get("level_idx", 0) or 0),
                reduce_only=True,
                qty_override=float(desired_qty),
                entry_price=order.get("entry_price"),
                allow_duplicate=True,
            )
            if placed:
                placed_qty += desired_qty

        self.trigger_message = (
            f"Normalized fixed-grid reduce-only protection: "
            f"{len(replacements)} order(s), replaced {self._fq(float(placed_qty))}"
        )
        logger.warning(
            "Normalized fixed-grid reduce-only protection symbol=%s orders=%s replaced_qty=%s grid_qty=%s",
            self.config.get("symbol"),
            len(replacements),
            self._fq(float(placed_qty)),
            self._fq(float(grid_qty)),
        )
        self._persist_state()
        return True

    def _halt_if_baseline_breached(self) -> bool:
        if not self.baseline_position_side or self.baseline_position_qty < self.min_qty:
            return False
        if self.config["direction"] not in {"long", "short"}:
            return False

        actual_same_side_qty = self._position_size(self.baseline_position_side)
        if actual_same_side_qty + self.min_qty >= self.baseline_position_qty:
            return False

        self.trigger_message = (
            f"Baseline position protection halted grid: expected at least "
            f"{self.baseline_position_qty:g} {self.baseline_position_side}, "
            f"exchange has {actual_same_side_qty:g}."
        )
        logger.error(
            "Baseline position breached symbol=%s baseline_side=%s baseline_qty=%s actual_qty=%s",
            self.config.get("symbol"),
            self.baseline_position_side,
            self.baseline_position_qty,
            actual_same_side_qty,
        )
        try:
            result = self.client.cancel_all_orders(self.config["symbol"])
            if result.get("retCode") != 0:
                raise RuntimeError(result.get("retMsg", "Failed to cancel orders after baseline breach"))
        except Exception as exc:
            logger.warning(
                "Failed to cancel orders after baseline breach symbol=%s msg=%s",
                self.config.get("symbol"),
                exc,
            )
        self.active_orders.clear()
        self.grid_ready = False
        self.running = False
        self._stopping = True
        self._persist_state()
        return True

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        return bool(value)

    @staticmethod
    def _parse_grid_link_id(link_id: str) -> tuple[int, str] | None:
        match = re.match(r"^g_(\d+)_([BS])_", str(link_id or ""))
        if not match:
            return None
        return int(match.group(1)), "Buy" if match.group(2) == "B" else "Sell"

    def _fetch_open_orders(self) -> list[dict]:
        resp = self.client.get_open_orders(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch open orders"))
        return list(resp.get("result", {}).get("list", []))

    def _adopt_exchange_grid_orders(self, open_orders: list[dict]) -> bool:
        known_order_ids = {str(order.get("order_id", "")) for order in self.active_orders.values()}
        adopted = False
        for item in open_orders:
            order_id = str(item.get("orderId", "") or "")
            link_id = str(item.get("orderLinkId", "") or "")
            parsed = self._parse_grid_link_id(link_id)
            if not order_id or not parsed or link_id in self.active_orders or order_id in known_order_ids:
                continue

            level_idx, side_from_link = parsed
            side = str(item.get("side") or side_from_link)
            self.active_orders[link_id] = {
                "link_id": link_id,
                "order_id": order_id,
                "level_idx": level_idx,
                "side": side,
                "price": str(item.get("price", "0")),
                "qty": str(item.get("qty", "0")),
                "status": "open",
                "order_type": "Limit",
                "time_in_force": "PostOnly" if str(item.get("timeInForce", "")) == "PostOnly" else "GTC",
                "reduce_only": self._truthy(item.get("reduceOnly", False)),
                "entry_price": None,
                "processed_fill_qty": 0.0,
                "processed_fill_volume": 0.0,
                "processed_fill_fee": 0.0,
            }
            known_order_ids.add(order_id)
            adopted = True
            logger.warning(
                "Adopted exchange grid order missing from local state symbol=%s order_id=%s link_id=%s",
                self.config.get("symbol"),
                order_id,
                link_id,
            )
        if adopted:
            self._persist_state()
        return adopted

    def _handle_order_execution_snapshot(self, order: dict, snapshot: dict) -> bool:
        fallback_price = float(order["price"])
        fallback_qty = float(order["qty"])
        stats = self._get_trade_stats(
            order["order_id"],
            fallback_price,
            fallback_qty,
            allow_estimate=False,
            liquidity_hint=self._order_liquidity_hint(order),
        )
        if not stats or stats["qty"] <= 0:
            stats = self._execution_stats_from_order_snapshot(
                snapshot,
                fallback_price,
                fallback_qty,
                liquidity_hint=self._order_liquidity_hint(order),
            )
        if not stats or stats["qty"] <= 0:
            return False
        return self._record_execution_delta(order, stats)

    def _reconcile_exchange_open_orders(self, open_orders: list[dict] | None = None) -> bool:
        open_orders = self._fetch_open_orders() if open_orders is None else open_orders
        changed = self._adopt_exchange_grid_orders(open_orders)
        open_order_ids = {str(item.get("orderId", "")) for item in open_orders}
        open_orders_by_id = {str(item.get("orderId", "")): item for item in open_orders}
        if self._stopping:
            return changed

        for order in list(self.active_orders.values()):
            order_id = str(order.get("order_id", "") or "")
            snapshot = open_orders_by_id.get(order_id)
            if not snapshot:
                continue
            if self._handle_order_execution_snapshot(order, snapshot):
                changed = True

        closed_links = [
            link_id
            for link_id, order in list(self.active_orders.items())
            if str(order.get("order_id", "")) not in open_order_ids
        ]
        for link_id in closed_links:
            if self._stopping or link_id not in self.active_orders:
                continue
            order = self.active_orders[link_id]
            status = self._get_order_status(order)
            if self._is_cancelled_status(status):
                self._handle_cancelled_order(link_id, order)
                changed = True
                continue
            if self._is_filled_status(status):
                changed = (
                    self._handle_confirmed_closed_order(
                        link_id,
                        order,
                        allow_estimate=not hasattr(self.client, "get_order_trades"),
                    )
                    or changed
                )
                continue
            if self._is_partial_status(status):
                snapshot = self._get_order_snapshot(order)
                if self._handle_order_execution_snapshot(order, snapshot):
                    changed = True
                self._mark_fast_poll()
                continue
            if status == "UNKNOWN":
                # Unknown may be a temporary exchange/API gap. Only mutate state if trades prove a fill.
                changed = (
                    self._handle_confirmed_closed_order(link_id, order, allow_estimate=False)
                    or changed
                )
                continue

            logger.info(
                "Grid order absent from open orders but not terminal symbol=%s order_id=%s link_id=%s status=%s",
                self.config.get("symbol"),
                order.get("order_id"),
                link_id,
                status,
            )
        return changed

    def _handle_cancelled_order(self, link_id: str, order: dict):
        fallback_qty = float(order["qty"])
        fallback_price = float(order["price"])
        stats = self._get_trade_stats(
            order["order_id"],
            fallback_price,
            fallback_qty,
            allow_estimate=False,
            liquidity_hint=self._order_liquidity_hint(order),
        )

        filled_qty = 0.0
        if stats and stats["qty"] > 0:
            filled_qty = min(float(stats["qty"]), fallback_qty)
            self._record_execution_delta(order, {**stats, "qty": filled_qty})
        else:
            filled_qty = float(order.get("processed_fill_qty", 0) or 0)

        self.active_orders.pop(link_id, None)

        remaining_qty = fallback_qty - filled_qty
        if remaining_qty >= self.min_qty:
            replacement = {
                **order,
                "qty": str(remaining_qty),
                "processed_fill_qty": 0.0,
                "processed_fill_volume": 0.0,
                "processed_fill_fee": 0.0,
            }
            self._replace_cancelled_order(replacement)
        self._persist_state()

    def _handle_confirmed_closed_order(self, link_id: str, order: dict, *, allow_estimate: bool) -> bool:
        handled = self._handle_closed_order(order, allow_estimate=allow_estimate)
        if handled:
            self.active_orders.pop(link_id, None)
            self._persist_state()
            return True
        logger.info(
            "Grid order closed without confirmed fill symbol=%s order_id=%s link_id=%s",
            self.config.get("symbol"),
            order.get("order_id"),
            link_id,
        )
        return False

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

    def _actual_position_net_qty(self) -> float:
        return sum(self._signed_qty(item["side"], item["qty"]) for item in self._position_snapshots())

    def _actual_grid_position_net_qty(self) -> float:
        actual_grid_qty = self._actual_position_net_qty() - self._baseline_position_net_qty()
        direction = self.config["direction"]
        if direction == "long":
            return max(0.0, actual_grid_qty)
        if direction == "short":
            return min(0.0, actual_grid_qty)
        return actual_grid_qty

    def _position_snapshots(self) -> list[dict]:
        resp = self.client.get_positions(self.config["symbol"])
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to fetch positions"))

        positions = []
        for position in resp["result"].get("list", []):
            side = str(position.get("side") or "")
            try:
                qty = abs(float(position.get("size", 0) or 0))
            except (TypeError, ValueError):
                qty = 0.0
            if side not in {"Buy", "Sell"} or qty < self.min_qty:
                continue
            try:
                entry_price = float(
                    position.get("avgPrice")
                    or position.get("entryPrice")
                    or position.get("entry_price")
                    or 0
                )
            except (TypeError, ValueError):
                entry_price = 0.0
            positions.append({"side": side, "qty": qty, "entry_price": entry_price})
        return positions

    def _capture_baseline_position(self, open_side: str):
        if self.baseline_position_qty >= self.min_qty or self.baseline_position_side:
            return

        positions = self._position_snapshots()
        if not positions:
            return

        direction = self.config["direction"]
        if direction == "neutral":
            raise RuntimeError(
                "Existing position detected; neutral grid cannot isolate old positions in one-way mode"
            )

        opposite = [position for position in positions if position["side"] != open_side]
        if opposite:
            summary = ", ".join(f"{item['side']} {item['qty']:g}" for item in positions)
            raise RuntimeError(
                f"Existing {self.config['symbol']} position would be offset by this {direction} grid: {summary}"
            )

        total_qty = sum(position["qty"] for position in positions)
        if total_qty < self.min_qty:
            return
        weighted_entry = sum(position["qty"] * position["entry_price"] for position in positions)
        self.baseline_position_side = open_side
        self.baseline_position_qty = total_qty
        self.baseline_position_entry_price = weighted_entry / total_qty if weighted_entry > 0 else 0.0

    def _migrate_baseline_position_from_exchange(self):
        if not self._allow_restore_baseline_migration:
            return
        if self.baseline_position_qty >= self.min_qty or self.baseline_position_side:
            return

        grid_net_qty = self._grid_position_net_qty()
        if abs(grid_net_qty) < self.min_qty:
            return

        positions = self._position_snapshots()
        actual_net_qty = sum(self._signed_qty(item["side"], item["qty"]) for item in positions)
        baseline_net_qty = actual_net_qty - grid_net_qty
        if abs(baseline_net_qty) < self.min_qty:
            return
        if baseline_net_qty * grid_net_qty <= 0:
            return

        baseline_side = "Buy" if baseline_net_qty > 0 else "Sell"
        baseline_qty = abs(baseline_net_qty)
        matching_positions = [item for item in positions if item["side"] == baseline_side]
        weighted_entry = sum(item["qty"] * item["entry_price"] for item in matching_positions)
        total_matching_qty = sum(item["qty"] for item in matching_positions)

        self.baseline_position_side = baseline_side
        self.baseline_position_qty = baseline_qty
        self.baseline_position_entry_price = (
            weighted_entry / total_matching_qty if weighted_entry > 0 and total_matching_qty > 0 else 0.0
        )

    @staticmethod
    def _signed_qty(side: str, qty: float) -> float:
        return qty if side == "Buy" else -qty if side == "Sell" else 0.0

    def _baseline_position_net_qty(self) -> float:
        return self._signed_qty(self.baseline_position_side, self.baseline_position_qty)

    def _grid_position_net_qty(self) -> float:
        if self.config["direction"] == "long":
            return max(0.0, self.grid_position_net_qty)
        if self.config["direction"] == "short":
            return min(0.0, self.grid_position_net_qty)
        return self.grid_position_net_qty

    def _derive_grid_position_net_qty(self) -> float:
        direction = self.config["direction"]
        if direction in {"long", "short"}:
            reduce_side = "Sell" if direction == "long" else "Buy"
            reduce_qty = sum(
                float(order.get("qty", 0) or 0)
                for order in self.active_orders.values()
                if order.get("side") == reduce_side and order.get("reduce_only")
            )
            if reduce_qty > 0:
                return reduce_qty if direction == "long" else -reduce_qty

        if direction == "long":
            net_qty = self.initial_qty
            for order in self.filled_orders:
                qty = float(order.get("qty", 0) or 0)
                if order.get("side") == "Buy" and not order.get("reduce_only"):
                    net_qty += qty
                elif order.get("side") == "Sell" and order.get("reduce_only"):
                    net_qty -= qty
            return max(0.0, net_qty)

        if direction == "short":
            net_qty = -self.initial_qty
            for order in self.filled_orders:
                qty = float(order.get("qty", 0) or 0)
                if order.get("side") == "Sell" and not order.get("reduce_only"):
                    net_qty -= qty
                elif order.get("side") == "Buy" and order.get("reduce_only"):
                    net_qty += qty
            return min(0.0, net_qty)

        for order in self.filled_orders:
            qty = float(order.get("qty", 0) or 0)
            net_qty += self._signed_qty(str(order.get("side") or ""), qty)
        return net_qty

    def _set_initial_grid_position(self, side: str, qty: float):
        self.grid_position_net_qty = self._signed_qty(side, qty)

    def _apply_grid_position_fill(self, order: dict, qty: float):
        direction = self.config["direction"]
        side = order.get("side")
        reduce_only = bool(order.get("reduce_only"))
        if direction == "long":
            if side == "Buy" and not reduce_only:
                self.grid_position_net_qty += qty
            elif side == "Sell" and reduce_only:
                self.grid_position_net_qty -= qty
            self.grid_position_net_qty = max(0.0, self.grid_position_net_qty)
        elif direction == "short":
            if side == "Sell" and not reduce_only:
                self.grid_position_net_qty -= qty
            elif side == "Buy" and reduce_only:
                self.grid_position_net_qty += qty
            self.grid_position_net_qty = min(0.0, self.grid_position_net_qty)
        else:
            self.grid_position_net_qty += self._signed_qty(str(side or ""), qty)

    def _apply_market_reduce_to_grid_position(self, side: str, qty: float):
        self._apply_grid_position_fill({"side": side, "reduce_only": True}, qty)
        self.reduce_lots_complete = False

    def _sync_grid_position_with_exchange(self):
        if self.config["direction"] not in {"long", "short"}:
            return

        if self._halt_if_baseline_breached():
            return

        actual_grid_qty = self._actual_grid_position_net_qty()
        local_grid_qty = self._grid_position_net_qty()
        if abs(actual_grid_qty - local_grid_qty) < self.min_qty:
            self._position_mismatch_seen_at = 0.0
            self._position_mismatch_signature = None
            return

        if self._active_reduce_qty(self._reduce_side()) >= self.min_qty:
            signature = (round(local_grid_qty, 8), round(actual_grid_qty, 8), len(self.active_orders))
            now = time.time()
            if self._position_mismatch_signature != signature:
                self._position_mismatch_signature = signature
                self._position_mismatch_seen_at = now
            if now - self._position_mismatch_seen_at < POSITION_SYNC_GRACE_SECONDS:
                logger.info(
                    "Delaying grid position sync until order ledger catches up symbol=%s local=%s actual=%s active_orders=%s",
                    self.config.get("symbol"),
                    local_grid_qty,
                    actual_grid_qty,
                    len(self.active_orders),
                )
                self._mark_fast_poll(POSITION_SYNC_GRACE_SECONDS)
                return

        logger.warning(
            "Grid position ledger reconciled with exchange symbol=%s local=%s actual=%s baseline=%s",
            self.config.get("symbol"),
            local_grid_qty,
            actual_grid_qty,
            self._baseline_position_net_qty(),
        )
        self.grid_position_net_qty = actual_grid_qty
        self.reduce_lots_complete = False
        self._position_mismatch_seen_at = 0.0
        self._position_mismatch_signature = None
        self._persist_state()

    def _reconcile_grid_position_protection(self):
        if self.config["direction"] not in {"long", "short"} or self._stopping:
            return

        self._sync_grid_position_with_exchange()
        if self._stopping:
            return
        if self._normalize_fixed_reduce_protection():
            return
        if self._trim_reduce_overcommit():
            return
        if self._handle_reduce_protection_level_risk():
            return

        reduce_side = self._reduce_side()
        if not reduce_side:
            return
        missing_qty = self._grid_position_qty() - self._active_reduce_qty(reduce_side)
        if missing_qty < self.min_qty:
            return

        if self._repair_missing_reduce_protection_from_ledger():
            return

        self._warn_missing_reduce_protection()

    @staticmethod
    def _lot_add(lots: dict[int, dict[str, Decimal]], level_idx: int, qty: Decimal, entry_price: Decimal):
        if qty <= 0:
            return
        lot = lots.setdefault(level_idx, {"qty": Decimal("0"), "entry_value": Decimal("0")})
        lot["qty"] += qty
        lot["entry_value"] += qty * entry_price

    @staticmethod
    def _normalize_reduce_lots(raw_lots: dict) -> dict[str, dict[str, float]]:
        normalized: dict[str, dict[str, float]] = {}
        for raw_level, raw_lot in (raw_lots or {}).items():
            try:
                level_idx = int(raw_level)
                qty = float((raw_lot or {}).get("qty") or 0)
                entry_value = float((raw_lot or {}).get("entry_value") or 0)
            except (TypeError, ValueError):
                continue
            if qty <= 0:
                continue
            normalized[str(level_idx)] = {"qty": qty, "entry_value": entry_value}
        return normalized

    @staticmethod
    def _normalize_level_qtys(raw_qtys: dict) -> dict[str, float]:
        normalized: dict[str, float] = {}
        for raw_level, raw_qty in (raw_qtys or {}).items():
            try:
                level_idx = int(raw_level)
                qty = float(raw_qty or 0)
            except (TypeError, ValueError):
                continue
            if qty > 0:
                normalized[str(level_idx)] = qty
        return normalized

    def _reduce_lot_decimal_map(self) -> dict[int, dict[str, Decimal]]:
        lots: dict[int, dict[str, Decimal]] = {}
        for raw_level, raw_lot in self.reduce_lots_by_level.items():
            try:
                level_idx = int(raw_level)
                qty = Decimal(str(raw_lot.get("qty", 0) or 0))
                entry_value = Decimal(str(raw_lot.get("entry_value", 0) or 0))
            except Exception:
                continue
            if qty > 0:
                lots[level_idx] = {"qty": qty, "entry_value": entry_value}
        return lots

    def _set_reduce_lot_decimal_map(self, lots: dict[int, dict[str, Decimal]]):
        self.reduce_lots_by_level = {
            str(level_idx): {
                "qty": float(lot["qty"]),
                "entry_value": float(lot["entry_value"]),
            }
            for level_idx, lot in sorted(lots.items())
            if lot.get("qty", Decimal("0")) > 0
        }

    def _lot_remove(self, lots: dict[int, dict[str, Decimal]], level_idx: int, qty: Decimal) -> bool:
        if qty <= 0:
            return True
        lot = lots.get(level_idx)
        minimum = Decimal(str(self.min_qty))
        if not lot or lot["qty"] + minimum < qty:
            return False
        if lot["qty"] <= qty + minimum:
            lots.pop(level_idx, None)
            return True
        average_entry = lot["entry_value"] / lot["qty"] if lot["qty"] > 0 else Decimal("0")
        lot["qty"] -= qty
        lot["entry_value"] -= average_entry * qty
        return True

    def _reset_reduce_lots_from_pending_targets(self, entry_price: float):
        if self.config["direction"] not in {"long", "short"} or not self._pending_targets:
            self.reduce_lots_by_level = {}
            self.reduce_lots_complete = False
            return

        lots: dict[int, dict[str, Decimal]] = {}
        entry = Decimal(str(entry_price or 0))
        for target, allocated_qty in zip(
            self._pending_targets.get("profit_targets") or [],
            self._pending_targets.get("allocated_qtys") or [],
        ):
            level_idx = int(target[0])
            target_qty = self.target_qty_by_level.get(str(level_idx), allocated_qty)
            self._lot_add(lots, level_idx, Decimal(str(target_qty)), entry)
        self._set_reduce_lot_decimal_map(lots)
        self.reduce_lots_complete = True

    def _record_reduce_lot_fill(self, order: dict, qty: float, price: float):
        if not self.reduce_lots_complete or self.config["direction"] not in {"long", "short"}:
            return

        try:
            level_idx = int(order.get("level_idx", 0) or 0)
            qty_decimal = Decimal(str(qty))
            price_decimal = Decimal(str(price))
        except Exception:
            self.reduce_lots_complete = False
            return

        lots = self._reduce_lot_decimal_map()
        direction = self.config["direction"]
        side = order.get("side")
        reduce_only = bool(order.get("reduce_only"))

        if direction == "short":
            if side == "Sell" and not reduce_only:
                self._lot_add(lots, level_idx, qty_decimal, price_decimal)
            elif side == "Buy" and reduce_only and not self._lot_remove(lots, level_idx, qty_decimal):
                self.reduce_lots_complete = False
                return
        elif direction == "long":
            if side == "Buy" and not reduce_only:
                self._lot_add(lots, level_idx, qty_decimal, price_decimal)
            elif side == "Sell" and reduce_only and not self._lot_remove(lots, level_idx, qty_decimal):
                self.reduce_lots_complete = False
                return

        self._set_reduce_lot_decimal_map(lots)

    def _reduce_target_for_level(self, level_idx: int) -> tuple[str, float] | None:
        direction = self.config["direction"]
        if direction == "short":
            if 0 <= level_idx < len(self.grid_levels):
                return "Buy", self.grid_levels[level_idx]
        elif direction == "long":
            if 0 <= level_idx + 1 < len(self.grid_levels):
                return "Sell", self.grid_levels[level_idx + 1]
        return None

    def _initial_reduce_lots_by_level(self) -> dict[int, dict[str, Decimal]] | None:
        direction = self.config["direction"]
        if direction not in {"long", "short"}:
            return {}
        if self.initial_qty < self.min_qty or self.initial_entry_price <= 0:
            return {}

        profit_targets, _ = self._target_orders_for_price(self.initial_entry_price)
        allocated_qtys = self._allocate_qtys(self.initial_qty, len(profit_targets))
        if len(allocated_qtys) != len(profit_targets):
            return None

        lots: dict[int, dict[str, Decimal]] = {}
        entry_price = Decimal(str(self.initial_entry_price))
        for target, allocated_qty in zip(profit_targets, allocated_qtys):
            level_idx = int(target[0])
            target_qty = self.target_qty_by_level.get(str(level_idx), allocated_qty)
            self._lot_add(lots, level_idx, Decimal(str(target_qty)), entry_price)
        return lots

    def _bootstrap_reduce_lots_from_legacy_state(self):
        if self.reduce_lots_complete or self.reduce_lots_by_level:
            return
        if self.config["direction"] not in {"long", "short"}:
            return
        lots, reason = self._reduce_lots_from_fill_ledger()
        if lots is None:
            logger.info(
                "Reduce lot ledger unavailable from legacy state symbol=%s reason=%s",
                self.config.get("symbol"),
                reason,
            )
            return
        ledger_qty = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if abs(ledger_qty - grid_qty) >= Decimal(str(self.min_qty)):
            logger.info(
                "Reduce lot ledger from legacy state does not match grid position symbol=%s ledger_qty=%s grid_qty=%s",
                self.config.get("symbol"),
                float(ledger_qty),
                float(grid_qty),
            )
            return
        self._set_reduce_lot_decimal_map(lots)
        self.reduce_lots_complete = True

    def _reduce_lots_from_fill_ledger(self) -> tuple[dict[int, dict[str, Decimal]] | None, str]:
        if self.config["direction"] not in {"long", "short"}:
            return {}, ""

        reduce_fill_count = sum(1 for order in self.filled_orders if order.get("reduce_only"))
        if int(self.completed_pairs or 0) > reduce_fill_count:
            return None, "fill history is truncated"

        lots = self._initial_reduce_lots_by_level()
        if lots is None:
            return None, "initial allocation is unavailable"

        direction = self.config["direction"]
        for order in self.filled_orders:
            try:
                level_idx = int(order.get("level_idx", 0) or 0)
                qty = Decimal(str(order.get("qty", 0) or 0))
                price = Decimal(str(order.get("price", 0) or 0))
            except Exception:
                return None, "fill ledger contains invalid values"
            side = order.get("side")
            reduce_only = bool(order.get("reduce_only"))

            if direction == "short":
                if side == "Sell" and not reduce_only:
                    self._lot_add(lots, level_idx, qty, price)
                elif side == "Buy" and reduce_only:
                    if not self._lot_remove(lots, level_idx, qty):
                        return None, "short reduce fill has no matching open lot"
            elif direction == "long":
                if side == "Buy" and not reduce_only:
                    self._lot_add(lots, level_idx, qty, price)
                elif side == "Sell" and reduce_only:
                    if not self._lot_remove(lots, level_idx, qty):
                        return None, "long reduce fill has no matching open lot"

        return lots, ""

    def _reduce_lots_for_repair(self) -> tuple[dict[int, dict[str, Decimal]] | None, str]:
        if self.reduce_lots_complete:
            return self._reduce_lot_decimal_map(), ""
        return self._reduce_lots_from_fill_ledger()

    def _should_log_reduce_warning(self, signature: tuple, interval: float = 60.0) -> bool:
        now = time.time()
        last_logged_at = self._reduce_warning_at_by_signature.get(signature, 0.0)
        if now - last_logged_at >= interval:
            self._reduce_warning_at_by_signature[signature] = now
            return True
        return False

    def _repair_missing_reduce_protection_from_ledger(self) -> bool:
        lots, reason = self._reduce_lots_for_repair()
        if lots is None:
            signature = ("ledger-unavailable", reason)
            if reason and self._should_log_reduce_warning(signature):
                logger.warning(
                    "Reduce-only protection ledger unavailable symbol=%s reason=%s",
                    self.config.get("symbol"),
                    reason,
                )
            return False

        expected_total = sum((lot["qty"] for lot in lots.values()), Decimal("0"))
        grid_qty = Decimal(str(self._grid_position_qty()))
        if abs(expected_total - grid_qty) >= Decimal(str(self.min_qty)):
            logger.warning(
                "Reduce-only protection ledger does not match grid position symbol=%s ledger_qty=%s grid_qty=%s",
                self.config.get("symbol"),
                float(expected_total),
                float(grid_qty),
            )
            return False

        placed_count = 0
        placed_qty = Decimal("0")
        for level_idx in sorted(lots):
            lot = lots[level_idx]
            target = self._reduce_target_for_level(level_idx)
            if not target:
                continue
            reduce_side, reduce_price = target
            active_qty = Decimal(str(self._active_order_qty(reduce_side, level_idx, True)))
            deficit = lot["qty"] - active_qty
            if deficit < Decimal(str(self.min_qty)):
                continue
            entry_price = lot["entry_value"] / lot["qty"] if lot["qty"] > 0 else Decimal("0")
            placed = self._place(
                reduce_side,
                reduce_price,
                level_idx,
                reduce_only=True,
                qty_override=float(deficit),
                entry_price=float(entry_price) if entry_price > 0 else None,
                allow_duplicate=True,
            )
            if placed:
                placed_count += 1
                placed_qty += deficit

        if placed_count:
            self.trigger_message = (
                f"Repaired {placed_count} missing reduce-only protection order(s) "
                f"from fill ledger: {self._fq(float(placed_qty))}"
            )
            logger.warning(
                "Repaired missing reduce-only protection from fill ledger symbol=%s orders=%s qty=%s",
                self.config.get("symbol"),
                placed_count,
                self._fq(float(placed_qty)),
            )
            self._persist_state()
            return True
        return False

    def _cancel_excess_reduce_protection_by_level(self, reduce_side: str, excess_by_level: list[dict]) -> bool:
        cancelled_count = 0
        cancelled_qty = Decimal("0")
        for excess in excess_by_level:
            level_idx = int(excess.get("level", 0) or 0)
            remaining_excess = Decimal(str(excess.get("excess_qty", 0) or 0))
            if remaining_excess < Decimal(str(self.min_qty)):
                continue
            candidates = [
                (link_id, order)
                for link_id, order in list(self.active_orders.items())
                if order.get("side") == reduce_side
                and order.get("reduce_only")
                and int(order.get("level_idx", 0) or 0) == level_idx
            ]
            for link_id, order in candidates:
                if remaining_excess < Decimal(str(self.min_qty)):
                    break
                order_qty = Decimal(str(order.get("qty", 0) or 0))
                order_id = str(order.get("order_id", "") or "")
                try:
                    result = {"retCode": 0}
                    if order_id:
                        result = self.client.cancel_order(self.config["symbol"], order_id)
                    if result.get("retCode") != 0:
                        raise RuntimeError(result.get("retMsg", "Failed to cancel excess reduce order"))
                    self.active_orders.pop(link_id, None)
                    cancelled_count += 1
                    cancelled_qty += order_qty
                    remaining_excess -= order_qty
                except Exception as exc:
                    logger.warning(
                        "Failed to cancel excess reduce protection symbol=%s level=%s order_id=%s msg=%s",
                        self.config.get("symbol"),
                        level_idx,
                        order_id,
                        exc,
                    )
                    self._mark_fast_poll()
                    self._persist_state()
                    return True

        if cancelled_count:
            self.trigger_message = (
                f"Cancelled {cancelled_count} misplaced reduce-only order(s); "
                "rebuilding level protection next poll."
            )
            logger.warning(
                "Cancelled misplaced reduce-only protection symbol=%s orders=%s qty=%s",
                self.config.get("symbol"),
                cancelled_count,
                self._fq(float(cancelled_qty)),
            )
            self._mark_fast_poll()
            self._persist_state()
            return True
        return False

    def _handle_reduce_protection_level_risk(self) -> bool:
        snapshot = self.reduce_protection_snapshot()
        if not snapshot.get("has_risk"):
            return False

        if not snapshot.get("ledger_ok", True):
            reason = snapshot.get("ledger_reason") or "reduce protection ledger is incomplete"
            self.trigger_message = (
                f"Reduce protection risk: {reason}; manual review required "
                "instead of placing guessed boundary orders."
            )
            signature = ("reduce-protection-ledger", reason)
            if self._should_log_reduce_warning(signature):
                logger.warning(
                    "Reduce protection ledger risk symbol=%s reason=%s",
                    self.config.get("symbol"),
                    reason,
                )
            self._persist_state()
            return True

        reduce_side = self._reduce_side()
        if snapshot.get("excess_by_level"):
            return self._cancel_excess_reduce_protection_by_level(
                reduce_side,
                list(snapshot.get("excess_by_level") or []),
            )

        if snapshot.get("missing_by_level"):
            if self._repair_missing_reduce_protection_from_ledger():
                return True
            missing_qty = sum(float(item.get("missing_qty", 0) or 0) for item in snapshot["missing_by_level"])
            self.trigger_message = (
                f"Reduce-only protection has level gaps: {self._fq(missing_qty)}; "
                "waiting for safe reduce capacity."
            )
            signature = ("reduce-protection-level-gap", self._fq(missing_qty))
            if self._should_log_reduce_warning(signature):
                logger.warning(
                    "Reduce protection level gaps symbol=%s missing_qty=%s gaps=%s",
                    self.config.get("symbol"),
                    self._fq(missing_qty),
                    snapshot["missing_by_level"],
                )
            self._persist_state()
            return True

        return False

    def _warn_missing_reduce_protection(self):
        grid_qty = self._grid_position_qty()
        if grid_qty < self.min_qty:
            return

        reduce_side = self._reduce_side()
        if not reduce_side:
            return

        active_reduce_qty = self._active_reduce_qty(reduce_side)
        missing_qty = grid_qty - active_reduce_qty
        if missing_qty < self.min_qty:
            return

        self.trigger_message = (
            f"Reduce-only protection is short by {self._fq(missing_qty)}; "
            "waiting for complete order/fill ledger instead of placing guessed boundary orders."
        )
        signature = (
            "missing-reduce-protection",
            reduce_side,
            self._fq(grid_qty),
            self._fq(active_reduce_qty),
            self._fq(missing_qty),
        )
        if self._should_log_reduce_warning(signature):
            logger.warning(
                "Reduce-only protection missing; not placing guessed boundary order symbol=%s side=%s grid_qty=%s active_reduce_qty=%s missing_qty=%s",
                self.config.get("symbol"),
                reduce_side,
                self._fq(grid_qty),
                self._fq(active_reduce_qty),
                self._fq(missing_qty),
            )
        self._persist_state()

    def _grid_position_qty(self) -> float:
        return abs(self._grid_position_net_qty())

    def _safe_grid_close_qty(self, position_side: str) -> float:
        position_qty = self._position_size(position_side)
        if position_qty < self.min_qty:
            return 0.0

        available_qty = position_qty
        if self.baseline_position_side == position_side:
            available_qty = max(0.0, position_qty - self.baseline_position_qty)

        return min(available_qty, self._grid_position_qty())

    def _expected_position_net_qty(self) -> float:
        return self._baseline_position_net_qty() + self._grid_position_net_qty()

    def estimate_grid_unrealized_pnl(self, mark_price: float) -> float:
        direction = self.config["direction"]
        grid_qty = self._grid_position_qty()
        if direction not in {"long", "short"} or grid_qty < self.min_qty:
            return 0.0

        reduce_side = "Sell" if direction == "long" else "Buy"
        remaining = grid_qty
        pnl = 0.0
        for order in self.active_orders.values():
            if not order.get("reduce_only") or order.get("side") != reduce_side:
                continue
            if remaining < self.min_qty:
                break
            qty = min(remaining, float(order.get("qty", 0) or 0))
            entry_price = float(order.get("entry_price") or self.initial_entry_price or order.get("price") or 0)
            if direction == "long":
                pnl += (mark_price - entry_price) * qty
            else:
                pnl += (entry_price - mark_price) * qty
            remaining -= qty

        if remaining >= self.min_qty and self.initial_entry_price > 0:
            if direction == "long":
                pnl += (mark_price - self.initial_entry_price) * remaining
            else:
                pnl += (self.initial_entry_price - mark_price) * remaining
        return pnl

    def _place_reduce_market(self, side: str, qty: float, reason: str) -> str:
        capped_qty = self._cap_reduce_order_qty(side, qty)
        if capped_qty < self.min_qty:
            logger.warning(
                "Skipped reduce-only market order with no grid allowance symbol=%s side=%s requested=%s reason=%s",
                self.config.get("symbol"),
                side,
                qty,
                reason,
            )
            return ""

        qty_text = self._fq(capped_qty)
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
        self._apply_market_reduce_to_grid_position(side, float(qty_text))
        self.trigger_message = f"Safety repair placed {side} reduce-only {qty_text}: {reason}"
        logger.warning(self.trigger_message)
        self._mark_fast_poll()
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
        self.initial_qty = stats["qty"]
        self.initial_entry_price = stats["price"]
        self._set_initial_grid_position(side, stats["qty"])
        self._record_trade_value(
            stats["price"],
            stats["qty"],
            volume=stats["volume"],
            fee=stats["fee"],
            fee_asset=stats["fee_asset"],
            fee_source=stats["fee_source"],
        )
        self._mark_fast_poll()
        self._persist_state()

    def _warn_reduce_limit_unplaced(self, side: str, price_text: str, qty: str, reason: str):
        self.trigger_message = (
            f"Reduce-only limit protection was not placed for {side} {qty} at {price_text}; "
            "keeping the lot ledger and retrying/reconciling without market-closing."
        )
        logger.warning(
            "Reduce-only limit protection not placed symbol=%s side=%s price=%s qty=%s reason=%s",
            self.config.get("symbol"),
            side,
            price_text,
            qty,
            reason,
        )
        self._mark_fast_poll()
        self._persist_state()

    def _place_limit_open(self, side: str, qty: float, price: float, *, post_only: bool = True):
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
            time_in_force="PostOnly" if post_only else None,
        )
        if result.get("retCode") != 0:
            raise RuntimeError(result.get("retMsg", "Failed to place initial limit order"))

        self.initial_side = side
        self.initial_qty = float(qty_text)
        self.initial_entry_price = 0.0
        self.waiting_initial_order = True
        order_label = "post-only" if post_only else "limit"
        self.trigger_message = f"Waiting for {order_label} opening order at {price_text}"
        self.opening_order = {
            "link_id": link_id,
            "order_id": result["result"]["orderId"],
            "side": side,
            "price": price_text,
            "qty": qty_text,
            "order_type": "Limit",
            "time_in_force": "PostOnly" if post_only else "GTC",
            "reduce_only": False,
        }
        self._mark_fast_poll()
        self._persist_state()

    def _initial_limit_price(self, side: str, current_price: float, *, post_only: bool = True) -> float:
        tick = float(self.tick_size)
        maker_safe_price = current_price - tick if side == "Buy" else current_price + tick
        configured_price = self.config.get("initial_order_price")
        if configured_price is not None and not post_only:
            return float(configured_price)

        if configured_price is not None:
            configured_price = float(configured_price)
            if side == "Buy" and configured_price < current_price:
                return configured_price
            if side == "Sell" and configured_price > current_price:
                return configured_price
            logger.warning(
                "Initial post-only price would cross market symbol=%s side=%s configured=%s current=%s; using maker-safe price=%s",
                self.config.get("symbol"),
                side,
                configured_price,
                current_price,
                maker_safe_price,
            )

        return maker_safe_price

    def _place(
        self,
        side: str,
        price: float,
        level_idx: int,
        reduce_only: bool,
        qty_override: float | None = None,
        entry_price: float | None = None,
        allow_duplicate: bool = False,
    ) -> Optional[str]:
        raw_qty = float(qty_override) if qty_override is not None else float(self.config["qty_per_grid"])
        if reduce_only:
            capped_qty = self._cap_reduce_order_qty(side, raw_qty)
            if capped_qty < self.min_qty:
                logger.warning(
                    "Skipped reduce-only order with no grid allowance symbol=%s side=%s level=%s requested=%s",
                    self.config.get("symbol"),
                    side,
                    level_idx,
                    raw_qty,
                )
                return None
            raw_qty = capped_qty

        qty = self._fq(raw_qty)
        price_text = self._fp(price)
        link_id = f"g_{level_idx}_{side[0]}_{uuid.uuid4().hex[:6]}"

        if not allow_duplicate and self._has_active_order(side, level_idx, reduce_only):
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
                    self._warn_reduce_limit_unplaced(side, price_text, qty, str(retry_exc))
                    return None
            else:
                if reduce_only:
                    self._warn_reduce_limit_unplaced(side, price_text, qty, str(exc))
                    return None
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
                self._warn_reduce_limit_unplaced(side, price_text, qty, str(retry_exc))
                return None

        if result.get("retCode") != 0 and reduce_only:
            self._warn_reduce_limit_unplaced(side, price_text, qty, str(result.get("retMsg") or "rejected"))
            return None

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
            "processed_fill_qty": 0.0,
            "processed_fill_volume": 0.0,
            "processed_fill_fee": 0.0,
        }
        self._mark_fast_poll()
        self._persist_state()
        return link_id

    def _supports_batch_orders(self) -> bool:
        # Post Only grids need per-order rejection handling, so keep them on the safer path.
        return hasattr(self.client, "place_orders") and not bool(self.config.get("grid_order_post_only", False))

    def _place_batch_limit_orders(self, order_specs: list[dict[str, Any]]) -> list[str]:
        planned = []
        reduce_remaining_by_side: dict[str, float] = {}

        for spec in order_specs:
            side = str(spec["side"])
            level_idx = int(spec["level_idx"])
            reduce_only = bool(spec.get("reduce_only", False))
            allow_duplicate = bool(spec.get("allow_duplicate", False))
            if not allow_duplicate and self._has_active_order(side, level_idx, reduce_only):
                continue
            if self._stopping:
                break

            raw_qty = float(spec.get("qty_override") or self.config["qty_per_grid"])
            if reduce_only:
                remaining = reduce_remaining_by_side.get(side)
                if remaining is None:
                    remaining = self._available_reduce_qty(side)
                raw_qty = min(raw_qty, remaining)
                reduce_remaining_by_side[side] = max(0.0, remaining - raw_qty)
                if raw_qty < self.min_qty:
                    logger.warning(
                        "Skipped batch reduce-only order with no grid allowance symbol=%s side=%s level=%s",
                        self.config.get("symbol"),
                        side,
                        level_idx,
                    )
                    continue

            qty = self._fq(raw_qty)
            price_text = self._fp(float(spec["price"]))
            link_id = f"g_{level_idx}_{side[0]}_{uuid.uuid4().hex[:6]}"
            planned.append(
                {
                    "request": {
                        "symbol": self.config["symbol"],
                        "side": side,
                        "qty": qty,
                        "price": price_text,
                        "order_type": "Limit",
                        "reduce_only": reduce_only,
                        "order_link_id": link_id,
                        "time_in_force": None,
                    },
                    "state": {
                        "link_id": link_id,
                        "level_idx": level_idx,
                        "side": side,
                        "price": price_text,
                        "qty": qty,
                        "reduce_only": reduce_only,
                        "entry_price": spec.get("entry_price"),
                    },
                    "fallback": spec,
                }
            )

        placed_links: list[str] = []
        for start in range(0, len(planned), BATCH_ORDER_CHUNK_SIZE):
            chunk = planned[start : start + BATCH_ORDER_CHUNK_SIZE]
            if self._stopping:
                break
            try:
                result = self.client.place_orders([item["request"] for item in chunk])
                if result.get("retCode") != 0:
                    raise RuntimeError(result.get("retMsg", "Batch order failed"))
                result_items = result.get("result", {}).get("list", [])
                if len(result_items) != len(chunk):
                    raise RuntimeError("Batch order response size mismatch")
            except Exception as exc:
                logger.warning(
                    "Batch order placement failed; falling back to single orders symbol=%s msg=%s",
                    self.config.get("symbol"),
                    exc,
                )
                with contextlib.suppress(Exception):
                    self._reconcile_exchange_open_orders()
                for item in chunk:
                    fallback = item["fallback"]
                    link_id = self._place(
                        str(fallback["side"]),
                        float(fallback["price"]),
                        int(fallback["level_idx"]),
                        reduce_only=bool(fallback.get("reduce_only", False)),
                        qty_override=float(fallback.get("qty_override") or self.config["qty_per_grid"]),
                        entry_price=fallback.get("entry_price"),
                        allow_duplicate=bool(fallback.get("allow_duplicate", False)),
                    )
                    if link_id:
                        placed_links.append(link_id)
                continue

            for item, order_result in zip(chunk, result_items):
                state = item["state"]
                if order_result.get("retCode") == 0 and order_result.get("result", {}).get("orderId"):
                    link_id = state["link_id"]
                    self.active_orders[link_id] = {
                        "link_id": link_id,
                        "order_id": str(order_result["result"]["orderId"]),
                        "level_idx": state["level_idx"],
                        "side": state["side"],
                        "price": state["price"],
                        "qty": state["qty"],
                        "status": "open",
                        "order_type": "Limit",
                        "time_in_force": "GTC",
                        "reduce_only": state["reduce_only"],
                        "entry_price": state["entry_price"],
                        "processed_fill_qty": 0.0,
                        "processed_fill_volume": 0.0,
                        "processed_fill_fee": 0.0,
                    }
                    placed_links.append(link_id)
                    continue

                fallback = item["fallback"]
                logger.warning(
                    "Batch order item failed; falling back to single order symbol=%s side=%s price=%s msg=%s",
                    self.config.get("symbol"),
                    fallback.get("side"),
                    fallback.get("price"),
                    order_result.get("retMsg"),
                )
                link_id = self._place(
                    str(fallback["side"]),
                    float(fallback["price"]),
                    int(fallback["level_idx"]),
                    reduce_only=bool(fallback.get("reduce_only", False)),
                    qty_override=float(fallback.get("qty_override") or self.config["qty_per_grid"]),
                    entry_price=fallback.get("entry_price"),
                    allow_duplicate=bool(fallback.get("allow_duplicate", False)),
                )
                if link_id:
                    placed_links.append(link_id)

        if placed_links:
            self._mark_fast_poll()
            self._persist_state()
        return placed_links

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

        self.waiting_trigger = False
        self.trigger_message = ""

        open_side = ""
        if direction == "long":
            open_side = "Buy"
        elif direction == "short":
            open_side = "Sell"

        initial_order_type = str(self.config.get("initial_order_type", "market")).lower().strip()
        limit_open_price = None
        reference_price = current_price
        if direction in {"long", "short"} and initial_order_type in {"post_only", "limit"}:
            limit_open_price = self._initial_limit_price(
                open_side,
                current_price,
                post_only=initial_order_type == "post_only",
            )
            reference_price = limit_open_price

        range_check_price = reference_price if limit_open_price is not None else current_price
        range_label = "Reference price" if limit_open_price is not None else "Current price"
        if not (levels[0] < range_check_price < levels[-1]):
            raise RuntimeError(
                f"{range_label} {range_check_price} must stay inside the configured range "
                f"{levels[0]} - {levels[-1]}"
            )

        if direction in {"long", "short"}:
            self._capture_baseline_position(open_side)

        total_qty = self._prepare_pending_targets(reference_price)

        if direction in {"long", "short"}:
            if initial_order_type in {"post_only", "limit"}:
                self._place_limit_open(
                    open_side,
                    total_qty,
                    limit_open_price,
                    post_only=initial_order_type == "post_only",
                )
                return
            self._place_market_open(open_side, total_qty)
            self._prepare_pending_targets(self.initial_entry_price or current_price, self.initial_qty)
            self._reset_reduce_lots_from_pending_targets(self.initial_entry_price or current_price)
            self._deploy_pending_targets()

        if direction == "neutral":
            self._deploy_pending_targets()

        self.grid_ready = True
        self._persist_state()

    def _target_orders_for_price(self, reference_price: float) -> tuple[list[tuple[int, float, str]], list[tuple[int, float, str]]]:
        levels = self.grid_levels
        direction = self.config["direction"]
        if direction == "long":
            profit_targets = [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > reference_price
            ]
            add_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < reference_price
            ]
        elif direction == "short":
            profit_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < reference_price
            ]
            add_targets = [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > reference_price
            ]
        else:
            profit_targets = []
            add_targets = [
                (idx, levels[idx], "Buy")
                for idx in range(len(levels) - 1)
                if levels[idx] < reference_price
            ] + [
                (idx, levels[idx + 1], "Sell")
                for idx in range(len(levels) - 1)
                if levels[idx + 1] > reference_price
            ]
        return profit_targets, add_targets

    def _prepare_pending_targets(self, reference_price: float, total_qty_override: float | None = None) -> float:
        direction = self.config["direction"]
        profit_targets, add_targets = self._target_orders_for_price(reference_price)
        target_count = len(profit_targets) if direction in {"long", "short"} else len(add_targets)
        if target_count <= 0:
            raise RuntimeError("No valid grid targets were found around current price")

        sizing_mode = self._position_sizing_mode()
        if total_qty_override is not None:
            raw_total_qty = float(total_qty_override)
            total_steps = self._qty_to_steps(raw_total_qty)
            allocated_qtys = self._allocate_qtys(raw_total_qty, target_count)
        elif sizing_mode == "fixed_grid_qty":
            per_grid_qty = float(self.config.get("grid_order_qty") or 0)
            per_grid_steps = self._qty_to_steps(per_grid_qty)
            if per_grid_steps <= 0:
                raise RuntimeError("grid_order_qty is too small for this symbol")
            allocated_qtys = [self._steps_to_qty(per_grid_steps) for _ in range(target_count)]
            total_steps = per_grid_steps * target_count
            raw_total_qty = self._steps_to_qty(total_steps)
        else:
            raw_total_qty = self._calc_total_qty(reference_price)
            total_steps = self._qty_to_steps(raw_total_qty)
            allocated_qtys = self._allocate_qtys(raw_total_qty, target_count)
        if total_steps < target_count:
            raise RuntimeError("Total investment is too small for this symbol and grid count")

        total_qty = self._steps_to_qty(total_steps)
        qty_per_grid = total_qty / target_count
        fallback_steps = self._qty_to_steps(qty_per_grid)
        fallback_qty = self._steps_to_qty(max(1, fallback_steps))
        target_qty_by_level: dict[str, float] = {}
        if direction in {"long", "short"}:
            for target, allocated_qty in zip(profit_targets, allocated_qtys):
                target_qty_by_level[str(target[0])] = allocated_qty
            for target in add_targets:
                target_qty_by_level.setdefault(str(target[0]), fallback_qty)
        else:
            for target, allocated_qty in zip(add_targets, allocated_qtys):
                target_qty_by_level[str(target[0])] = allocated_qty

        self.config["active_grid_count"] = target_count
        self.config["derived_total_qty"] = total_qty
        self.config["qty_per_grid"] = qty_per_grid
        self.target_qty_by_level = target_qty_by_level

        self._pending_targets = {
            "profit_targets": profit_targets,
            "add_targets": add_targets,
            "allocated_qtys": allocated_qtys,
            "allocated_qty_by_level": target_qty_by_level,
            "qty_per_grid": qty_per_grid,
        }
        self._persist_state()
        return total_qty

    def _deploy_pending_targets(self, qty_scale: float = 1.0):
        if not self._pending_targets:
            raise RuntimeError("No pending grid targets were prepared")

        direction = self.config["direction"]
        profit_targets = self._pending_targets["profit_targets"]
        add_targets = self._pending_targets["add_targets"]
        allocated_qtys = [qty * qty_scale for qty in self._pending_targets["allocated_qtys"]]
        qty_per_grid = self._pending_targets["qty_per_grid"] * qty_scale
        allocated_qty_by_level = {
            int(level_idx): float(qty) * qty_scale
            for level_idx, qty in (self._pending_targets.get("allocated_qty_by_level") or {}).items()
        }

        def qty_for_level(level_idx: int) -> float:
            return allocated_qty_by_level.get(level_idx, qty_per_grid)

        batch_specs: list[dict[str, Any]] = []

        def deploy_or_queue(
            side: str,
            price: float,
            level_idx: int,
            *,
            reduce_only: bool,
            qty_override: float,
            entry_price: float | None = None,
        ):
            if self._supports_batch_orders():
                batch_specs.append(
                    {
                        "side": side,
                        "price": price,
                        "level_idx": level_idx,
                        "reduce_only": reduce_only,
                        "qty_override": qty_override,
                        "entry_price": entry_price,
                    }
                )
                return
            self._place(
                side,
                price,
                level_idx,
                reduce_only=reduce_only,
                qty_override=qty_override,
                entry_price=entry_price,
            )

        if direction in {"long", "short"}:
            for target, allocated_qty in zip(profit_targets, allocated_qtys):
                idx, target_price, target_side = target
                deploy_or_queue(
                    target_side,
                    target_price,
                    idx,
                    reduce_only=True,
                    qty_override=allocated_qty,
                    entry_price=self.initial_entry_price,
                )

            for idx, target_price, target_side in add_targets:
                deploy_or_queue(
                    target_side,
                    target_price,
                    idx,
                    reduce_only=False,
                    qty_override=qty_for_level(idx),
                )
        else:
            for target, allocated_qty in zip(add_targets, allocated_qtys):
                idx, target_price, target_side = target
                deploy_or_queue(
                    target_side,
                    target_price,
                    idx,
                    reduce_only=False,
                    qty_override=allocated_qty,
                )

        if batch_specs:
            self._place_batch_limit_orders(batch_specs)

        self.grid_ready = True
        self.waiting_initial_order = False
        self.trigger_message = ""
        self._pending_targets = None
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
        direction = self.config["direction"]
        if direction in {"long", "short"}:
            position_side = "Buy" if direction == "long" else "Sell"
            close_side = "Sell" if position_side == "Buy" else "Buy"
            size = self._safe_grid_close_qty(position_side)
            if size < self.min_qty:
                return
            result = self.client.place_order(
                symbol=self.config["symbol"],
                side=close_side,
                qty=self._fq(size),
                order_type="Market",
                reduce_only=True,
                order_link_id=f"close_{close_side[0]}_{uuid.uuid4().hex[:6]}",
            )
            if result.get("retCode") != 0:
                raise RuntimeError(result.get("retMsg", "Failed to close grid position"))
            self.grid_position_net_qty = 0.0
            return

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
                    self._resume_paused_replacements()
                    self._reconcile_grid_position_protection()

                await self._sleep_until_next_poll()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Grid polling failed: %s", exc)
                await asyncio.sleep(1 if self.waiting_initial_order else 5)

    def _repair_boundary_position(self):
        if not bool(self.config.get("boundary_market_repair", False)):
            return

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

        position_qty = self._safe_grid_close_qty(position_side)
        if position_qty < self.min_qty:
            return

        self._boundary_repair_in_progress = True
        try:
            self._cancel_stale_reduce_orders(close_side)
            refreshed_qty = self._safe_grid_close_qty(position_side)
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

    def _replace_unfilled_opening_order(self, status: str):
        order = self.opening_order or {}
        side = order.get("side") or self.initial_side
        qty = float(order.get("qty") or self.initial_qty or 0)
        if not side or qty < self.min_qty:
            self.waiting_initial_order = False
            self.opening_order = None
            self.running = False
            self.trigger_message = "Opening order closed without fills and retry quantity is too small."
            self._persist_state()
            return

        current_price = self._get_current_price()
        self.current_price = current_price
        if not self._in_grid_range(current_price):
            self.waiting_initial_order = False
            self.opening_order = None
            self.running = False
            self.trigger_message = (
                "Opening order closed without fills and price is outside grid range; "
                "please review before restarting."
            )
            self._persist_state()
            return

        self.waiting_initial_order = False
        self.opening_order = None
        post_only = str(order.get("time_in_force") or "") == "PostOnly"
        retry_price = self._initial_limit_price(side, current_price, post_only=post_only)
        self._place_limit_open(side, qty, retry_price, post_only=post_only)
        order_label = "Post-only" if post_only else "Limit"
        self.trigger_message = (
            f"{order_label} opening order ended as {status} without fills; "
            f"replaced at {self.opening_order['price']}."
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
        snapshot = self._get_order_snapshot(self.opening_order)
        status = self._order_status_from_snapshot(snapshot)
        liquidity_hint = self._order_liquidity_hint(self.opening_order)
        stats = self._get_trade_stats(
            order_id,
            fallback_price,
            planned_qty,
            allow_estimate=False,
            liquidity_hint=liquidity_hint,
        )
        if not stats or stats["qty"] <= 0:
            stats = self._execution_stats_from_order_snapshot(
                snapshot,
                fallback_price,
                planned_qty,
                liquidity_hint=liquidity_hint,
            )
        if not stats or stats["qty"] <= 0:
            if status == "UNKNOWN":
                logger.info(
                    "Opening order absent from open orders but status is unknown; waiting symbol=%s order_id=%s",
                    self.config.get("symbol"),
                    order_id,
                )
                self._mark_fast_poll()
                return
            if self._is_cancelled_status(status) or self._is_filled_status(status):
                self._replace_unfilled_opening_order(status)
                return
            logger.info(
                "Opening order absent from open orders but not terminal symbol=%s order_id=%s status=%s",
                self.config.get("symbol"),
                order_id,
                status,
            )
            self._mark_fast_poll()
            return

        if self._is_partial_status(status):
            self.trigger_message = (
                f"Opening order partially filled {self._fq(stats['qty'])}/{self._fq(planned_qty)}; "
                "waiting for final order status before deploying grid."
            )
            self._mark_fast_poll()
            self._persist_state()
            return

        qty_scale = stats["qty"] / planned_qty if planned_qty > 0 else 0
        if qty_scale <= 0:
            self.waiting_initial_order = False
            self.running = False
            self.trigger_message = "Opening order fill quantity is too small; please restart the grid."
            return

        try:
            self._prepare_pending_targets(stats["price"], stats["qty"])
        except RuntimeError as exc:
            self.waiting_initial_order = False
            self.opening_order = None
            self.running = False
            self.trigger_message = str(exc)
            self._persist_state()
            return
        allocated_qtys = self._pending_targets["allocated_qtys"] if self._pending_targets else []
        if allocated_qtys and min(allocated_qtys) < self.min_qty:
            self.waiting_initial_order = False
            self.running = False
            self.trigger_message = "Opening order partial fill is too small for grid allocation."
            return

        self.initial_qty = stats["qty"]
        self.initial_entry_price = stats["price"]
        self._set_initial_grid_position(self.initial_side, stats["qty"])
        self._reset_reduce_lots_from_pending_targets(stats["price"])
        self._record_trade_value(
            stats["price"],
            stats["qty"],
            volume=stats["volume"],
            fee=stats["fee"],
            fee_asset=stats["fee_asset"],
            fee_source=stats["fee_source"],
        )
        self.opening_order = None
        self._deploy_pending_targets()
        self._persist_state()

    async def _check_fills(self):
        # Counter orders can be placed and filled before the next polling tick.
        # Reconcile a few rounds so protection checks see a stable order ledger.
        for _ in range(3):
            changed = self._reconcile_exchange_open_orders()
            if not changed or self._stopping:
                break

    def _handle_closed_order(self, order: dict, *, allow_estimate: bool | None = None) -> bool:
        fallback_qty = float(order["qty"])
        fallback_price = float(order["price"])
        if allow_estimate is None:
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

        self._record_execution_delta(order, stats)
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
        return "maker" if order.get("time_in_force") == "PostOnly" else "taker"

    @staticmethod
    def _float_field(item: dict, *keys: str) -> float:
        for key in keys:
            value = item.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _order_status_from_snapshot(snapshot: dict | None) -> str:
        if not snapshot:
            return "UNKNOWN"
        status = snapshot.get("orderStatus") or snapshot.get("status")
        return str(status or "UNKNOWN").upper()

    def _get_order_status(self, order: dict) -> str:
        return self._order_status_from_snapshot(self._get_order_snapshot(order))

    def _get_order_snapshot(self, order: dict) -> dict:
        if not hasattr(self.client, "get_order"):
            return self._get_order_from_history(order)
        try:
            resp = self.client.get_order(self.config["symbol"], str(order.get("order_id", "")))
        except Exception as exc:
            logger.warning("Fetch order status failed order_id=%s msg=%s", order.get("order_id"), exc)
            return self._get_order_from_history(order)
        if resp.get("retCode") != 0:
            logger.warning(
                "Fetch order status rejected order_id=%s msg=%s",
                order.get("order_id"),
                resp.get("retMsg"),
            )
            return self._get_order_from_history(order)

        snapshot = resp.get("result", {}) or {}
        if self._order_status_from_snapshot(snapshot) == "UNKNOWN":
            history_snapshot = self._get_order_from_history(order)
            if history_snapshot:
                return history_snapshot
        return snapshot

    def _get_order_status_from_history(self, order: dict) -> str:
        return self._order_status_from_snapshot(self._get_order_from_history(order))

    def _get_order_from_history(self, order: dict) -> dict:
        if not hasattr(self.client, "get_order_history"):
            return {}

        order_id = str(order.get("order_id", "") or "")
        link_id = str(order.get("link_id", "") or "")
        try:
            resp = self.client.get_order_history(self.config["symbol"], limit=1000)
        except Exception as exc:
            logger.warning("Fetch order history failed order_id=%s msg=%s", order_id, exc)
            return {}
        if resp.get("retCode") != 0:
            logger.warning(
                "Fetch order history rejected order_id=%s msg=%s",
                order_id,
                resp.get("retMsg"),
            )
            return {}

        for item in resp.get("result", {}).get("list", []):
            if order_id and str(item.get("orderId", "") or "") == order_id:
                return item
            if link_id and str(item.get("orderLinkId", "") or item.get("order_link_id", "") or "") == link_id:
                return item
        return {}

    def _execution_stats_from_order_snapshot(
        self,
        snapshot: dict,
        fallback_price: float,
        fallback_qty: float,
        *,
        liquidity_hint: str,
    ) -> dict | None:
        if not snapshot:
            return None

        status = self._order_status_from_snapshot(snapshot)
        qty = self._float_field(snapshot, "executedQty", "cumExecQty", "cumQty", "cum_exec_qty")
        if qty <= 0 and self._is_filled_status(status):
            qty = fallback_qty
        if qty <= 0:
            return None

        volume = self._float_field(snapshot, "cumQuote", "cumExecValue", "cum_exec_value", "volume")
        price = self._float_field(snapshot, "avgPrice", "avg_price", "averagePrice")
        if price <= 0 and volume > 0:
            price = volume / qty
        if price <= 0:
            price = fallback_price
        if volume <= 0:
            volume = price * qty

        return {
            "price": price,
            "qty": qty,
            "volume": volume,
            "fee": self._estimate_fee(volume, liquidity_hint),
            "fee_asset": "USDT estimated",
            "fee_source": "estimated",
            "maker_count": 1 if liquidity_hint == "maker" else 0,
            "taker_count": 1 if liquidity_hint != "maker" else 0,
        }

    @staticmethod
    def _is_filled_status(status: str) -> bool:
        return status == "FILLED"

    @staticmethod
    def _is_partial_status(status: str) -> bool:
        return status in {"PARTIALLY_FILLED", "FILLED_PARTIALLY", "PARTIAL_FILLED"}

    @staticmethod
    def _is_cancelled_status(status: str) -> bool:
        return status in {
            "CANCELED",
            "CANCELLED",
            "REJECTED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "DEACTIVATED",
        }

    def _replace_cancelled_order(self, order: dict):
        placed = self._place(
            order["side"],
            float(order["price"]),
            int(order["level_idx"]),
            reduce_only=bool(order["reduce_only"]),
            qty_override=float(order["qty"]),
            entry_price=order.get("entry_price"),
            allow_duplicate=bool(order["reduce_only"]),
        )
        if placed:
            logger.warning(
                "Replaced cancelled grid order symbol=%s old_order_id=%s new_link_id=%s",
                self.config.get("symbol"),
                order.get("order_id"),
                placed,
            )

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
        self._record_reduce_lot_fill(order, qty, price)
        self._apply_grid_position_fill(order, qty)

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
        requested_qty = float(qty)
        if self._position_sizing_mode() == "fixed_grid_qty" or not reduce_only:
            deficit = self._active_order_qty_deficit(side, level_idx, reduce_only, requested_qty)
            if deficit <= 0:
                return True
            qty = deficit
        return (
            self._place(
                side,
                price,
                level_idx,
                reduce_only=reduce_only,
                qty_override=qty,
                entry_price=entry_price,
                allow_duplicate=bool(reduce_only) or qty < requested_qty,
            )
            is not None
        )

    def _counter_order_reduces_grid_position(self, order: dict) -> bool:
        direction = self.config["direction"]
        side = order.get("side")
        level_idx = int(order.get("level_idx", 0) or 0)
        if direction == "long":
            return side == "Buy" and level_idx + 1 < len(self.grid_levels)
        if direction == "short":
            return side == "Sell"
        return False

    def _place_counter_order(self, order: dict) -> bool:
        direction = self.config["direction"]
        side = order["side"]
        level_idx = order["level_idx"]
        qty = self._counter_qty_for_order(order)

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
