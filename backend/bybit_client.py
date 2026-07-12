import hashlib
import hmac
import json
import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

import requests

from exchange_errors import (
    ExchangeRateLimitError,
    ExchangeRequestUncertainError,
    is_exchange_rate_limit_message,
)
from fee_rates import fee_rate_response


class BybitClient:
    exchange = "bybit"
    ASSET_PRICE_TTL_SECONDS = 60
    HISTORICAL_ASSET_PRICE_CACHE_MAX_ITEMS = 2048
    FEE_RATE_TTL_SECONDS = 300
    MAX_PAGINATION_PAGES = 100
    RATE_LIMIT_DEFAULT_RETRY_SECONDS = 60.0
    IP_RATE_LIMIT_RETRY_SECONDS = 600.0

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.recv_window = "5000"
        self._asset_price_cache: dict[str, Decimal | tuple[Decimal, float]] = {}
        self._historical_asset_price_cache: dict[tuple[str, int], Decimal] = {}
        self._fee_rate_cache: dict[str, tuple[str, str, int, float]] = {}
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0.0

    def _rate_limit_remaining(self) -> float:
        with self._rate_limit_lock:
            return max(0.0, self._rate_limit_until - time.time())

    def _activate_rate_limit(
        self,
        message: str,
        response: Any | None = None,
        *,
        minimum_retry_seconds: float | None = None,
    ) -> float:
        retry_after = max(
            self.RATE_LIMIT_DEFAULT_RETRY_SECONDS,
            float(minimum_retry_seconds or 0),
        )
        headers = getattr(response, "headers", {}) or {}
        try:
            retry_after = max(retry_after, float(headers.get("Retry-After", 0) or 0))
        except (TypeError, ValueError):
            pass
        with self._rate_limit_lock:
            self._rate_limit_until = max(self._rate_limit_until, time.time() + retry_after)
        return retry_after

    def _raise_if_rate_limited(self):
        remaining = self._rate_limit_remaining()
        if remaining > 0:
            raise ExchangeRateLimitError(
                "Bybit request paused after an exchange rate-limit rejection",
                retry_after=remaining,
            )

    def _sign(self, payload: str, timestamp: str) -> str:
        raw = f"{timestamp}{self.api_key}{self.recv_window}{payload}"
        return hmac.new(
            self.api_secret.encode("utf-8"),
            raw.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _headers(self, payload: str = "") -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": self._sign(payload, timestamp),
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: str = "",
        payload: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict:
        self._raise_if_rate_limited()
        url = f"{self.base_url}{path}"
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
        headers = self._headers(body if method == "POST" else params) if auth else None

        if method == "GET":
            if params:
                url = f"{url}?{params}"
            response = requests.get(url, headers=headers, timeout=10)
        else:
            response = requests.post(url, headers=headers, data=body, timeout=10)

        if (
            response.status_code == 403
            and "access too frequent" in str(response.text or "").lower()
        ):
            retry_after = self._activate_rate_limit(
                "Bybit IP rate limit reached: access too frequent",
                response,
                minimum_retry_seconds=self.IP_RATE_LIMIT_RETRY_SECONDS,
            )
            raise ExchangeRateLimitError(
                "Bybit IP rate limit reached: access too frequent",
                retry_after=retry_after,
            )
        if response.status_code == 429:
            try:
                data = response.json()
            except ValueError:
                data = {}
            message = data.get("retMsg") if isinstance(data, dict) else ""
            retry_after = self._activate_rate_limit(
                str(message or "Bybit rate limit reached"), response
            )
            raise ExchangeRateLimitError(
                str(message or "Bybit rate limit reached"),
                retry_after=retry_after,
            )
        if response.status_code == 408 or response.status_code >= 500:
            try:
                data = response.json()
            except ValueError:
                data = {}
            message = data.get("retMsg") if isinstance(data, dict) else ""
            raise ExchangeRequestUncertainError(
                message or f"Bybit request status unknown after HTTP {response.status_code}"
            )
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError as exc:
            message = f"Bybit returned invalid JSON for {path}"
            if method.upper() != "GET":
                raise ExchangeRequestUncertainError(
                    f"{message}; request status unknown"
                ) from exc
            raise RuntimeError(message) from exc
        if not isinstance(data, dict):
            message = f"Bybit returned an invalid response structure for {path}"
            if method.upper() != "GET":
                raise ExchangeRequestUncertainError(
                    f"{message}; request status unknown"
                )
            raise RuntimeError(message)
        if data.get("retCode") in {429, 10006, 10018} or is_exchange_rate_limit_message(
            data.get("retMsg")
        ):
            retry_after = self._activate_rate_limit(
                str(data.get("retMsg") or "Bybit rate limit reached")
            )
            raise ExchangeRateLimitError(
                str(data.get("retMsg") or "Bybit rate limit reached"),
                retry_after=retry_after,
            )
        if data.get("retCode") in {10000, 10016}:
            raise ExchangeRequestUncertainError(
                str(data.get("retMsg") or "Bybit server timeout; request status unknown")
            )
        return data

    def get_ticker(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/market/tickers",
            params=f"category=linear&symbol={symbol}",
        )

    def get_instrument_info(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/market/instruments-info",
            params=f"category=linear&symbol={symbol}",
        )

    def get_balance(self) -> dict:
        return self._request(
            "GET",
            "/v5/account/wallet-balance",
            params="accountType=UNIFIED",
            auth=True,
        )

    def set_leverage(self, symbol: str, leverage: str) -> dict:
        return self._request(
            "POST",
            "/v5/position/set-leverage",
            payload={
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": leverage,
                "sellLeverage": leverage,
            },
            auth=True,
        )

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: str,
        price: str | None = None,
        order_type: str = "Limit",
        reduce_only: bool = False,
        order_link_id: str = "",
        time_in_force: str | None = None,
    ) -> dict:
        payload: dict[str, Any] = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty,
            "reduceOnly": reduce_only,
        }
        if order_type == "Limit":
            payload["price"] = price
            payload["timeInForce"] = time_in_force or "GTC"
        if order_link_id:
            payload["orderLinkId"] = order_link_id
        return self._request("POST", "/v5/order/create", payload=payload, auth=True)

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        return self._request(
            "POST",
            "/v5/order/cancel",
            payload={"category": "linear", "symbol": symbol, "orderId": order_id},
            auth=True,
        )

    def cancel_all_orders(self, symbol: str) -> dict:
        return self._request(
            "POST",
            "/v5/order/cancel-all",
            payload={"category": "linear", "symbol": symbol},
            auth=True,
        )

    def get_fee_rates(self, symbol: str) -> dict:
        symbol = symbol.upper()
        cached = self._fee_rate_cache.get(symbol)
        cache_clock = time.monotonic()
        if cached and cache_clock - cached[3] < self.FEE_RATE_TTL_SECONDS:
            return fee_rate_response(
                symbol,
                cached[0],
                cached[1],
                source="exchange_cache",
                fetched_at=cached[2],
            )

        data = self._request(
            "GET",
            "/v5/account/fee-rate",
            params=f"category=linear&symbol={symbol}",
            auth=True,
        )
        if data.get("retCode") != 0:
            raise RuntimeError(str(data.get("retMsg") or f"Bybit fee rate query failed for {symbol}"))
        items = data.get("result", {}).get("list") or []
        if not items:
            raise RuntimeError(f"Bybit returned no fee rate for {symbol}")
        if (
            len(items) != 1
            or not isinstance(items[0], dict)
            or str(items[0].get("symbol") or "").upper() != symbol
        ):
            raise RuntimeError(f"Bybit fee rate response symbol is ambiguous for {symbol}")
        fetched_at = int(time.time() * 1000)
        response = fee_rate_response(
            symbol,
            items[0].get("makerFeeRate"),
            items[0].get("takerFeeRate"),
            source="exchange",
            fetched_at=fetched_at,
        )
        result = response["result"]
        self._fee_rate_cache[symbol] = (
            result["makerFeeRate"],
            result["takerFeeRate"],
            fetched_at,
            cache_clock,
        )
        return response

    def _get_paginated(
        self,
        path: str,
        *,
        base_params: str,
        page_size: int,
        max_items: int | None = None,
    ) -> dict:
        items: list[dict] = []
        cursor = ""
        seen_cursors: set[str] = set()
        last_response: dict = {"retCode": 0, "result": {"list": []}}
        for _ in range(self.MAX_PAGINATION_PAGES):
            params = f"{base_params}&limit={page_size}"
            if cursor:
                params += f"&cursor={requests.utils.quote(cursor, safe='%')}"
            response = self._request("GET", path, params=params, auth=True)
            last_response = response
            if response.get("retCode") != 0:
                return response
            result = response.get("result", {}) or {}
            page_items = list(result.get("list", []) or [])
            items.extend(page_items)
            if max_items is not None and len(items) >= max_items:
                items = items[:max_items]
                break
            next_cursor = str(result.get("nextPageCursor", "") or "")
            if not next_cursor:
                break
            if next_cursor in seen_cursors:
                raise RuntimeError(f"Bybit pagination cursor did not advance for {path}")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            raise RuntimeError(f"Bybit pagination exceeded {self.MAX_PAGINATION_PAGES} pages for {path}")

        result = dict(last_response.get("result", {}) or {})
        result["list"] = items
        result["nextPageCursor"] = ""
        return {**last_response, "result": result}

    def get_open_orders(self, symbol: str) -> dict:
        return self._get_paginated(
            "/v5/order/realtime",
            base_params=f"category=linear&symbol={symbol}&openOnly=0",
            page_size=50,
        )

    def get_order(self, symbol: str, order_id: str) -> dict:
        resp = self._request(
            "GET",
            "/v5/order/realtime",
            params=f"category=linear&symbol={symbol}&orderId={order_id}",
            auth=True,
        )
        if resp.get("retCode") == 0 and resp.get("result", {}).get("list"):
            return {"retCode": 0, "result": resp["result"]["list"][0]}

        history = self._request(
            "GET",
            "/v5/order/history",
            params=f"category=linear&symbol={symbol}&orderId={order_id}",
            auth=True,
        )
        if history.get("retCode") != 0:
            return history
        items = history.get("result", {}).get("list", [])
        return {"retCode": 0, "result": items[0] if items else {}}

    def get_order_by_link(self, symbol: str, order_link_id: str) -> dict:
        resp = self._request(
            "GET",
            "/v5/order/realtime",
            params=(
                f"category=linear&symbol={symbol}&orderLinkId={order_link_id}"
            ),
            auth=True,
        )
        if resp.get("retCode") != 0:
            return resp
        items = resp.get("result", {}).get("list", [])
        if items:
            return {"retCode": 0, "result": items[0]}

        history = self._request(
            "GET",
            "/v5/order/history",
            params=(
                f"category=linear&symbol={symbol}&orderLinkId={order_link_id}&limit=1"
            ),
            auth=True,
        )
        if history.get("retCode") != 0:
            return history
        items = history.get("result", {}).get("list", [])
        return {"retCode": 0, "result": items[0] if items else {}}

    def get_positions(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/position/list",
            params=f"category=linear&symbol={symbol}",
            auth=True,
        )

    def get_order_history(self, symbol: str, limit: int = 50) -> dict:
        safe_limit = max(1, min(int(limit or 50), 1000))
        return self._get_paginated(
            "/v5/order/history",
            base_params=f"category=linear&symbol={symbol}",
            page_size=min(50, safe_limit),
            max_items=safe_limit,
        )

    def get_order_trades(self, symbol: str, order_id: str) -> dict:
        resp = self._get_paginated(
            "/v5/execution/list",
            base_params=f"category=linear&symbol={symbol}&orderId={order_id}",
            page_size=100,
        )
        if resp.get("retCode") != 0:
            return resp
        matching: dict[tuple[str, ...], dict[str, Any]] = {}
        for item in resp.get("result", {}).get("list", []):
            if not isinstance(item, dict):
                raise RuntimeError("Bybit order execution history contains an invalid row")
            if str(item.get("orderId", "")) != str(order_id):
                continue
            matching[self._execution_identity(item)] = item
        resp["result"]["list"] = [self._normalize_trade(item) for item in matching.values()]
        return resp

    def get_recent_trades(self, symbol: str, limit: int = 100) -> dict:
        resp = self._request(
            "GET",
            "/v5/execution/list",
            params=f"category=linear&symbol={symbol}&limit={limit}",
            auth=True,
        )
        if resp.get("retCode") != 0:
            return resp
        resp["result"]["list"] = [self._normalize_trade(item) for item in resp["result"].get("list", [])]
        return resp

    def _normalize_trade(self, item: dict[str, Any]) -> dict:
        fee_asset = str(item.get("feeCurrency") or item.get("feeCoin") or "USDT").upper()
        fee_amount = Decimal(str(item.get("execFee", "0")))
        fee_usdt, fee_usdt_source = self._fee_to_usdt_with_source(
            fee_amount,
            fee_asset,
            trade_time_ms=item.get("execTime"),
        )

        return {
            "orderId": str(item.get("orderId", "")),
            "tradeId": str(item.get("execId", "")),
            "side": item.get("side", ""),
            "price": item.get("execPrice", "0"),
            "qty": item.get("execQty", "0"),
            "volume": item.get("execValue", "0"),
            "fee": str(fee_amount),
            "feeAsset": fee_asset,
            "feeUsdt": "" if fee_usdt is None else str(fee_usdt),
            "feeUsdtSource": fee_usdt_source,
            "realizedPnl": item.get("execPnl", "0"),
            "isMaker": str(item.get("isMaker", "")).lower() == "true",
            "time": item.get("execTime", ""),
        }

    @staticmethod
    def _execution_identity(item: dict[str, Any]) -> tuple[str, ...]:
        execution_id = str(item.get("execId", "") or "")
        if execution_id:
            return ("id", execution_id)
        return (
            "shape",
            str(item.get("orderId", "") or ""),
            str(item.get("execTime", "") or ""),
            str(item.get("side", "") or ""),
            str(item.get("execPrice", "") or ""),
            str(item.get("execQty", "") or ""),
            str(item.get("execFee", "") or ""),
        )

    def _historical_fee_asset_price(self, symbol: str, trade_time_ms: int) -> Decimal | None:
        minute_start = trade_time_ms - (trade_time_ms % 60_000)
        key = (symbol, minute_start)
        cached = self._historical_asset_price_cache.get(key)
        if cached is not None:
            return cached

        try:
            response = self._request(
                "GET",
                "/v5/market/kline",
                params=(
                    f"category=linear&symbol={symbol}&interval=1&"
                    f"start={minute_start}&limit=1"
                ),
            )
            if response.get("retCode") != 0:
                return None
            row = response.get("result", {}).get("list", [])[0]
            if int(row[0]) != minute_start:
                return None
            price = Decimal(str(row[1]))
            if price <= 0:
                return None
        except Exception:
            return None

        self._historical_asset_price_cache[key] = price
        while len(self._historical_asset_price_cache) > self.HISTORICAL_ASSET_PRICE_CACHE_MAX_ITEMS:
            self._historical_asset_price_cache.pop(next(iter(self._historical_asset_price_cache)))
        return price

    def _fee_to_usdt_with_source(
        self,
        amount: Decimal,
        asset: str,
        *,
        trade_time_ms: Any = None,
    ) -> tuple[Decimal | None, str]:
        if amount == 0:
            return Decimal("0"), "exchange_zero"
        if asset in {"", "USDT", "USDC", "BUSD", "FDUSD", "USD"}:
            return amount, "quote_asset"

        symbol = f"{asset}USDT"
        try:
            timestamp = int(trade_time_ms or 0)
        except (TypeError, ValueError):
            timestamp = 0
        if timestamp > 0:
            price = self._historical_fee_asset_price(symbol, timestamp)
            if price is None:
                return None, "historical_price_unavailable"
            return amount * price, "historical_minute_open"

        now = time.time()
        cached = self._asset_price_cache.get(symbol)
        if isinstance(cached, tuple):
            cached_price, cached_at = cached
            if now - cached_at < self.ASSET_PRICE_TTL_SECONDS:
                return amount * cached_price, "current_ticker_cache"
        elif cached is not None:
            return amount * cached, "current_ticker_cache"

        try:
            ticker = self.get_ticker(symbol)
            price = Decimal(str(ticker["result"]["list"][0]["lastPrice"]))
            if price <= 0:
                raise ValueError("invalid fee asset price")
            self._asset_price_cache[symbol] = (price, now)
            return amount * price, "current_ticker"
        except Exception:
            if isinstance(cached, tuple):
                return amount * cached[0], "stale_current_ticker_cache"
            if cached is not None:
                return amount * cached, "stale_current_ticker_cache"
            return None, "current_price_unavailable"

    def _fee_to_usdt(
        self,
        amount: Decimal,
        asset: str,
        *,
        trade_time_ms: Any = None,
    ) -> Decimal | None:
        fee_usdt, _ = self._fee_to_usdt_with_source(
            amount,
            asset,
            trade_time_ms=trade_time_ms,
        )
        return fee_usdt

    @staticmethod
    def round_to_step(value: float, step: str) -> str:
        step_decimal = Decimal(step)
        value_decimal = Decimal(str(value))
        rounded = (
            (value_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN)
            * step_decimal
        )
        decimals = max(0, -step_decimal.as_tuple().exponent)
        return f"{rounded:.{decimals}f}"
