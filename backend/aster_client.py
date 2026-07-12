import copy
import json
import os
import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data

from exchange_errors import (
    ExchangeRateLimitError,
    ExchangeRequestUncertainError,
    is_exchange_rate_limit_message,
)
from fee_rates import fee_rate_response


class AsterFuturesClient:
    exchange = "aster"
    ASSET_PRICE_TTL_SECONDS = 60
    HISTORICAL_ASSET_PRICE_CACHE_MAX_ITEMS = 2048
    UNCERTAIN_EXECUTION_CODES = {-1006, -1007}
    TRADE_PAGE_LIMIT = 1000
    TRADE_PROBE_PADDING_MS = 5 * 60 * 1000
    TRADE_WINDOW_LIMIT_MS = (7 * 24 * 60 * 60 * 1000) - 1
    MAX_TRADE_HISTORY_QUERIES = 64
    FEE_RATE_TTL_SECONDS = 300
    RATE_LIMIT_DEFAULT_RETRY_SECONDS = 60.0
    LIST_RESPONSE_PATHS = frozenset(
        {
            "/fapi/v3/allOrders",
            "/fapi/v3/balance",
            "/fapi/v3/batchOrders",
            "/fapi/v3/klines",
            "/fapi/v3/openOrders",
            "/fapi/v3/positionRisk",
            "/fapi/v3/userTrades",
        }
    )

    def __init__(
        self,
        user: str,
        signer_private_key: str,
        testnet: bool = False,
        *,
        signer: str | None = None,
        base_url: str | None = None,
        include_user_param: bool | None = None,
    ):
        self.user = str(user or "").strip()
        self.signer_private_key = str(signer_private_key or "").strip()
        self.testnet = testnet
        self.signer = str(signer or os.getenv("ASTER_SIGNER") or "").strip()
        if not self.signer and self.signer_private_key:
            self.signer = Account.from_key(self.signer_private_key).address

        self.include_user_param = (
            str(os.getenv("ASTER_INCLUDE_USER_PARAM", "true")).strip().lower() not in {"0", "false", "no", "off"}
            if include_user_param is None
            else bool(include_user_param)
        )
        self.base_url = (
            base_url
            or os.getenv("ASTER_BASE_URL")
            or (
                "https://fapi.asterdex.com"
                if not testnet
                else os.getenv("ASTER_TESTNET_BASE_URL", "https://fapi.asterdex-testnet.com")
            )
        ).rstrip("/")
        self.session = requests.Session()
        self._asset_price_cache: dict[str, tuple[Decimal, float]] = {}
        self._historical_asset_price_cache: dict[tuple[str, int], Decimal] = {}
        self._instrument_info_cache: dict[str, tuple[dict, float]] = {}
        self._fee_rate_cache: dict[str, tuple[str, str, int, float]] = {}
        self._nonce_lock = threading.Lock()
        self._last_nonce = 0
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0.0

    def _rate_limit_remaining(self) -> float:
        with self._rate_limit_lock:
            return max(0.0, self._rate_limit_until - time.time())

    def _activate_rate_limit(self, message: str, response: Any | None = None) -> float:
        retry_after = self.RATE_LIMIT_DEFAULT_RETRY_SECONDS
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
                "Aster request paused after an exchange rate-limit rejection",
                retry_after=remaining,
            )

    def _nonce(self) -> int:
        with self._nonce_lock:
            nonce = int(time.time() * 1_000_000)
            if nonce <= self._last_nonce:
                nonce = self._last_nonce + 1
            self._last_nonce = nonce
            return nonce

    @staticmethod
    def _typed_data(message: str) -> dict:
        return {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [{"name": "msg", "type": "string"}],
            },
            "primaryType": "Message",
            "domain": {
                "name": "AsterSignTransaction",
                "version": "1",
                "chainId": 1666,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {"msg": message},
        }

    def _auth_params(self, params: dict[str, Any]) -> dict[str, Any]:
        if not self.signer_private_key:
            raise RuntimeError("Aster signer private key is required")
        if not self.signer:
            raise RuntimeError("Aster signer address is required")

        signed_params = dict(params)
        signed_params["nonce"] = str(self._nonce())
        if self.include_user_param and self.user:
            signed_params["user"] = self.user
        signed_params["signer"] = self.signer
        message = urlencode(signed_params, doseq=True)
        signable = encode_typed_data(full_message=self._typed_data(message))
        signature = Account.sign_message(signable, private_key=self.signer_private_key).signature.hex()
        signed_params["signature"] = signature
        return signed_params

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> Any:
        self._raise_if_rate_limited()
        request_params = self._auth_params(params or {}) if auth else dict(params or {})
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "grid-trading/1.0"}
        request_kwargs: dict[str, Any] = {"headers": headers, "timeout": 10}
        if method.upper() == "GET":
            request_kwargs["params"] = request_params
        else:
            request_kwargs["data"] = request_params
        response = self.session.request(method, url, **request_kwargs)
        try:
            data = response.json()
        except ValueError as exc:
            if response.status_code < 400:
                message = f"Aster returned invalid JSON for {path}"
                if method.upper() != "GET":
                    raise ExchangeRequestUncertainError(
                        f"{message}; request status unknown"
                    ) from exc
                raise RuntimeError(message) from exc
            data = {"code": response.status_code, "msg": response.text}
        message = data.get("msg") if isinstance(data, dict) else response.text
        error_code = data.get("code") if isinstance(data, dict) else None
        try:
            normalized_error_code = int(error_code) if error_code is not None else None
        except (TypeError, ValueError):
            normalized_error_code = None
        if response.status_code in {418, 429} or is_exchange_rate_limit_message(message):
            retry_after = self._activate_rate_limit(str(message or "Aster rate limit reached"), response)
            raise ExchangeRateLimitError(
                str(message or "Aster rate limit reached"),
                retry_after=retry_after,
            )
        if normalized_error_code in self.UNCERTAIN_EXECUTION_CODES:
            raise ExchangeRequestUncertainError(
                str(message or f"Aster request execution status unknown ({normalized_error_code})")
            )
        if response.status_code >= 400:
            if response.status_code == 408 or response.status_code >= 500:
                raise ExchangeRequestUncertainError(
                    message or f"Aster request status unknown after HTTP {response.status_code}"
                )
            raise RuntimeError(message or f"Aster request failed with {response.status_code}")
        if isinstance(data, dict) and data.get("code") not in (None, 0, "0", 200, "200"):
            raise RuntimeError(str(data.get("msg") or data))
        expected_type = list if path in self.LIST_RESPONSE_PATHS else dict
        if not isinstance(data, expected_type):
            message = f"Aster returned an invalid response structure for {path}"
            if method.upper() != "GET":
                raise ExchangeRequestUncertainError(
                    f"{message}; request status unknown"
                )
            raise RuntimeError(message)
        return data

    def get_ticker(self, symbol: str) -> dict:
        symbol = symbol.upper()
        ticker = self._request("GET", "/fapi/v3/ticker/24hr", params={"symbol": symbol})
        try:
            premium = self._request("GET", "/fapi/v3/premiumIndex", params={"symbol": symbol})
        except Exception:
            premium = {}
        price_change_pct = float(ticker.get("priceChangePercent", "0")) / 100
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": ticker.get("symbol", symbol),
                        "lastPrice": ticker.get("lastPrice") or ticker.get("price", "0"),
                        "indexPrice": premium.get("indexPrice", ""),
                        "markPrice": premium.get("markPrice", ""),
                        "price24hPcnt": str(price_change_pct),
                        "volume24h": ticker.get("quoteVolume") or ticker.get("volume", "0"),
                    }
                ]
            },
        }

    def get_instrument_info(self, symbol: str) -> dict:
        symbol = symbol.upper()
        cached = self._instrument_info_cache.get(symbol)
        now = time.time()
        if cached and now - cached[1] < 300:
            return cached[0]

        data = self._request("GET", "/fapi/v3/exchangeInfo", params={"symbol": symbol})
        instrument = next((item for item in data.get("symbols", []) if item.get("symbol") == symbol), None)
        if not instrument:
            return {"retCode": -1, "retMsg": f"Symbol {symbol} not found"}

        filters = {item.get("filterType"): item for item in instrument.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        market_lot_filter = filters.get("MARKET_LOT_SIZE", lot_filter)
        notional_filter = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})
        min_notional = (
            notional_filter.get("notional")
            or notional_filter.get("minNotional")
            or "0"
        )
        result = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "priceFilter": {"tickSize": price_filter.get("tickSize", "0.1")},
                        "lotSizeFilter": {
                            "qtyStep": lot_filter.get("stepSize", "0.001"),
                            "minOrderQty": lot_filter.get("minQty", "0.001"),
                            "maxOrderQty": lot_filter.get("maxQty", "0"),
                            "minNotionalValue": min_notional,
                        },
                        "marketLotSizeFilter": {
                            "qtyStep": market_lot_filter.get(
                                "stepSize", lot_filter.get("stepSize", "0.001")
                            ),
                            "minOrderQty": market_lot_filter.get(
                                "minQty", lot_filter.get("minQty", "0.001")
                            ),
                            "maxOrderQty": market_lot_filter.get("maxQty", "0"),
                        },
                    }
                ]
            },
        }
        self._instrument_info_cache[symbol] = (result, now)
        return result

    def get_balance(self) -> dict:
        balances = self._request("GET", "/fapi/v3/balance", auth=True)
        usdt = next((item for item in balances if item.get("asset") == "USDT"), {})
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "availableToWithdraw": usdt.get("availableBalance", "0"),
                                "walletBalance": usdt.get("balance", "0"),
                                "equity": usdt.get("balance", "0"),
                                "unrealisedPnl": usdt.get("crossUnPnl", "0"),
                            }
                        ]
                    }
                ]
            },
        }

    def set_leverage(self, symbol: str, leverage: str) -> dict:
        self._request(
            "POST",
            "/fapi/v3/leverage",
            params={"symbol": symbol.upper(), "leverage": leverage},
            auth=True,
        )
        return {"retCode": 0}

    def _build_order_params(
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
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": self._to_aster_side(side),
            "type": order_type.upper(),
            "quantity": qty,
            "reduceOnly": "true" if reduce_only else "false",
        }
        if order_type.lower() == "limit":
            params["price"] = price
            params["timeInForce"] = "GTX" if time_in_force == "PostOnly" else (time_in_force or "GTC")
        if order_link_id:
            params["newClientOrderId"] = order_link_id
        return params

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
        params = self._build_order_params(
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            order_type=order_type,
            reduce_only=reduce_only,
            order_link_id=order_link_id,
            time_in_force=time_in_force,
        )
        result = self._request("POST", "/fapi/v3/order", params=params, auth=True)
        normalized = self._normalize_order(result)
        normalized["orderId"] = str(result.get("orderId", normalized.get("orderId", "")))
        return {"retCode": 0, "result": normalized}

    def place_orders(self, orders: list[dict[str, Any]]) -> dict:
        batch_orders = []
        for order in orders:
            batch_orders.append(
                self._build_order_params(
                    symbol=str(order["symbol"]),
                    side=str(order["side"]),
                    qty=str(order["qty"]),
                    price=None if order.get("price") is None else str(order.get("price")),
                    order_type=str(order.get("order_type") or "Limit"),
                    reduce_only=bool(order.get("reduce_only", False)),
                    order_link_id=str(order.get("order_link_id") or ""),
                    time_in_force=order.get("time_in_force"),
                )
            )
        payload = json.dumps(batch_orders, separators=(",", ":"))
        results = self._request(
            "POST",
            "/fapi/v3/batchOrders",
            params={"batchOrders": payload},
            auth=True,
        )
        normalized = []
        for item in results if isinstance(results, list) else []:
            if isinstance(item, dict) and "orderId" in item:
                normalized_item = self._normalize_order(item)
                normalized_item["orderId"] = str(item.get("orderId", normalized_item.get("orderId", "")))
                normalized.append({"retCode": 0, "result": normalized_item})
            else:
                message = item.get("msg", "Batch order failed") if isinstance(item, dict) else "Batch order failed"
                if is_exchange_rate_limit_message(message):
                    self._activate_rate_limit(str(message))
                normalized.append(
                    {
                        "retCode": int(item.get("code", -1) or -1) if isinstance(item, dict) else -1,
                        "retMsg": message,
                        "result": {},
                    }
                )
        return {"retCode": 0, "result": {"list": normalized}}

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v3/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
        return {"retCode": 0}

    def cancel_all_orders(self, symbol: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v3/allOpenOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0}

    def get_open_orders(self, symbol: str) -> dict:
        orders = self._request(
            "GET",
            "/fapi/v3/openOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_order(item) for item in orders]}}

    def get_order(self, symbol: str, order_id: str) -> dict:
        order = self._request(
            "GET",
            "/fapi/v3/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
        return {"retCode": 0, "result": self._normalize_order(order)}

    def get_order_by_link(self, symbol: str, order_link_id: str) -> dict:
        try:
            order = self._request(
                "GET",
                "/fapi/v3/order",
                params={"symbol": symbol.upper(), "origClientOrderId": order_link_id},
                auth=True,
            )
        except RuntimeError as exc:
            message = str(exc).lower()
            if "order does not exist" in message or "unknown order" in message:
                return {"retCode": 0, "result": {}}
            raise
        return {"retCode": 0, "result": self._normalize_order(order)}

    def get_positions(self, symbol: str) -> dict:
        positions = self._request(
            "GET",
            "/fapi/v3/positionRisk",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_position(item) for item in positions]}}

    def get_order_history(self, symbol: str, limit: int = 50) -> dict:
        orders = self._request(
            "GET",
            "/fapi/v3/allOrders",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_order(item) for item in orders]}}

    @staticmethod
    def _trade_identity(item: dict[str, Any]) -> tuple[str, ...]:
        trade_id = str(item.get("id", "") or "")
        if trade_id:
            return ("id", trade_id)
        return (
            "shape",
            str(item.get("orderId", "") or ""),
            str(item.get("time", "") or ""),
            str(item.get("side", "") or ""),
            str(item.get("price", "") or ""),
            str(item.get("qty", "") or ""),
        )

    def _get_user_trades_window(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
    ) -> list[dict[str, Any]]:
        if end_time < start_time:
            return []

        windows: list[tuple[int, int]] = []
        window_start = max(0, int(start_time))
        final_end = max(window_start, int(end_time))
        while window_start <= final_end:
            window_end = min(final_end, window_start + self.TRADE_WINDOW_LIMIT_MS)
            windows.append((window_start, window_end))
            window_start = window_end + 1

        rows: dict[tuple[str, ...], dict[str, Any]] = {}
        query_count = 0
        for current_start, current_end in windows:
            query_count += 1
            if query_count > self.MAX_TRADE_HISTORY_QUERIES:
                raise RuntimeError("Aster trade history requires too many time-window queries")

            page = self._request(
                "GET",
                "/fapi/v3/userTrades",
                params={
                    "symbol": symbol,
                    "startTime": current_start,
                    "endTime": current_end,
                    "limit": self.TRADE_PAGE_LIMIT,
                },
                auth=True,
            )
            if not isinstance(page, list):
                raise RuntimeError("Aster trade history returned an invalid response")

            while True:
                page_ids: list[int] = []
                page_times: list[int] = []
                reached_window_end = False
                for item in page:
                    if not isinstance(item, dict):
                        raise RuntimeError("Aster trade history contains an invalid row")
                    try:
                        trade_id = int(item.get("id", ""))
                        trade_time = int(item.get("time", ""))
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Aster trade history row has no numeric id or time"
                        ) from exc
                    page_ids.append(trade_id)
                    page_times.append(trade_time)
                    if trade_time > current_end:
                        reached_window_end = True
                        continue
                    if trade_time >= current_start:
                        rows[self._trade_identity(item)] = item

                if len(page) < self.TRADE_PAGE_LIMIT or reached_window_end:
                    break
                if not page_ids:
                    raise RuntimeError("Aster trade history cannot advance without trade ids")
                if (
                    page_ids != sorted(page_ids)
                    or page_times != sorted(page_times)
                    or len(set(page_ids)) != len(page_ids)
                ):
                    raise RuntimeError(
                        "Aster full trade history page is not strictly ordered"
                    )

                next_from_id = max(page_ids) + 1
                query_count += 1
                if query_count > self.MAX_TRADE_HISTORY_QUERIES:
                    raise RuntimeError("Aster trade history requires too many paginated queries")
                next_page = self._request(
                    "GET",
                    "/fapi/v3/userTrades",
                    params={
                        "symbol": symbol,
                        "fromId": next_from_id,
                        "limit": self.TRADE_PAGE_LIMIT,
                    },
                    auth=True,
                )
                if not isinstance(next_page, list):
                    raise RuntimeError("Aster trade history returned an invalid response")
                if next_page:
                    try:
                        minimum_next_id = min(int(item.get("id", "")) for item in next_page)
                    except (AttributeError, TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Aster trade history row has no numeric trade id"
                        ) from exc
                    if minimum_next_id < next_from_id:
                        raise RuntimeError("Aster trade history pagination did not advance")
                page = next_page

        return sorted(
            rows.values(),
            key=lambda item: (
                int(item.get("time", 0) or 0),
                int(item.get("id", 0) or 0),
            ),
        )

    @staticmethod
    def _matching_order_trades(
        rows: list[dict[str, Any]],
        order_id: str,
    ) -> list[dict[str, Any]]:
        matching: dict[tuple[str, ...], dict[str, Any]] = {}
        for item in rows:
            if str(item.get("orderId", "")) != str(order_id):
                continue
            matching[AsterFuturesClient._trade_identity(item)] = item
        return sorted(
            matching.values(),
            key=lambda item: (
                int(item.get("time", 0) or 0),
                int(item.get("id", 0) or 0),
            ),
        )

    @staticmethod
    def _trade_qty(rows: list[dict[str, Any]]) -> Decimal:
        return sum(
            (Decimal(str(item.get("qty", "0") or "0")) for item in rows),
            Decimal("0"),
        )

    def get_order_trades(self, symbol: str, order_id: str) -> dict:
        symbol = symbol.upper()
        order_id = str(order_id)
        detail: dict[str, Any] | None = None
        try:
            detail = self._request(
                "GET",
                "/fapi/v3/order",
                params={"symbol": symbol, "orderId": order_id},
                auth=True,
            )
        except RuntimeError as exc:
            message = str(exc).lower()
            if "order does not exist" not in message and "unknown order" not in message:
                raise

        if detail is not None and not isinstance(detail, dict):
            raise RuntimeError("Aster order lookup returned an invalid response")

        expected_qty: Decimal | None = None
        if detail is not None:
            expected_qty = Decimal(str(detail.get("executedQty", "0") or "0"))
            if expected_qty <= 0:
                return {"retCode": 0, "result": {"list": []}}

            created_time = int(detail.get("time", detail.get("updateTime", 0)) or 0)
            updated_time = int(detail.get("updateTime", created_time) or created_time)
            if updated_time > 0:
                probe_start = max(0, updated_time - self.TRADE_PROBE_PADDING_MS)
                probe_end = updated_time + self.TRADE_PROBE_PADDING_MS
                probe_rows = self._get_user_trades_window(symbol, probe_start, probe_end)
                matches = self._matching_order_trades(probe_rows, order_id)
                matched_qty = self._trade_qty(matches)
                if matched_qty == expected_qty:
                    return {
                        "retCode": 0,
                        "result": {"list": [self._normalize_trade(item) for item in matches]},
                    }
                if matched_qty > expected_qty:
                    raise RuntimeError(
                        "Aster trade history quantity exceeds the order executed quantity"
                    )

                if created_time > 0:
                    full_start = max(
                        0,
                        min(created_time, updated_time) - self.TRADE_PROBE_PADDING_MS,
                    )
                    full_end = max(created_time, updated_time) + self.TRADE_PROBE_PADDING_MS
                    full_rows = self._get_user_trades_window(symbol, full_start, full_end)
                    matches = self._matching_order_trades(full_rows, order_id)
                    matched_qty = self._trade_qty(matches)
                    if matched_qty == expected_qty:
                        return {
                            "retCode": 0,
                            "result": {
                                "list": [self._normalize_trade(item) for item in matches]
                            },
                        }
                    if matched_qty > expected_qty:
                        raise RuntimeError(
                            "Aster trade history quantity exceeds the order executed quantity"
                        )

                raise RuntimeError(
                    "Aster trade history is incomplete for the order executed quantity"
                )

        recent = self._request(
            "GET",
            "/fapi/v3/userTrades",
            params={"symbol": symbol, "limit": self.TRADE_PAGE_LIMIT},
            auth=True,
        )
        if not isinstance(recent, list):
            raise RuntimeError("Aster trade history returned an invalid response")
        matches = self._matching_order_trades(recent, order_id)
        if expected_qty is not None and self._trade_qty(matches) != expected_qty:
            raise RuntimeError(
                "Aster recent trade history is incomplete for the order executed quantity"
            )
        if len(recent) >= self.TRADE_PAGE_LIMIT:
            raise RuntimeError(
                "Aster recent trade page is full and the order time is unavailable"
            )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_trade(item) for item in matches]},
        }

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
            "/fapi/v3/commissionRate",
            params={"symbol": symbol},
            auth=True,
        )
        fetched_at = int(time.time() * 1000)
        response = fee_rate_response(
            symbol,
            data.get("makerCommissionRate"),
            data.get("takerCommissionRate"),
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

    def get_recent_trades(self, symbol: str, limit: int = 100) -> dict:
        trades = self._request(
            "GET",
            "/fapi/v3/userTrades",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_trade(item) for item in trades]}}

    @staticmethod
    def _to_aster_side(side: str) -> str:
        return "BUY" if side.lower() == "buy" else "SELL"

    @staticmethod
    def _from_aster_side(side: str) -> str:
        return "Buy" if side.upper() == "BUY" else "Sell"

    def _normalize_order(self, item: dict[str, Any]) -> dict:
        return {
            "orderId": str(item.get("orderId", "")),
            "orderLinkId": item.get("clientOrderId", ""),
            "side": self._from_aster_side(str(item.get("side", ""))),
            "price": item.get("price", "0"),
            "qty": item.get("origQty", item.get("executedQty", "0")),
            "avgPrice": item.get("avgPrice", "0"),
            "executedQty": item.get("executedQty", "0"),
            "cumQuote": item.get("cumQuote", "0"),
            "orderStatus": item.get("status", ""),
            "reduceOnly": str(item.get("reduceOnly", "false")).lower() == "true" or item.get("reduceOnly") is True,
            "timeInForce": item.get("timeInForce", ""),
            "createdTime": str(item.get("time", item.get("updateTime", ""))),
        }

    def _normalize_trade(self, item: dict[str, Any]) -> dict:
        price = Decimal(str(item.get("price", "0")))
        qty = Decimal(str(item.get("qty", "0")))
        volume = Decimal(str(item.get("quoteQty") or (price * qty)))
        # Production V3 currently reports paid commission as positive, while
        # the official V3 response example uses a negative balance delta.
        # The normalized client contract represents fee cost as positive.
        fee_amount = abs(Decimal(str(item.get("commission", "0"))))
        fee_asset = str(item.get("commissionAsset", "USDT")).upper()
        fee_usdt, fee_usdt_source = self._fee_to_usdt_with_source(
            fee_amount,
            fee_asset,
            trade_time_ms=item.get("time"),
        )
        return {
            "orderId": str(item.get("orderId", "")),
            "tradeId": str(item.get("id", "")),
            "side": self._from_aster_side(str(item.get("side", ""))),
            "price": str(price),
            "qty": str(qty),
            "volume": str(volume),
            "fee": str(fee_amount),
            "feeAsset": fee_asset,
            "feeUsdt": "" if fee_usdt is None else str(fee_usdt),
            "feeUsdtSource": fee_usdt_source,
            "realizedPnl": item.get("realizedPnl", "0"),
            "isMaker": bool(item.get("maker", False)),
            "time": str(item.get("time", "")),
        }

    def _historical_fee_asset_price(self, symbol: str, trade_time_ms: int) -> Decimal | None:
        minute_start = trade_time_ms - (trade_time_ms % 60_000)
        key = (symbol, minute_start)
        cached = self._historical_asset_price_cache.get(key)
        if cached is not None:
            return cached

        try:
            rows = self._request(
                "GET",
                "/fapi/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": minute_start,
                    "limit": 1,
                },
            )
            row = rows[0]
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
        if asset in {"USDT", "USDC", "BUSD", "FDUSD", "USD"}:
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
        if cached and now - cached[1] < self.ASSET_PRICE_TTL_SECONDS:
            return amount * cached[0], "current_ticker_cache"
        try:
            ticker = self._request("GET", "/fapi/v3/ticker/price", params={"symbol": symbol})
            price = Decimal(str(ticker.get("price", "0")))
            if price <= 0:
                raise ValueError("invalid fee asset price")
            self._asset_price_cache[symbol] = (price, now)
            return amount * price, "current_ticker"
        except Exception:
            if cached:
                return amount * cached[0], "stale_current_ticker_cache"
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
    def _normalize_position(item: dict[str, Any]) -> dict:
        amount = Decimal(str(item.get("positionAmt", "0")))
        side = "Buy" if amount > 0 else "Sell"
        size = abs(amount)
        return {
            "side": side,
            "size": str(size.normalize()) if size else "0",
            "avgPrice": item.get("entryPrice", "0"),
            "markPrice": item.get("markPrice", "0"),
            "unrealisedPnl": item.get("unRealizedProfit", "0"),
            "leverage": item.get("leverage", ""),
            "liqPrice": item.get("liquidationPrice", ""),
        }

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

    def signature_payload(self, params: dict[str, Any]) -> str:
        signed_params = dict(params)
        signed_params["nonce"] = "1"
        if self.include_user_param and self.user:
            signed_params["user"] = self.user
        signed_params["signer"] = self.signer
        return urlencode(signed_params, doseq=True)

    def signature_typed_data(self, params: dict[str, Any]) -> dict:
        return copy.deepcopy(self._typed_data(self.signature_payload(params)))
