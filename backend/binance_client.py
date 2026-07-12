import hashlib
import hmac
import json
import logging
import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests

from exchange_errors import (
    ExchangeRateLimitError,
    ExchangeRequestUncertainError,
    is_exchange_rate_limit_message,
)
from exchange_snapshots import (
    OPEN_ORDER_STATUSES,
    normalize_binance_style_cancel_ack,
    normalize_binance_style_order_ack,
    normalize_binance_style_order_rows,
    normalize_futures_balance_rows,
    snapshot_boolean,
    snapshot_decimal,
    snapshot_text,
    validate_execution_response,
    validate_execution_row,
    validate_instrument_response,
    validate_positive_decimal,
    validate_positive_integer,
    validate_price_cache_entry,
    validate_symbol_price_row,
)
from fee_rates import fee_rate_response


logger = logging.getLogger(__name__)


class BinanceFuturesClient:
    exchange = "binance"
    ASSET_PRICE_TTL_SECONDS = 60
    HISTORICAL_ASSET_PRICE_CACHE_MAX_ITEMS = 2048
    MAX_TRADE_HISTORY_PAGES = 100
    FEE_RATE_TTL_SECONDS = 300
    RATE_LIMIT_DEFAULT_RETRY_SECONDS = 60.0
    TIME_SYNC_DEDUP_SECONDS = 1.0
    MAX_TIME_OFFSET_MS = 24 * 60 * 60 * 1000
    LIST_RESPONSE_PATHS = frozenset(
        {
            "/fapi/v1/allOrders",
            "/fapi/v1/batchOrders",
            "/fapi/v1/klines",
            "/fapi/v1/openOrders",
            "/fapi/v1/userTrades",
            "/fapi/v3/balance",
            "/fapi/v3/positionRisk",
        }
    )

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        self.recv_window = 5000
        self.session = requests.Session()
        self._asset_price_cache: dict[str, tuple[Decimal, float]] = {}
        self._historical_asset_price_cache: dict[tuple[str, int], Decimal] = {}
        self._instrument_info_cache: dict[str, tuple[dict, float]] = {}
        self._fee_rate_cache: dict[str, tuple[str, str, int, float]] = {}
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0.0
        self._time_sync_lock = threading.Lock()
        self._time_offset_ms = 0
        self._time_sync_monotonic = 0.0

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
                "Binance request paused after an exchange rate-limit rejection",
                retry_after=remaining,
            )

    def _sign(self, params: dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    @staticmethod
    def _is_timestamp_rejection(data: Any, message: object) -> bool:
        code = data.get("code") if isinstance(data, dict) else None
        if str(code) == "-1021":
            return True
        normalized = str(message or "").lower()
        return "timestamp" in normalized and (
            "recvwindow" in normalized
            or "ahead of the server" in normalized
            or "outside of the" in normalized
        )

    def _sync_server_time(self) -> bool:
        with self._time_sync_lock:
            now = time.monotonic()
            if (
                self._time_sync_monotonic > 0
                and now - self._time_sync_monotonic < self.TIME_SYNC_DEDUP_SECONDS
            ):
                return True

            before_ms = int(time.time() * 1000)
            try:
                response = self.session.request(
                    "GET",
                    f"{self.base_url}/fapi/v1/time",
                    timeout=10,
                )
            except requests.RequestException:
                return False
            after_ms = int(time.time() * 1000)

            try:
                data = response.json()
            except ValueError:
                data = {}
            message = data.get("msg") if isinstance(data, dict) else response.text
            if response.status_code in {418, 429} or is_exchange_rate_limit_message(
                message
            ):
                retry_after = self._activate_rate_limit(
                    str(message or "Binance rate limit reached while synchronizing time"),
                    response,
                )
                raise ExchangeRateLimitError(
                    str(message or "Binance rate limit reached while synchronizing time"),
                    retry_after=retry_after,
                )
            if response.status_code >= 400 or not isinstance(data, dict):
                logger.warning(
                    "Binance server-time synchronization failed status=%s",
                    response.status_code,
                )
                return False
            try:
                server_time = int(data.get("serverTime"))
            except (TypeError, ValueError):
                return False
            if server_time <= 0:
                return False

            midpoint_ms = (before_ms + after_ms) // 2
            offset_ms = server_time - midpoint_ms
            if abs(offset_ms) > self.MAX_TIME_OFFSET_MS:
                logger.warning(
                    "Binance server-time synchronization returned implausible offset_ms=%s",
                    offset_ms,
                )
                return False
            self._time_offset_ms = offset_ms
            self._time_sync_monotonic = time.monotonic()
            logger.warning(
                "Synchronized Binance server time after timestamp rejection offset_ms=%s rtt_ms=%s",
                offset_ms,
                after_ms - before_ms,
            )
            return True

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        auth: bool = False,
        api_key: bool = False,
    ) -> Any:
        base_params = dict(params or {})
        headers = {"X-MBX-APIKEY": self.api_key} if auth or api_key else None
        url = f"{self.base_url}{path}"
        for attempt in range(2):
            self._raise_if_rate_limited()
            request_params = dict(base_params)
            if auth:
                request_params["timestamp"] = self._timestamp_ms()
                request_params["recvWindow"] = self.recv_window
                request_params["signature"] = self._sign(request_params)

            response = self.session.request(
                method,
                url,
                params=request_params,
                headers=headers,
                timeout=10,
            )
            try:
                data = response.json()
            except ValueError as exc:
                if response.status_code < 400:
                    message = f"Binance returned invalid JSON for {path}"
                    if method.upper() != "GET":
                        raise ExchangeRequestUncertainError(
                            f"{message}; request status unknown"
                        ) from exc
                    raise RuntimeError(message) from exc
                data = {"code": response.status_code, "msg": response.text}
            if response.status_code >= 400:
                message = data.get("msg") if isinstance(data, dict) else response.text
                if response.status_code in {418, 429} or is_exchange_rate_limit_message(
                    message
                ):
                    retry_after = self._activate_rate_limit(
                        str(message or "Binance rate limit reached"), response
                    )
                    raise ExchangeRateLimitError(
                        str(message or "Binance rate limit reached"),
                        retry_after=retry_after,
                    )
                if response.status_code == 408 or response.status_code >= 500:
                    raise ExchangeRequestUncertainError(
                        message
                        or f"Binance request status unknown after HTTP {response.status_code}"
                    )
                if (
                    auth
                    and attempt == 0
                    and self._is_timestamp_rejection(data, message)
                    and self._sync_server_time()
                ):
                    continue
                raise RuntimeError(
                    message or f"Binance request failed with {response.status_code}"
                )
            expected_type = list if path in self.LIST_RESPONSE_PATHS else dict
            if not isinstance(data, expected_type):
                message = f"Binance returned an invalid response structure for {path}"
                if method.upper() != "GET":
                    raise ExchangeRequestUncertainError(
                        f"{message}; request status unknown"
                    )
                raise RuntimeError(message)
            return data
        raise RuntimeError("Binance request failed after timestamp synchronization")

    def get_ticker(self, symbol: str) -> dict:
        symbol = symbol.upper()
        ticker = self._request("GET", "/fapi/v1/ticker/24hr", params={"symbol": symbol})
        premium = self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
        ticker_symbol = str(ticker.get("symbol") or "").upper()
        premium_symbol = str(premium.get("symbol") or "").upper()
        if ticker_symbol != symbol or premium_symbol != symbol:
            raise RuntimeError(
                f"Binance ticker symbol mismatch for {symbol}: "
                f"ticker={ticker_symbol or 'missing'} premium={premium_symbol or 'missing'}"
            )
        price_change_pct = float(ticker.get("priceChangePercent", "0")) / 100
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": ticker_symbol,
                        "lastPrice": ticker.get("lastPrice", "0"),
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
        if cached:
            try:
                cached_response, cached_at = cached
                if now - float(cached_at) < 300:
                    validate_instrument_response(cached_response, symbol=symbol)
                    return cached_response
            except (RuntimeError, TypeError, ValueError):
                self._instrument_info_cache.pop(symbol, None)

        data = self._request("GET", "/fapi/v1/exchangeInfo", params={"symbol": symbol})
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
                        "symbol": symbol,
                        "priceFilter": {"tickSize": price_filter.get("tickSize")},
                        "lotSizeFilter": {
                            "qtyStep": lot_filter.get("stepSize"),
                            "minOrderQty": lot_filter.get("minQty"),
                            "maxOrderQty": lot_filter.get("maxQty", "0"),
                            "minNotionalValue": min_notional,
                        },
                        "marketLotSizeFilter": {
                            "qtyStep": market_lot_filter.get(
                                "stepSize", lot_filter.get("stepSize")
                            ),
                            "minOrderQty": market_lot_filter.get(
                                "minQty", lot_filter.get("minQty")
                            ),
                            "maxOrderQty": market_lot_filter.get("maxQty", "0"),
                        },
                    }
                ]
            },
        }
        validate_instrument_response(result, symbol=symbol)
        self._instrument_info_cache[symbol] = (result, now)
        return result

    def get_balance(self) -> dict:
        balances = self._request("GET", "/fapi/v3/balance", auth=True)
        return normalize_futures_balance_rows(balances)

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
            "/fapi/v1/commissionRate",
            params={"symbol": symbol},
            auth=True,
        )
        returned_symbol = str(data.get("symbol") or "").upper()
        if returned_symbol != symbol:
            raise RuntimeError(
                f"Binance fee rate response symbol mismatch for {symbol}: "
                f"{returned_symbol or 'missing symbol'}"
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

    def set_leverage(self, symbol: str, leverage: str) -> dict:
        self._request(
            "POST",
            "/fapi/v1/leverage",
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
            "side": self._to_binance_side(side),
            "type": order_type.upper(),
            "quantity": qty,
            "reduceOnly": reduce_only,
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

        result = self._request("POST", "/fapi/v1/order", params=params, auth=True)
        try:
            normalized = normalize_binance_style_order_ack(
                result,
                expected_symbol=symbol,
                expected_link_id=order_link_id,
            )
        except RuntimeError as exc:
            raise ExchangeRequestUncertainError(
                f"Binance order acknowledgement is not authoritative: {exc}"
            ) from exc
        return {"retCode": 0, "result": normalized}

    def place_orders(self, orders: list[dict[str, Any]]) -> dict:
        batch_orders = []
        for order in orders:
            params = self._build_order_params(
                symbol=str(order["symbol"]),
                side=str(order["side"]),
                qty=str(order["qty"]),
                price=None if order.get("price") is None else str(order.get("price")),
                order_type=str(order.get("order_type") or "Limit"),
                reduce_only=bool(order.get("reduce_only", False)),
                order_link_id=str(order.get("order_link_id") or ""),
                time_in_force=order.get("time_in_force"),
            )
            params["reduceOnly"] = "true" if bool(order.get("reduce_only", False)) else "false"
            batch_orders.append(params)
        payload = json.dumps(batch_orders, separators=(",", ":"))
        results = self._request(
            "POST",
            "/fapi/v1/batchOrders",
            params={"batchOrders": payload},
            auth=True,
        )
        if not isinstance(results, list) or len(results) != len(orders):
            raise ExchangeRequestUncertainError(
                "Binance batch acknowledgement count does not match the request"
            )
        normalized = []
        seen_order_ids: set[str] = set()
        seen_link_ids: set[str] = set()
        for index, item in enumerate(results):
            if isinstance(item, dict) and "orderId" in item:
                request = orders[index]
                try:
                    normalized_item = normalize_binance_style_order_ack(
                        item,
                        expected_symbol=str(request.get("symbol") or ""),
                        expected_link_id=str(request.get("order_link_id") or ""),
                    )
                except RuntimeError as exc:
                    raise ExchangeRequestUncertainError(
                        f"Binance batch acknowledgement is not authoritative: {exc}"
                    ) from exc
                order_id = str(normalized_item["orderId"])
                link_id = str(normalized_item.get("orderLinkId") or "")
                if order_id in seen_order_ids or (link_id and link_id in seen_link_ids):
                    raise ExchangeRequestUncertainError(
                        "Binance batch acknowledgement contains duplicate order identities"
                    )
                seen_order_ids.add(order_id)
                if link_id:
                    seen_link_ids.add(link_id)
                normalized.append({"retCode": 0, "result": normalized_item})
            else:
                if not isinstance(item, dict) or "code" not in item:
                    raise ExchangeRequestUncertainError(
                        "Binance batch acknowledgement contains an unidentified item"
                    )
                try:
                    error_code = int(item.get("code", 0))
                except (TypeError, ValueError) as exc:
                    raise ExchangeRequestUncertainError(
                        "Binance batch acknowledgement contains an invalid error code"
                    ) from exc
                if error_code == 0:
                    raise ExchangeRequestUncertainError(
                        "Binance batch acknowledgement omitted an accepted order identity"
                    )
                normalized.append(
                    {
                        "retCode": error_code,
                        "retMsg": item.get("msg", "Batch order failed"),
                        "result": {},
                    }
                )
        return {"retCode": 0, "result": {"list": normalized}}

    def start_user_stream(self) -> str:
        data = self._request("POST", "/fapi/v1/listenKey", api_key=True)
        return str(data.get("listenKey", ""))

    def keepalive_user_stream(self, listen_key: str) -> dict:
        self._request("PUT", "/fapi/v1/listenKey", params={"listenKey": listen_key}, api_key=True)
        return {"retCode": 0}

    def close_user_stream(self, listen_key: str) -> dict:
        self._request("DELETE", "/fapi/v1/listenKey", params={"listenKey": listen_key}, api_key=True)
        return {"retCode": 0}

    def user_stream_url(self, listen_key: str) -> str:
        base_url = "wss://stream.binancefuture.com/ws" if self.testnet else "wss://fstream.binance.com/ws"
        return f"{base_url}/{listen_key}"

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        symbol = symbol.upper()
        order_id = str(order_id)
        response = self._request(
            "DELETE",
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id},
            auth=True,
        )
        try:
            result = normalize_binance_style_cancel_ack(
                response,
                expected_symbol=symbol,
                expected_order_id=order_id,
            )
        except RuntimeError as exc:
            raise ExchangeRequestUncertainError(
                f"Binance cancellation acknowledgement is not authoritative: {exc}"
            ) from exc
        return {"retCode": 0, "result": result}

    def cancel_all_orders(self, symbol: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0}

    def get_open_orders(self, symbol: str) -> dict:
        symbol = symbol.upper()
        orders = self._request(
            "GET",
            "/fapi/v1/openOrders",
            params={"symbol": symbol},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {
                "list": normalize_binance_style_order_rows(
                    orders,
                    expected_symbol=symbol,
                    allowed_statuses=OPEN_ORDER_STATUSES,
                    unique_link_ids=True,
                )
            },
        }

    def get_order(self, symbol: str, order_id: str) -> dict:
        symbol = symbol.upper()
        order_id = str(order_id)
        order = self._request(
            "GET",
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id},
            auth=True,
        )
        rows = normalize_binance_style_order_rows(
            [order],
            expected_symbol=symbol,
            expected_order_id=order_id,
            require_single=True,
        )
        return {"retCode": 0, "result": rows[0]}

    def get_order_by_link(self, symbol: str, order_link_id: str) -> dict:
        symbol = symbol.upper()
        order_link_id = str(order_link_id)
        try:
            order = self._request(
                "GET",
                "/fapi/v1/order",
                params={"symbol": symbol, "origClientOrderId": order_link_id},
                auth=True,
            )
        except RuntimeError as exc:
            message = str(exc).lower()
            if "order does not exist" in message or "unknown order" in message:
                return {"retCode": 0, "result": {}}
            raise
        rows = normalize_binance_style_order_rows(
            [order],
            expected_symbol=symbol,
            expected_link_id=order_link_id,
            require_single=True,
        )
        return {"retCode": 0, "result": rows[0]}

    def get_positions(self, symbol: str) -> dict:
        positions = self._request(
            "GET",
            "/fapi/v3/positionRisk",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_position(item) for item in positions]}}

    def get_order_history(self, symbol: str, limit: int = 50) -> dict:
        symbol = symbol.upper()
        orders = self._request(
            "GET",
            "/fapi/v1/allOrders",
            params={"symbol": symbol, "limit": limit},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {
                "list": normalize_binance_style_order_rows(
                    orders,
                    expected_symbol=symbol,
                )
            },
        }

    def get_order_trades(self, symbol: str, order_id: str) -> dict:
        symbol = symbol.upper()
        order_id = str(order_id)
        page_limit = 1000
        params: dict[str, Any] = {
            "symbol": symbol,
            "orderId": order_id,
            "limit": page_limit,
        }
        trades: list[Any] = []
        requested_from_id: int | None = None
        for _ in range(self.MAX_TRADE_HISTORY_PAGES):
            page = self._request(
                "GET",
                "/fapi/v1/userTrades",
                params=dict(params),
                auth=True,
            )
            if not isinstance(page, list):
                raise RuntimeError(
                    f"{symbol} execution snapshot response must be an array"
                )
            if len(page) > page_limit:
                raise RuntimeError(
                    f"{symbol} execution snapshot page exceeds the requested limit"
                )
            trades.extend(page)
            if len(page) < page_limit:
                break

            page_ids: list[int] = []
            for row_index, item in enumerate(page):
                if not isinstance(item, dict):
                    raise RuntimeError(
                        f"{symbol} execution snapshot row {row_index} must be an object"
                    )
                parsed_id = snapshot_decimal(
                    item.get("id"),
                    context=f"{symbol} execution snapshot",
                    row_index=row_index,
                    field="tradeId",
                )
                assert parsed_id is not None
                if parsed_id < 0 or parsed_id != parsed_id.to_integral_value():
                    raise RuntimeError(
                        f"{symbol} execution snapshot row {row_index} has invalid tradeId"
                    )
                page_ids.append(int(parsed_id))
            if not page_ids:
                raise RuntimeError(
                    f"{symbol} execution snapshot cannot advance without trade ids"
                )
            next_from_id = max(page_ids) + 1
            if requested_from_id is not None and next_from_id <= requested_from_id:
                raise RuntimeError(
                    f"{symbol} execution snapshot pagination did not advance"
                )
            requested_from_id = next_from_id
            params["fromId"] = next_from_id
        else:
            raise RuntimeError(
                f"{symbol} execution snapshot pagination exceeded "
                f"{self.MAX_TRADE_HISTORY_PAGES} pages"
            )

        matching: list[dict[str, Any]] = []
        for item in trades:
            if not isinstance(item, dict):
                raise RuntimeError(f"{symbol} execution snapshot row must be an object")
            if str(item.get("orderId", "")) != str(order_id):
                continue
            matching.append(
                self._normalize_trade(
                    item,
                    expected_symbol=symbol,
                    expected_order_id=order_id,
                )
            )
        response = {
            "retCode": 0,
            "result": {"list": matching},
        }
        validated = validate_execution_response(
            response,
            expected_symbol=symbol,
            expected_order_id=order_id,
        )
        response["result"]["list"] = [item["raw"] for item in validated]
        return response

    def get_recent_trades(self, symbol: str, limit: int = 100) -> dict:
        symbol = symbol.upper()
        trades = self._request(
            "GET",
            "/fapi/v1/userTrades",
            params={"symbol": symbol, "limit": limit},
            auth=True,
        )
        if not isinstance(trades, list):
            raise RuntimeError(f"{symbol} execution snapshot response must be an array")
        response = {
            "retCode": 0,
            "result": {
                "list": [
                    self._normalize_trade(item, expected_symbol=symbol)
                    for item in trades
                ]
            },
        }
        validated = validate_execution_response(
            response,
            expected_symbol=symbol,
        )
        response["result"]["list"] = [item["raw"] for item in validated]
        return response

    @staticmethod
    def _to_binance_side(side: str) -> str:
        return "BUY" if side.lower() == "buy" else "SELL"

    @staticmethod
    def _from_binance_side(side: str) -> str:
        return "Buy" if side.upper() == "BUY" else "Sell"

    def _normalize_order(self, item: dict[str, Any]) -> dict:
        return {
            "orderId": str(item.get("orderId", "")),
            "orderLinkId": item.get("clientOrderId", ""),
            "side": self._from_binance_side(str(item.get("side", ""))),
            "price": item.get("price", "0"),
            "qty": item.get("origQty", item.get("executedQty", "0")),
            "avgPrice": item.get("avgPrice", "0"),
            "executedQty": item.get("executedQty", "0"),
            "cumQuote": item.get("cumQuote", "0"),
            "orderStatus": item.get("status", ""),
            "reduceOnly": item.get("reduceOnly", False),
            "timeInForce": item.get("timeInForce", ""),
            "createdTime": str(item.get("time", "")),
        }

    def _normalize_trade(
        self,
        item: dict[str, Any],
        *,
        expected_symbol: str = "",
        expected_order_id: str = "",
    ) -> dict:
        context = f"{str(expected_symbol or '').upper()} execution snapshot".strip()
        if context == "execution snapshot":
            context = "execution snapshot"
        if not isinstance(item, dict):
            raise RuntimeError(f"{context} row 0 must be an object")

        symbol = snapshot_text(
            item.get("symbol"),
            context=context,
            row_index=0,
            field="symbol",
        ).upper()
        order_id = snapshot_text(
            item.get("orderId"),
            context=context,
            row_index=0,
            field="orderId",
        )
        trade_id = snapshot_text(
            item.get("id"),
            context=context,
            row_index=0,
            field="tradeId",
        )
        raw_side = snapshot_text(
            item.get("side"),
            context=context,
            row_index=0,
            field="side",
        ).upper()
        if raw_side not in {"BUY", "SELL"}:
            raise RuntimeError(f"{context} row 0 has invalid side")

        price = validate_positive_decimal(
            item.get("price"), context=context, field="price"
        )
        qty = validate_positive_decimal(
            item.get("qty"), context=context, field="qty"
        )
        volume = snapshot_decimal(
            item.get("quoteQty"),
            context=context,
            row_index=0,
            field="volume",
            allow_blank=True,
        )
        if volume is None:
            volume = price * qty
        if volume <= 0:
            raise RuntimeError(f"{context} row 0 has non-positive volume")
        fee_amount = snapshot_decimal(
            item.get("commission"),
            context=context,
            row_index=0,
            field="fee",
        )
        assert fee_amount is not None
        fee_asset = snapshot_text(
            item.get("commissionAsset"),
            context=context,
            row_index=0,
            field="feeAsset",
        ).upper()
        realized_pnl = snapshot_decimal(
            item.get("realizedPnl", "0"),
            context=context,
            row_index=0,
            field="realizedPnl",
            allow_blank=True,
        )
        if realized_pnl is None:
            realized_pnl = Decimal("0")
        is_maker = snapshot_boolean(
            item.get("maker"),
            context=context,
            row_index=0,
            field="isMaker",
        )
        trade_time = validate_positive_integer(
            item.get("time"), context=context, field="time"
        )
        fee_usdt, fee_usdt_source = self._fee_to_usdt_with_source(
            fee_amount,
            fee_asset,
            trade_time_ms=trade_time,
        )

        normalized = {
            "symbol": symbol,
            "orderId": order_id,
            "tradeId": trade_id,
            "side": self._from_binance_side(raw_side),
            "price": str(price),
            "qty": str(qty),
            "volume": str(volume),
            "fee": str(fee_amount),
            "feeAsset": fee_asset,
            "feeUsdt": "" if fee_usdt is None else str(fee_usdt),
            "feeUsdtSource": fee_usdt_source,
            "realizedPnl": str(realized_pnl),
            "isMaker": is_maker,
            "time": str(trade_time),
        }
        validate_execution_row(
            normalized,
            expected_symbol=expected_symbol,
            expected_order_id=expected_order_id,
        )
        return normalized

    def _historical_fee_asset_price(self, symbol: str, trade_time_ms: int) -> Decimal | None:
        minute_start = trade_time_ms - (trade_time_ms % 60_000)
        key = (symbol, minute_start)
        cached = self._historical_asset_price_cache.get(key)
        if cached is not None:
            try:
                return validate_positive_decimal(
                    cached,
                    context=f"{symbol} historical fee price cache",
                    field="open price",
                )
            except RuntimeError:
                self._historical_asset_price_cache.pop(key, None)

        try:
            rows = self._request(
                "GET",
                "/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": minute_start,
                    "limit": 1,
                },
            )
            if not isinstance(rows, list) or len(rows) != 1:
                return None
            row = rows[0]
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                return None
            if int(row[0]) != minute_start:
                return None
            price = validate_positive_decimal(
                row[1],
                context=f"{symbol} historical fee price snapshot",
                field="open price",
            )
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
        cached_price = None
        cached_fresh = False
        if cached is not None:
            try:
                cached_price, cached_fresh = validate_price_cache_entry(
                    cached,
                    symbol=symbol,
                    now=now,
                    ttl_seconds=self.ASSET_PRICE_TTL_SECONDS,
                )
            except RuntimeError:
                self._asset_price_cache.pop(symbol, None)
        if cached_fresh:
            return amount * cached_price, "current_ticker_cache"

        try:
            ticker = self._request("GET", "/fapi/v1/ticker/price", params={"symbol": symbol})
            price = validate_symbol_price_row(ticker, symbol=symbol)
            self._asset_price_cache[symbol] = (price, now)
            return amount * price, "current_ticker"
        except Exception:
            if cached_price is not None:
                return amount * cached_price, "stale_current_ticker_cache"
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
            "symbol": str(item.get("symbol", "") or "").upper(),
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
