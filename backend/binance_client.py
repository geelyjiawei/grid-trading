import hashlib
import hmac
import json
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests


class BinanceFuturesClient:
    exchange = "binance"
    ASSET_PRICE_TTL_SECONDS = 60

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        self.recv_window = 5000
        self.session = requests.Session()
        self._asset_price_cache: dict[str, Decimal | tuple[Decimal, float]] = {}
        self._instrument_info_cache: dict[str, tuple[dict, float]] = {}

    def _sign(self, params: dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        auth: bool = False,
        api_key: bool = False,
    ) -> Any:
        request_params = dict(params or {})
        headers = {"X-MBX-APIKEY": self.api_key} if auth or api_key else None

        if auth:
            request_params["timestamp"] = int(time.time() * 1000)
            request_params["recvWindow"] = self.recv_window
            request_params["signature"] = self._sign(request_params)

        url = f"{self.base_url}{path}"
        response = self.session.request(method, url, params=request_params, headers=headers, timeout=10)
        try:
            data = response.json()
        except ValueError:
            data = {"code": response.status_code, "msg": response.text}
        if response.status_code >= 400:
            message = data.get("msg") if isinstance(data, dict) else response.text
            raise RuntimeError(message or f"Binance request failed with {response.status_code}")
        return data

    def get_ticker(self, symbol: str) -> dict:
        symbol = symbol.upper()
        ticker = self._request("GET", "/fapi/v1/ticker/24hr", params={"symbol": symbol})
        premium = self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
        price_change_pct = float(ticker.get("priceChangePercent", "0")) / 100
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": ticker.get("symbol", symbol),
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
        if cached and now - cached[1] < 300:
            return cached[0]

        data = self._request("GET", "/fapi/v1/exchangeInfo", params={"symbol": symbol})
        instrument = next((item for item in data.get("symbols", []) if item.get("symbol") == symbol), None)
        if not instrument:
            return {"retCode": -1, "retMsg": f"Symbol {symbol} not found"}

        filters = {item.get("filterType"): item for item in instrument.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        result = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "priceFilter": {"tickSize": price_filter.get("tickSize", "0.1")},
                        "lotSizeFilter": {
                            "qtyStep": lot_filter.get("stepSize", "0.001"),
                            "minOrderQty": lot_filter.get("minQty", "0.001"),
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
        normalized = self._normalize_order(result)
        normalized["orderId"] = str(result.get("orderId", normalized.get("orderId", "")))
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
        normalized = []
        for item in results if isinstance(results, list) else []:
            if "orderId" in item:
                normalized_item = self._normalize_order(item)
                normalized_item["orderId"] = str(item.get("orderId", normalized_item.get("orderId", "")))
                normalized.append({"retCode": 0, "result": normalized_item})
            else:
                normalized.append(
                    {
                        "retCode": int(item.get("code", -1) or -1),
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
        self._request(
            "DELETE",
            "/fapi/v1/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
        return {"retCode": 0}

    def cancel_all_orders(self, symbol: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0}

    def get_open_orders(self, symbol: str) -> dict:
        orders = self._request(
            "GET",
            "/fapi/v1/openOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_order(item) for item in orders]},
        }

    def get_order(self, symbol: str, order_id: str) -> dict:
        order = self._request(
            "GET",
            "/fapi/v1/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
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
            "/fapi/v1/allOrders",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_order(item) for item in orders]},
        }

    def get_order_trades(self, symbol: str, order_id: str) -> dict:
        trades = self._request(
            "GET",
            "/fapi/v1/userTrades",
            params={"symbol": symbol.upper(), "orderId": order_id, "limit": 100},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_trade(item) for item in trades]},
        }

    def get_recent_trades(self, symbol: str, limit: int = 100) -> dict:
        trades = self._request(
            "GET",
            "/fapi/v1/userTrades",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_trade(item) for item in trades]},
        }

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

    def _normalize_trade(self, item: dict[str, Any]) -> dict:
        price = Decimal(str(item.get("price", "0")))
        qty = Decimal(str(item.get("qty", "0")))
        volume = Decimal(str(item.get("quoteQty") or (price * qty)))
        fee_amount = Decimal(str(item.get("commission", "0")))
        fee_asset = str(item.get("commissionAsset", "USDT")).upper()
        fee_usdt = self._fee_to_usdt(fee_amount, fee_asset)

        return {
            "orderId": str(item.get("orderId", "")),
            "tradeId": str(item.get("id", "")),
            "side": self._from_binance_side(str(item.get("side", ""))),
            "price": str(price),
            "qty": str(qty),
            "volume": str(volume),
            "fee": str(fee_amount),
            "feeAsset": fee_asset,
            "feeUsdt": "" if fee_usdt is None else str(fee_usdt),
            "realizedPnl": item.get("realizedPnl", "0"),
            "isMaker": bool(item.get("maker", False)),
            "time": str(item.get("time", "")),
        }

    def _fee_to_usdt(self, amount: Decimal, asset: str) -> Decimal | None:
        if amount == 0:
            return Decimal("0")
        if asset in {"USDT", "USDC", "BUSD", "FDUSD", "USD"}:
            return amount

        symbol = f"{asset}USDT"
        now = time.time()
        cached = self._asset_price_cache.get(symbol)
        if isinstance(cached, tuple):
            cached_price, cached_at = cached
            if now - cached_at < self.ASSET_PRICE_TTL_SECONDS:
                return amount * cached_price
        elif cached is not None:
            return amount * cached

        try:
            ticker = self._request("GET", "/fapi/v1/ticker/price", params={"symbol": symbol})
            price = Decimal(str(ticker.get("price", "0")))
            self._asset_price_cache[symbol] = (price, now)
            return amount * price
        except Exception:
            if isinstance(cached, tuple):
                return amount * cached[0]
            if cached is not None:
                return amount * cached
            return None

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
