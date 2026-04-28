import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from auth import (
    COOKIE_NAME,
    build_totp_uri,
    create_session,
    get_auth_settings,
    verify_password,
    verify_session,
    verify_totp,
)
from bybit_client import BybitClient
from grid_engine import GridEngine
from secret_store import decrypt_text, encrypt_text, storage_backend


_engines: dict[str, GridEngine] = {}

BASE_DIR = os.path.dirname(__file__)
FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "frontend"))
API_CONFIG_FILE = os.getenv("GRID_CONFIG_FILE") or os.path.join(BASE_DIR, "api_config.json")


def _parse_bool(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _load_env_api_config() -> dict | None:
    api_key = os.getenv("BYBIT_API_KEY", "").strip()
    api_secret = os.getenv("BYBIT_API_SECRET", "").strip()
    if not api_key or not api_secret:
        return None

    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "testnet": _parse_bool(os.getenv("BYBIT_TESTNET")),
        "source": "env",
    }


def _load_api_config() -> dict:
    env_config = _load_env_api_config()
    if env_config:
        return env_config

    if not os.path.exists(API_CONFIG_FILE):
        return {"api_key": "", "api_secret": "", "testnet": False, "source": "none"}

    try:
        with open(API_CONFIG_FILE, "r", encoding="utf-8") as file:
            config = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {"api_key": "", "api_secret": "", "testnet": False}

    try:
        if config.get("encrypted"):
            return {
                "api_key": decrypt_text(str(config.get("api_key", ""))),
                "api_secret": decrypt_text(str(config.get("api_secret", ""))),
                "testnet": bool(config.get("testnet", False)),
                "source": "file",
            }

        # One-time migration for configs saved before encrypted storage existed.
        migrated = {
            "api_key": str(config.get("api_key", "")),
            "api_secret": str(config.get("api_secret", "")),
            "testnet": bool(config.get("testnet", False)),
            "source": "file",
        }
        if migrated["api_key"] or migrated["api_secret"]:
            _save_api_config(migrated)
        return migrated
    except Exception:
        return {"api_key": "", "api_secret": "", "testnet": False, "source": "none"}


def _save_api_config(config: dict):
    encrypted_config = {
        "encrypted": True,
        "backend": storage_backend(),
        "api_key": encrypt_text(str(config.get("api_key", ""))),
        "api_secret": encrypt_text(str(config.get("api_secret", ""))),
        "testnet": bool(config.get("testnet", False)),
    }
    with open(API_CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump(encrypted_config, file, ensure_ascii=False, indent=2)


def _mask_api_key(api_key: str) -> str:
    return f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) >= 8 else api_key


def _build_client_from_config(config: dict) -> BybitClient | None:
    if not config.get("api_key") or not config.get("api_secret"):
        return None
    return BybitClient(config["api_key"], config["api_secret"], bool(config.get("testnet", False)))


_api_config = _load_api_config()
_client = _build_client_from_config(_api_config)


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    for engine in list(_engines.values()):
        if engine.running:
            await engine.stop()


app = FastAPI(title="Grid Trading", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


AUTH_PUBLIC_PATHS = {"/api/auth/status", "/api/auth/login", "/api/auth/logout"}


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    settings = get_auth_settings()
    if (
        not settings.required
        or not request.url.path.startswith("/api/")
        or request.url.path in AUTH_PUBLIC_PATHS
    ):
        return await call_next(request)

    if not settings.configured:
        return JSONResponse(
            status_code=503,
            content={"detail": "Authentication is required but not configured"},
        )

    username = verify_session(request.cookies.get(COOKIE_NAME, ""), settings)
    if not username:
        return JSONResponse(status_code=401, content={"detail": "Authentication required"})

    request.state.auth_user = username
    return await call_next(request)


class ApiConfig(BaseModel):
    api_key: str = Field(min_length=1)
    api_secret: str = Field(min_length=1)
    testnet: bool = False


class GridConfig(BaseModel):
    symbol: str
    direction: str
    upper_price: float
    lower_price: float
    grid_count: int
    total_investment: float
    leverage: int
    grid_mode: str = "arithmetic"
    trigger_price: float | None = None
    stop_loss_price: float | None = None
    take_profit_price: float | None = None


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    code: str = Field(min_length=6, max_length=12)


@app.get("/")
def index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


@app.get("/api/auth/status")
def auth_status(request: Request):
    settings = get_auth_settings()
    username = None
    if settings.configured:
        username = verify_session(request.cookies.get(COOKIE_NAME, ""), settings)

    response = {
        "required": settings.required,
        "configured": settings.configured,
        "authenticated": bool(username) or not settings.required,
        "username": username,
    }
    if (
        settings.required
        and settings.configured
        and not username
        and _parse_bool(os.getenv("AUTH_SHOW_TOTP_SETUP"))
    ):
        response["totp_uri"] = build_totp_uri(settings)
        response["totp_secret"] = settings.totp_secret
    return response


@app.post("/api/auth/login")
def auth_login(payload: LoginRequest, response: Response):
    settings = get_auth_settings()
    if not settings.required:
        return {"ok": True, "message": "Authentication is disabled"}
    if not settings.configured:
        raise HTTPException(status_code=503, detail="Authentication is required but not configured")
    if payload.username != settings.username:
        raise HTTPException(status_code=401, detail="Invalid username, password, or code")
    if not verify_password(payload.password, settings.password_hash):
        raise HTTPException(status_code=401, detail="Invalid username, password, or code")
    if not verify_totp(payload.code, settings.totp_secret):
        raise HTTPException(status_code=401, detail="Invalid username, password, or code")

    token = create_session(settings.username, settings)
    response.set_cookie(
        COOKIE_NAME,
        token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        max_age=43200,
    )
    return {"ok": True, "message": "Logged in"}


@app.post("/api/auth/logout")
def auth_logout(response: Response):
    response.delete_cookie(COOKIE_NAME)
    return {"ok": True, "message": "Logged out"}


@app.get("/api/config")
def get_config():
    api_key = _api_config.get("api_key", "")
    return {
        "api_key": _mask_api_key(api_key),
        "testnet": _api_config.get("testnet", False),
        "configured": bool(api_key),
        "source": _api_config.get("source", "none"),
        "storage": storage_backend(),
    }


@app.post("/api/config")
def set_config(cfg: ApiConfig):
    global _client, _api_config

    candidate = BybitClient(cfg.api_key.strip(), cfg.api_secret.strip(), cfg.testnet)
    try:
        balance = candidate.get_balance()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to connect to Bybit: {exc}") from exc

    if balance.get("retCode") != 0:
        raise HTTPException(
            status_code=400,
            detail=f"API verification failed: {balance.get('retMsg', 'unknown error')}",
        )

    saved_config = cfg.model_dump()
    try:
        _save_api_config(saved_config)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=f"API verified but not saved securely: {exc}") from exc

    _client = candidate
    _api_config = {**saved_config, "source": "file"}
    return {"ok": True, "message": "API config saved"}


@app.get("/api/price/{symbol}")
def get_price(symbol: str):
    client = _client or BybitClient("", "", bool(_api_config.get("testnet", False)))
    resp = client.get_ticker(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch ticker"))

    data = resp["result"]["list"][0]
    return {
        "symbol": data["symbol"],
        "last_price": data["lastPrice"],
        "index_price": data.get("indexPrice", ""),
        "mark_price": data.get("markPrice", ""),
        "price_24h_pcnt": data.get("price24hPcnt", "0"),
        "volume_24h": data.get("volume24h", "0"),
    }


@app.get("/api/balance")
def get_balance():
    client = _get_client()
    resp = client.get_balance()
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch balance"))

    wallets = resp["result"]["list"][0].get("coin", [])
    usdt = next((item for item in wallets if item.get("coin") == "USDT"), None)
    if not usdt:
        return {"available": "0", "equity": "0", "unrealised_pnl": "0"}

    return {
        "available": usdt.get("availableToWithdraw") or usdt.get("walletBalance", "0"),
        "equity": usdt.get("equity", "0"),
        "unrealised_pnl": usdt.get("unrealisedPnl", "0"),
    }


@app.get("/api/positions/{symbol}")
def get_positions(symbol: str):
    client = _get_client()
    resp = client.get_positions(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch positions"))

    positions = []
    for item in resp["result"].get("list", []):
        try:
            size = float(item.get("size", 0))
        except (TypeError, ValueError):
            size = 0

        if size <= 0:
            continue

        positions.append(
            {
                "side": item.get("side", ""),
                "size": item.get("size", "0"),
                "entry_price": item.get("avgPrice", "0"),
                "mark_price": item.get("markPrice", "0"),
                "unrealised_pnl": item.get("unrealisedPnl", "0"),
                "leverage": item.get("leverage", "0"),
                "liq_price": item.get("liqPrice", ""),
            }
        )

    return {"positions": positions}


@app.post("/api/grid/start")
async def start_grid(cfg: GridConfig):
    client = _get_client()
    symbol = cfg.symbol.upper().strip()
    direction = cfg.direction.lower().strip()
    grid_mode = cfg.grid_mode.lower().strip()
    existing_engine = _engines.get(symbol)

    if existing_engine and existing_engine.running:
        raise HTTPException(status_code=400, detail=f"A grid is already running for {symbol}")
    if cfg.upper_price <= cfg.lower_price:
        raise HTTPException(status_code=400, detail="upper_price must be greater than lower_price")
    if cfg.grid_count < 2 or cfg.grid_count > 100:
        raise HTTPException(status_code=400, detail="grid_count must be between 2 and 100")
    if cfg.total_investment <= 0:
        raise HTTPException(status_code=400, detail="total_investment must be greater than 0")
    if cfg.leverage < 1 or cfg.leverage > 125:
        raise HTTPException(status_code=400, detail="leverage must be between 1 and 125")
    if direction not in {"long", "short", "neutral"}:
        raise HTTPException(status_code=400, detail="direction must be long, short, or neutral")
    if grid_mode not in {"arithmetic", "geometric"}:
        raise HTTPException(status_code=400, detail="grid_mode must be arithmetic or geometric")

    engine_config = cfg.model_dump()
    engine_config["symbol"] = symbol
    engine_config["direction"] = direction
    engine_config["grid_mode"] = grid_mode
    engine = GridEngine(client, engine_config)

    try:
        await engine.initialize()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _engines[symbol] = engine
    engine.start()
    return {"ok": True, "message": f"{symbol} {direction} grid started"}


@app.post("/api/grid/stop")
async def stop_grid():
    running_symbols = [symbol for symbol, engine in _engines.items() if engine.running]
    if not running_symbols:
        raise HTTPException(status_code=400, detail="No active grid")
    if len(running_symbols) > 1:
        raise HTTPException(status_code=400, detail="Multiple grids are running; stop by symbol")

    return await _stop_grid(running_symbols[0])


@app.post("/api/grid/stop/{symbol}")
async def stop_grid_symbol(symbol: str):
    return await _stop_grid(symbol.upper().strip())


@app.post("/api/grid/stop-all")
async def stop_all_grids():
    running = [engine for engine in _engines.values() if engine.running]
    if not running:
        raise HTTPException(status_code=400, detail="No active grid")

    for engine in running:
        await engine.stop()

    return {"ok": True, "message": "All grids stopped and open orders cancelled"}


async def _stop_grid(symbol: str):
    engine = _engines.get(symbol)
    if not engine or not engine.running:
        raise HTTPException(status_code=400, detail="No active grid")

    await engine.stop()
    return {"ok": True, "message": f"{symbol} grid stopped and open orders cancelled"}


@app.get("/api/grid/status")
def grid_status():
    statuses = [_engine_status(engine) for engine in _engines.values()]
    return {
        "running": any(status["running"] for status in statuses),
        "engine_count": len(statuses),
        "running_count": sum(1 for status in statuses if status["running"]),
        "engines": statuses,
    }


@app.get("/api/grid/status/{symbol}")
def grid_symbol_status(symbol: str):
    symbol = symbol.upper().strip()
    engine = _engines.get(symbol)
    if not engine:
        return {"running": False, "symbol": symbol}
    return _engine_status(engine)


def _engine_status(engine: GridEngine) -> dict:
    return engine.get_status()


@app.get("/api/orders/history/{symbol}")
def order_history(symbol: str):
    client = _get_client()
    resp = client.get_order_history(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch order history"))

    orders = [
        {
            "order_id": item.get("orderId", ""),
            "side": item.get("side", ""),
            "price": item.get("price", "0"),
            "qty": item.get("qty", "0"),
            "status": item.get("orderStatus", ""),
            "created_time": item.get("createdTime", ""),
        }
        for item in resp["result"].get("list", [])
    ]
    return {"orders": orders}


@app.get("/api/orders/open/{symbol}")
def open_orders(symbol: str):
    client = _get_client()
    resp = client.get_open_orders(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch open orders"))

    orders = [
        {
            "order_id": item.get("orderId", ""),
            "order_link_id": item.get("orderLinkId", ""),
            "side": item.get("side", ""),
            "price": item.get("price", "0"),
            "qty": item.get("qty", "0"),
            "status": item.get("orderStatus", ""),
            "reduce_only": item.get("reduceOnly", False),
            "created_time": item.get("createdTime", ""),
        }
        for item in resp["result"].get("list", [])
    ]
    return {"orders": orders}


@app.post("/api/orders/cancel-all/{symbol}")
def cancel_all_symbol_orders(symbol: str):
    client = _get_client()
    resp = client.cancel_all_orders(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to cancel open orders"))
    return {"ok": True, "message": f"All open orders for {symbol.upper()} were cancelled"}


def _get_client() -> BybitClient:
    if not _client:
        raise HTTPException(status_code=400, detail="Please configure API Key first")
    return _client


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
