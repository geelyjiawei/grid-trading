# 服务器部署说明

## 1. 上传代码

把整个 `grid-trading` 目录上传到服务器，例如：

```bash
/opt/grid-trading
```

## 2. 准备环境变量

复制模板：

```bash
cp .env.example .env
```

编辑 `.env`：

```bash
GRID_EXCHANGE=bybit
BYBIT_API_KEY=你的Key
BYBIT_API_SECRET=你的Secret
BYBIT_TESTNET=false

# 兜底手续费率：仅在交易所成交明细拿不到真实手续费时使用。
# Binance USDT-M 合约普通默认：挂单 0.02%，吃单 0.05%。
GRID_MAKER_FEE_RATE=0.0002
GRID_TAKER_FEE_RATE=0.0005

# 如果使用 Binance，把 GRID_EXCHANGE 改为 binance，并填写：
BINANCE_API_KEY=你的Key
BINANCE_API_SECRET=你的Secret
BINANCE_TESTNET=false
```

建议服务器部署优先使用环境变量，不要在网页里保存 API。API 权限不要开启提现，最好在交易所后台限制服务器 IP。

## 2.1 配置网页登录 + Google 验证码

生成管理员密码哈希：

```bash
python backend/auth.py hash "换成你的强密码"
```

生成 Google Authenticator 密钥：

```bash
python backend/auth.py totp
```

生成会话密钥：

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

把这些值写入 `.env`：

```bash
AUTH_REQUIRED=true
ADMIN_USERNAME=admin
ADMIN_PASSWORD_HASH=上面生成的密码哈希
TOTP_SECRET=上面生成的验证码密钥
SESSION_SECRET=上面生成的会话密钥
AUTH_COOKIE_SECURE=false
```

`SESSION_SECRET` 仅供当前 Python 生产服务使用。Vue + Rust 候选服务采用服务端不透明随机会话，只保存令牌摘要，不读取该值；在正式切换完成前仍需保留它，避免影响旧服务。

然后在 Google Authenticator 里选择“输入设置密钥”，账户名可填 `grid-trading`，密钥填 `TOTP_SECRET`。

如果以后配置了 HTTPS，把 `AUTH_COOKIE_SECURE=true`。

下面的网页加密配置仅供旧 Python 服务兼容。Vue + Rust 候选版不会从浏览器接收密钥，只会在启动时读取服务器 `.env`。如果旧服务仍需网页保存 API，才需要生成 `GRID_CONFIG_KEY`：

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

然后填入 `.env`。

如果你使用普通口令作为 `GRID_CONFIG_KEY`，建议同时生成并配置 `GRID_CONFIG_SALT`：

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

公网服务器不建议直接用 HTTP 在网页里输入 API Key。生产环境请使用 Caddy / Nginx 配置 HTTPS，并把 `AUTH_COOKIE_SECURE=true`。

## 2.2 Vue + Rust 候选服务

候选服务与当前生产服务隔离，默认监听服务器本机 `127.0.0.1:8001`，并使用独立目录 `data-rust-preview/`：

```bash
docker compose -f docker-compose.rust-vue.yml up -d --build
curl --fail http://127.0.0.1:8001/healthz
curl --fail http://127.0.0.1:8001/api/config
curl --fail http://127.0.0.1:8001/api/grid/status
```

第一阶段必须保持：

```bash
GRID_RUST_TRADING_ENABLED=false
```

此时可以核对 Vue 页面、交易所配置状态和只读 API，但所有启动/停止写请求都会拒绝执行。候选服务读取 Binance、AsterDEX 和 Bybit 的环境变量配置，真实 `.env` 不得提交到 GitHub。

启用 Rust 实盘写入前必须同时满足：网页登录保护配置完整、交易所凭据有效、持久状态恢复无异常、隔离影子核验通过，并已准备生产回滚方案。不要让 Python 与 Rust 两个引擎同时管理同一交易所和交易对。

## 3. Docker Compose 启动

```bash
docker compose up -d --build
```

查看日志：

```bash
docker compose logs -f
```

停止：

```bash
docker compose down
```

## 4. 访问

浏览器打开：

```text
http://服务器IP:8000
```

生产环境建议再加 Nginx / Caddy 反向代理和 HTTPS。

默认不开放跨域访问。如果确实需要从其他域名访问 API，在 `.env` 中明确设置：

```bash
CORS_ALLOWED_ORIGINS=https://你的域名
```

## 5. 更新部署

上传新代码后执行：

```bash
docker compose up -d --build
```

## 6. 安全提醒

- 不要把 `.env` 发给别人。
- 不要开启 API 提现权限。
- 服务器安全组只开放必要端口。
- 如果公网访问，必须使用 HTTPS，避免 API Key 和登录 Cookie 明文传输。
- 网格运行状态会保存到 `GRID_STATE_FILE`，容器重启后会尝试恢复；但重启或异常后仍建议先查看页面风险提示、挂单和持仓。
