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

然后在 Google Authenticator 里选择“输入设置密钥”，账户名可填 `grid-trading`，密钥填 `TOTP_SECRET`。

如果以后配置了 HTTPS，把 `AUTH_COOKIE_SECURE=true`。

如果你确实要在网页里保存 API，需要生成 `GRID_CONFIG_KEY`：

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

然后填入 `.env`。

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

## 5. 更新部署

上传新代码后执行：

```bash
docker compose up -d --build
```

## 6. 安全提醒

- 不要把 `.env` 发给别人。
- 不要开启 API 提现权限。
- 服务器安全组只开放必要端口。
- 如果公网访问，建议使用 HTTPS。
- 当前网格运行状态仍在内存中，容器重启后不会自动恢复旧网格；交易所残留挂单/持仓需要先检查。
