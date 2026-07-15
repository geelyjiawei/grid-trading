# Vue + Rust 正式版使用说明

这套发布方式会保留旧 Python 容器作为回滚点，但正式端口只允许一个引擎占用。切换脚本不会导入旧 Python 网格台账，也不会取消或修改人工持仓、人工挂单。

## 首次切换条件

- 旧 Python 进程中没有运行中网格、内存引擎或仍归属旧网格的活动订单记录。
- `.env` 和加密的 `api_config.json` 只保存在服务器，不提交到 GitHub。
- 服务器的旧项目位于 `/opt/grid-trading`，旧容器名为 `grid-trading`。
- 待发布提交已经通过 GitHub Actions。

人工仓位和人工挂单可以存在。脚本会在预检前、预检后和切换后读取交易所快照；数量、订单 ID、价格或状态发生变化时，发布失败并恢复旧容器。

## 生成登录凭据

在 Rust 项目目录执行：

```bash
python3 scripts/provision-rust-auth.py \
  --env-file .env
```

密码、TOTP 密钥和管理令牌只写服务器文件，权限为 `0600`。使用认证器添加 TOTP 后，应安全保存或删除一次性凭据文件。

## 正式切换

```bash
export GRID_RUST_PRODUCTION_EXPECTED_COMMIT="$(git rev-parse HEAD)"
export GRID_RUST_CUTOVER_CONFIRM=rust-replaces-python
sh scripts/deploy-rust-production.sh
```

脚本按以下顺序执行：

1. 校验提交、干净工作区、认证配置和 Compose 安全限制。
2. 只读检查旧网格状态，并保存交易所持仓及挂单快照。
3. 在 `127.0.0.1:18001` 启动带实盘运行时但无策略的隔离预检实例。
4. 确认预检没有改变交易所数据后才停止旧 Python 容器。
5. 在 `127.0.0.1:8000` 启动 Vue + Rust，验证认证、运行时、活动策略数和容器权限。
6. 再次核对持仓与挂单；任何失败都会停止 Rust 并恢复旧 Python 容器。

切换证据保存在未跟踪的 `release-backups/`，Rust 数据保存在未跟踪的 `data-rust-production/`。

## 验证

```bash
curl -sS http://127.0.0.1:8000/healthz
docker ps --filter name=grid-trading
```

健康响应必须包含：

```json
{"ok":true,"runtime":"rust","trading_enabled":true,"runtime_ready":true,"active_strategies":0}
```

登录后先用小额、单交易对策略验证。Rust 会把启动前已有仓位记为基线仓位，新网格只核算并管理自己的增量，不会把旧仓计入网格数量。

## 回滚

必须先在页面停止所有 Rust 策略，并等待 `/healthz` 显示 `active_strategies: 0`：

```bash
export GRID_RUST_ROLLBACK_CONFIRM=stop-rust-start-python
sh scripts/rollback-to-python.sh
```

回滚脚本在活动策略不为零时会拒绝执行，避免在未撤完策略订单时切换所有权。
