# Virtuals-Launch-Hunter

Virtuals-Launch-Hunter 是一个面向 Base 链的实时监控与回扫分析工具，支持三进程低延迟架构（writer/realtime/backfill），用于观察项目盘面、买家行为、分钟消耗与钱包持仓。

## 功能总览
- 实时监听：WS 新块/日志监听，快速捕捉交易。
- 自动补漏：backfill 周期回扫，修复实时链路漏单。
- 手动回扫：支持 UTC+8 时间区间回扫。
- 项目管理：UI 可切换/保存/删除项目（保留历史数据）。
- 钱包管理：UI 可新增/删除监控钱包，支持单钱包重算。
- 统计面板：
  - 分钟消耗 SpentV
  - 大户榜
  - 我的钱包持仓
  - 交易录入延迟
  - 项目累计税收(V)

## 架构说明（三进程）
- `writer`：唯一写库进程，同时提供 API/UI。
- `realtime`：实时监听 + 回执解析，写入事件总线。
- `backfill`：自动/手动回扫，写入事件总线。

说明：实时与回扫不直接写主库，由 writer 统一落库，降低写锁竞争。

## 目录（发布最小集合）
- `virtuals_bot.py`：后端主程序
- `dashboard.html`：前端页面
- `favicon-vpulse.svg`：浏览器图标
- `config.example.json`：配置模板（API 已脱敏）
- `requirements.txt`：依赖
- `start_3roles.ps1`：一键启动三进程
- `stop_3roles.ps1`：一键停止三进程
- `RELEASE_v3.1.0_更新说明.md`：历史版本更新日志
- `RELEASE_v3.1.0_使用说明.md`：历史版本使用教程
- `需求文档_v3.0.0.md`：需求说明模板

## 环境要求
- Windows 10/11 + PowerShell
- Python 3.10+
- Base RPC 节点（建议）：
  - 1 个 WS（实时）
  - 1 个 HTTP（实时补充）
  - 1 个 HTTP（回扫专用，建议独立）

## 快速开始
1. 安装依赖
```powershell
cd virtual
python -m pip install -r requirements.txt
```

2. 准备配置
```powershell
copy .\config.example.json .\config.json
```

3. 修改最少参数
- `WS_RPC_URL`
- `HTTP_RPC_URL`
- `BACKFILL_HTTP_RPC_URL`

4. 启动
```powershell
.\start_3roles.ps1
```

5. 打开 UI
- `http://127.0.0.1:8080/`

## 手动启动（排障）
```powershell
python virtuals_bot.py --config .\config.json --role writer
python virtuals_bot.py --config .\config.json --role realtime
python virtuals_bot.py --config .\config.json --role backfill
```

## 服务器部署（前后端分离/同域）
- 详细部署文档：`deploy/README_DEPLOY.md`
- 一键安装脚本（Ubuntu22.04，同域 `/api` 反代）：`deploy/install_ubuntu22_oneclick.sh`
- systemd 模板：`deploy/systemd/vpulse@.service`
- Nginx 配置示例：
  - 分域：`deploy/nginx/vpulse-split-app.conf` + `deploy/nginx/vpulse-split-api.conf`
  - 同域 `/api`：`deploy/nginx/vpulse-same-domain.conf`
- 前端可通过 `dashboard.html` 中的
  - `<meta name="vpulse-api-base" content="...">`
  指定 API 基地址（如 `https://api.example.com` 或 `/api`）。

## 关键 API
- `GET /health`：系统健康状态
- `GET /meta`：项目/钱包/运行参数
- `GET /minutes`：分钟消耗
- `GET /leaderboard`：大户榜
- `GET /mywallets`：我的钱包
- `GET /event-delays`：录入延迟
- `GET /project-tax`：项目累计税收
- `POST /scan-range`：发起区间回扫
- `POST /scan-jobs/{job_id}/cancel`：取消回扫任务

## 常见问题
- 端口占用（10048）：说明 8080 已被占用，停掉旧进程或改 `API_PORT`。
- 回扫限制错误（`eth_getLogs is limited`）：调小 `BACKFILL_CHUNK_BLOCKS`。
- 没有实时数据：检查 realtime 进程、WS 节点连通性、项目地址正确性。

## 版本文档
- `RELEASE_v3.1.0_更新说明.md`
- `RELEASE_v3.1.0_使用说明.md`
- `RELEASE_v3.0.0_更新说明.md`（历史）
- `RELEASE_v3.0.0_使用说明.md`（历史）
