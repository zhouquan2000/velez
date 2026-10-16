# Velez — Oliver Velez 大象柱量化交易系统

基于 [Oliver Velez](https://en.wikipedia.org/wiki/Oliver_Velez) 交易法则（"大象柱" Elephant Bar 形态 + 8 条交易法则中的 7 条）构建的美股日内量化交易程序，通过 IBKR（Interactive Brokers）的 TWS / IB Gateway API 自动执行交易。

> ⚠️ **免责声明**：本项目仅供**学习与研究**使用，不构成任何投资建议。代码内包含**真实下单逻辑**（市价单 / 止损单 / 追踪止损）。使用 Live 账户运行将**真实动用资金并产生盈亏**，风险自担。请务必先在 **Paper（模拟）账户**上充分测试，确认理解全部行为后再考虑实盘。

---

## 目录

- [功能特性](#功能特性)
- [策略说明](#策略说明)
- [支持的股票](#支持的股票)
- [环境要求](#环境要求)
- [安装](#安装)
- [IBKR 网关配置](#ibkr-网关配置)
  - [方式一：IB Gateway（推荐）](#方式一ib-gateway推荐)
  - [方式二：TWS](#方式二tws)
- [账户模式（Paper / Live）](#账户模式paper--live)
- [市场数据订阅](#市场数据订阅)
- [使用方法](#使用方法)
- [命令行参数](#命令行参数)
- [目录结构](#目录结构)
- [日志](#日志)
- [风险提示](#风险提示)
- [常见问题](#常见问题)
- [License](#license)

---

## 功能特性

- **大象柱（Elephant Bar）信号识别**：基于实体长度 / 成交量 / 实体纯度三维度识别爆发性 K 线，作为入场信号。
- **Oliver Velez 7 条法则**：实现了 8 条法则中的 7 条（`Law #7: 200MA Reversion` 因逆势风险高、必要性低而刻意未实现）。
- **全自动交易闭环**：入场 → 保护性止损 → 追踪止损 → 止盈 → 收盘清仓，全程由程序治理。
- **持仓自愈对账**：启动时与柜台事实对齐，接管实盘持仓与在途订单；异常状态下自动补防 / 撤单 / 强平。
- **VIX 环境自适应**：根据 VIX 水平动态调整风控参数。
- **多品种并行**：8 只股票可各自独立进程运行（tmux 或 Windows Terminal 分窗）。
- **完整日志审计**：系统级 + 个股级日志落盘，含资金报告、信号、下单、治理全程。

## 策略说明

策略核心是 **Oliver Velez 的"大象柱"（Elephant Bar）**：一根相对 ATR(14) 异常放大的实体 K 线，代表机构资金的爆发性介入，作为入场触发器。

每只股票有一组个性化"指纹"参数（`STOCK_CONFIGS`，见 `velez_bot/shared.py`）：

| 参数 | 含义 |
|---|---|
| `eb_range_mult` | 实体长度相对 ATR 的倍数（爆发力门槛） |
| `eb_vol_mult` | 成交量相对 20 根均量的倍数（资金确认） |
| `eb_body_ratio` | 实体占整根 K 线的比例（纯度） |
| `risk_unit` | 每笔交易的风险单元（美元） |
| `max_qty` | 单只股票最大持仓股数 |

信号由 **2 分钟 K 线**（历史数据）计算，交易循环由 **5 秒实时 bar**（`reqRealTimeBars`）驱动。

## 支持的股票

默认内置 8 只美股（均为 NASDAQ 上市），按波动性分组：

| 波动组 | 股票 | 特点 |
|---|---|---|
| 低波动 | AAPL、MSFT | 宽松门槛，捕捉稀缺信号 |
| 中波动 | GOOG、AMZN、AMD | 标准门槛 |
| 高波动 | META、NVDA、TSLA | 严格门槛，过滤噪音 |

## 环境要求

- **Python 3.12**（⚠️ 不可用 3.13 / 3.14：依赖 `eventkit==1.0.3` 在 3.14 下 import 即崩溃，锁定版 `pandas/numpy` 在 3.14 无预编译 wheel）
- **IBKR 账户** + TWS 或 IB Gateway（任一即可）
- 操作系统：Windows / Linux / WSL 均可（非 Windows 系统已自动禁用 `winsound` 报警）

## 安装

```bash
# 1. 克隆仓库
git clone <your-repo-url>
cd velez

# 2. 创建 Python 3.12 虚拟环境
python3.12 -m venv .venv

# 3. 激活环境
# Linux / WSL:
source .venv/bin/activate
# Windows:
.venv\Scripts\Activate.ps1

# 4. 安装依赖
pip install -r requirements.txt
```

> 说明：`requirements.txt` 中的 `ibapi` 与 `futu` 为冗余项，`ib_insync 0.9.x` 自带 IB API 协议实现，不装也不影响运行。

## IBKR 网关配置

程序通过 TCP 连接本机运行的 TWS / IB Gateway，端口是**区分 Paper / Live 的标识**：

| 网关 | Live 端口 | Paper 端口 |
|---|---|---|
| IB Gateway | 4001 | 4002 |
| TWS（GUI） | 7496 | 7497 |

### 方式一：IB Gateway（推荐）

IB Gateway 是 IBKR 官方的轻量网关，专为 API 设计，适合无人值守。

1. 从 [IB Gateway 下载页](https://www.interactivebrokers.com/en/trading/ibgateway-latest.php) 下载并安装（Windows 版默认装到 `C:\Jts\ibgateway\`）。
2. 启动后登录：
   - **API 类型**：选 `IB API`（不是 FIX CTCI）
   - **Trading Mode**：Paper 选 `Paper Trading`，Live 选 `Live Trading`
3. 登录后进入 `Configure → Settings → API → Settings`：
   - 取消勾选 **Read Only API**（首次手动登录默认只读，不取消则所有下单报 `Error 321`）
   - 确认 **Socket port** = `4002`（Paper）或 `4001`（Live）
   - **Download open orders on connection** 建议勾选（程序启动需同步在途单）

### 方式二：TWS

1. 下载安装 Trader Workstation（TWS）。
2. 登录后进入 `Configure → Settings → API → Settings`（新版在 `Global Configuration` 里）：
   - 勾选 **Enable ActiveX and Socket Clients**
   - 取消勾选 **Read Only API**
   - **Socket port** 设 `7497`（Paper）或 `7496`（Live）
3. 启动程序时用 `--port` 指定对应端口（见下文）。

## 账户模式（Paper / Live）

程序不区分账户类型——**唯一区别是端口**。切换账户只需改 `--port`（及 Gateway 登录时选的 Trading Mode）。

### Paper（模拟）账户

```bash
# IB Gateway（默认，端口已预设为 4002，无需 --port）
python main.py --symbol AAPL

# 或显式指定（TWS Paper）
python main.py --symbol AAPL --port 7497
```

- Gateway 登录选 `Paper Trading`；TWS 登录选 Paper 模式。
- Paper 账户**无需二步验证（IB Key）**。
- 下单是模拟的，不会动用真实资金（但下单逻辑完全真实）。

### Live（实盘）账户

```bash
# IB Gateway Live
python main.py --symbol AAPL --port 4001

# TWS Live
python main.py --symbol AAPL --port 7496
```

- Gateway 登录选 `Live Trading`。
- Live 账户登录需要 **IB Key 二步验证**（手机 app 确认，180 秒内）。
- ⚠️ **会真实下单、动用真实资金**。务必先确认策略逻辑理解无误。

> **关于 `--host`**：默认 `127.0.0.1`（程序与 TWS/Gateway 同一台机器）。若网关跑在另一台机器，用 `--host <网关IP>`，并在网关的 API 设置里把程序所在机器的 IP 加入 **Trusted IP** 列表。

## 市场数据订阅

程序依赖两类数据，权限是**分开的两套**：

| 数据类型 | 接口 | 是否需要付费订阅 |
|---|---|---|
| 历史数据（2 分钟 K 线、VIX 15 分钟线） | `reqHistoricalData` | 否，默认可用 |
| 实时行情（5 秒 bar，驱动交易循环） | `reqRealTimeBars` | **是，需要 Level 1 订阅** |

实时行情（Level 1）订阅：登录 [Client Portal](https://ndcdyn.interactivebrokers.com) → `Settings → Market Data Subscriptions`，订阅美股三大实时行情（Non-Pro 价格）：

| 订阅项 | 覆盖 | 价格 |
|---|---|---|
| NASDAQ (Network C/UTP) (NPL1) | NASDAQ 上市股票（本项目 8 只全在此） | $1.50/月 |
| NYSE (Network A/CTA) (NPL1) | NYSE 上市股票 | $1.50/月 |
| NYSE American, BATS, ARCA, IEX (Network B) (NPL1) | 区域交易所（SMART 路由可能走这些） | $1.50/月 |

- **最低要求**：至少订阅 NASDAQ Network C（本项目 8 只股票全为 NASDAQ）。
- **推荐**：三档全订（共 $4.50/月），覆盖 SMART 智能路由可能成交的所有交易所。
- 未订阅实时行情时，`reqRealTimeBars` 会返回空（`Error 420`），交易循环无法启动——这是"程序跑不起来"的常见原因，先查订阅。

## 使用方法

### 单只股票

```bash
cd velez
source .venv/bin/activate        # Windows: .venv\Scripts\Activate.ps1
python main.py --symbol AAPL      # 默认 clientId 31
```

或使用脚本（自动激活环境）：

```bash
./run_one.sh AAPL 31             # 参数：股票代码 客户端ID
```

### 8 只股票全量

| 脚本 | 方式 | 说明 |
|---|---|---|
| `./run_all.sh` | tmux | 8 只股票各占一个 tmux 窗口（Linux / WSL 推荐） |
| `./run_all_wt.sh` | Windows Terminal | 8 只股票各占一个 tab（Windows 推荐） |

内置 8 只股票的 clientId 映射（31–38）：

```
AAPL:31  AMD:32  AMZN:33  GOOG:34  META:35  MSFT:36  NVDA:37  TSLA:38
```

> clientId 必须唯一：同一 TWS/Gateway 连接下的每个程序实例要使用不同的 clientId，否则会互相挤下线。

### 停止

`Ctrl + C` 会触发**紧急清仓**（撤销挂单并平掉持仓）后安全退出。

### 运行前检查清单

启动前请逐项确认：

| 检查项 | 如何确认 |
|---|---|
| tmux 已安装（`run_all.sh` 依赖） | `tmux -V`；未装则 `sudo apt install tmux` |
| TWS / IB Gateway 已登录 | 网关窗口 Connection Status 显示 `connected`，且能读到账户数据 |
| 美股交易时段内 | 美东 9:30–16:00（收盘后启动会立即退出，属正常行为） |
| live 账号未在别处登录 | 手机 app / Client Portal 网页都登出，避免 Error 162 |

### tmux 操作（run_all.sh）

`run_all.sh` 启动后会自动 `tmux attach` 进入总控台，底部显示 8 个窗口（AAPL / AMD / ... / TSLA）：

| 操作 | 作用 |
|---|---|
| `Ctrl+b` 然后 `d` | 脱离（detach），程序继续后台运行 |
| `Ctrl+b` + `0`–`7` | 跳到第 N 个窗口（对应 8 只股票） |
| `Ctrl+b` + `n` / `p` | 下一个 / 上一个窗口 |
| `tmux attach -t velez` | 重新进入会话 |
| `tmux kill-session -t velez` | 全部关停 |

### 推荐启动流程

1. 先 `./run_one.sh AAPL` 跑单只，观察约 10 分钟日志，确认信号 / 下单 / 止损全链路正常。
2. 确认无误后再 `./run_all.sh` 上 8 只全量。

单只先验证比 8 只一起上更容易定位问题。

## 命令行参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--symbol` | `AAPL` | 股票代码（须在 `STOCK_CONFIGS` 中定义） |
| `--clientid` | `31` | IBKR 客户端 ID（多实例时须唯一） |
| `--host` | `127.0.0.1` | TWS / IB Gateway 主机地址 |
| `--port` | `4002` | 网关端口（Paper Gateway=4002，TWS Paper=7497，Live Gateway=4001，TWS Live=7496） |

## 目录结构

```
velez/
├── main.py                 # 入口：python main.py --symbol AAPL
├── velez_bot/              # 主交易程序
│   ├── app.py              # 启动流程、交易时段、实时链路
│   ├── shared.py           # 连接、参数、STOCK_CONFIGS、日志、收盘守护
│   ├── trading_context.py  # 核心交易逻辑（信号、下单、治理、追踪止损）
│   ├── context_snapshot.py # 持仓/挂单快照
│   ├── events.py           # 成交/bar 事件回调
│   └── __init__.py
├── backtest/               # 回测模块（未完成，开发中）
├── run_one.sh              # 单只股票启动脚本
├── run_all.sh              # tmux 启动 8 只
├── run_all_wt.sh           # Windows Terminal 启动 8 只
├── requirements.txt
└── log_file/               # 运行日志（自动生成）
```

## 日志

程序运行日志落盘在 `log_file/` 目录，包括：

- 系统级日志（资金报告、VIX、收盘清仓）
- 个股级日志（信号、下单、持仓治理、追踪止损）
- `VIX-15m-<日期>.csv`、个股 2 分钟原始 K 线 CSV

日志时间戳采用**美东时间（America/New_York）**。

## 风险提示

1. **真实下单逻辑**：本程序的入场 / 止损 / 止盈 / 加仓 / 强平全部是真实订单（market / stop 单）。即使是 Paper 账户，订单逻辑也与实盘完全一致。
2. **Live 账户会真实动用资金**：切换 Live 前务必在 Paper 上充分测试。
3. **断点调试风险**：用调试器断点停在交易逻辑里，恢复执行可能触发下单。建议先用回测逻辑做纯逻辑验证，再连实盘网关动态调试。
4. **市场数据会话限制**：IBKR 不允许同一账号的 Live 与 Paper 会话**同时**接收市场数据（报 `Error 162`）。运行 Paper 程序时，请确保 live 账号没有在手机 app / Client Portal 网页同时登录。
5. **订阅费用**：实时行情订阅每月计费（详见[市场数据订阅](#市场数据订阅)），费用在 IBKR 账单中结算。

## 常见问题

### Error 162：历史数据报「Trading TWS session is connected from a different IP address」

- **症状**：账户数据（`reqAccountSummary`）能正常读取，但历史数据（`reqHistoricalData`）返回空并报 Error 162；程序日志出现「历史数据初始化失败，进入停摆模式」。
- **根因**：你的 live 账号在**别处**还有活跃会话。IBKR 市场数据服务不允许同一凭证的 Live 与 Paper 会话同时收数据——用 live 账号登录 Paper Trading 时，两者是同一套凭证。
- **解决**：断开所有其他登录点——手机 IBKR app（**真正 Logout，划掉后台进程 ≠ 登出**）、Client Portal 网页、其他机器上的 TWS/Gateway。全部登出后稍等片刻重试。

### Error 321：下单报「The API interface is currently in Read-Only mode」

- **症状**：程序能连接、能读数据，但一发起下单就报 Error 321。
- **根因**：IB Gateway 首次手动登录时 API 默认是只读模式（安全默认）。
- **解决**：Gateway 里 `Configure → Settings → API → Settings`，取消勾选 **Read Only API**，点 Apply / OK 生效。

### Error 420 / 10089：实时数据拿不到（tick 全 nan，5 秒 bar 为 0 根）

- **症状**：`reqMktData` 返回 bid/ask/last 全 nan，`reqRealTimeBars` 收到 0 根 bar，报 "No market data permissions" / "requires additional subscription"。
- **根因**：未订阅实时行情（Level 1）。注意**历史数据和实时行情是两套独立权限**，历史数据默认可用，实时行情需付费订阅。
- **解决**：Client Portal → Settings → Market Data Subscriptions，订阅对应 Level 1（本项目 8 只股票为 NASDAQ，至少订阅 NASDAQ Network C/UTP）。

### import ib_insync 直接崩溃（RuntimeError）

- **症状**：`import ib_insync`（或其依赖 eventkit）时抛 `RuntimeError`。
- **根因**：用了 Python 3.14（eventkit 1.0.3 在 3.14 下 import 即崩）。
- **解决**：改用 **Python 3.12**（见[环境要求](#环境要求)）。不要试图在 3.14 上强跑。

### Windows 11 找不到 IB Gateway 图标

- **症状**：安装完成但在开始菜单 / 桌面找不到 IB Gateway 图标。
- **根因**：开启 OneDrive 桌面备份后，桌面快捷方式落在 `C:\Users\<用户名>\OneDrive\Desktop\`，不在传统 Desktop 目录。
- **解决**：主程序在 `C:\Jts\ibgateway\<版本号>\ibgateway.exe`，直接去这里找，或在 OneDrive\Desktop 找快捷方式。

### WSL 里连不上 Windows 上的 TWS/Gateway

- **症状**：程序跑在 WSL，TWS/Gateway 跑在 Windows，`ib.connect("127.0.0.1", ...)` 连不上。
- **根因**：WSL2 默认 NAT 模式，WSL 内的 127.0.0.1 不等于 Windows 的 127.0.0.1。
- **解决**：在 Windows 侧 `C:\Users\<用户名>\.wslconfig` 写入 `[wsl2]` + `networkingMode=mirrored`，然后 `wsl --shutdown` 重启 WSL。重启后 WSL 与 Windows 共享 127.0.0.1，程序可直连。

### 收盘后启动程序立即退出

- **症状**：程序启动打印「交易时段(9:30-16:00)已经结束,执行退出流程」后退出。
- **说明**：这是**正常行为**，不是 bug。程序只在美东 9:30–16:00 交易时段运行，收盘后启动会立即退出。请在交易时段内运行，或等下一个交易日开盘。

## License

（待定 —— 发布前请选择并添加 LICENSE 文件）
