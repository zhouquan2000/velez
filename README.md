# Velez — Oliver Velez 8种策略的量化交易系统

基于 [Oliver Velez](https://en.wikipedia.org/wiki/Oliver_Velez) 交易法则（8 条交易法则中的 7 条）构建的美股日内量化交易程序，通过 IBKR（Interactive Brokers）的 TWS / IB Gateway API 自动执行交易。

> ⚠️ **免责声明**：本项目仅供**学习与研究**使用，不构成任何投资建议。代码内包含**真实下单逻辑**（市价单 / 止损单 / 追踪止损）。使用 Live 账户运行将**真实动用资金并产生盈亏**，风险自担。请务必先在 **Paper（模拟）账户**上充分测试，确认理解全部行为后再考虑实盘。

---

## 项目现状（务必先读）

**这套 8 法则系统在 15 个月回测中没有统计意义上的正期望，作者已停止在该策略上继续投入。**
本仓库开源的是**工程实现与研究方法**，不是一套能赚钱的策略。

回测（2025-01-02 ~ 2025-12-31，17 个变体文件，按品种+时间+盈亏去重后 7,485 笔平仓）：

| 指标 | 数值 |
| --- | --- |
| 合计 / 单笔均值 | -13,738.29 / -1.84 |
| t 值 | -2.36 |
| 佣金 | 7,784.40（占亏损额 56.7%） |
| 扣费前毛盈亏 | -5,954（-0.80/笔） |
| 同期 8 只等权买入持有 | **+24.7%** |

分法则：`V180L` / `Law8L` / `Law3L` 显著为负（t < -2.5）；`Law4L` / `Law1L` / `Law1S` / `Law6L`
看不出效应（|t| < 1）；仅有 `Law2L` 呈正期望迹象（+16.50/笔，n=123，t=3.36），
但未经样本外验证，且很可能是 2025 年上涨行情的 beta。

如果你的目标是找一套能直接跑的策略，这个仓库不适合；如果你要看一套完整的
IBKR 自动交易工程实现（订单 / 止损 / 风控 / 对账 / 监控 / 报表），这里的代码可以作为参考。

---

## 目录

- [项目现状（务必先读）](#项目现状务必先读)
- [功能特性](#功能特性)
- [目录与文件说明](#目录与文件说明)
- [策略说明](#策略说明)
- [支持的股票](#支持的股票)
- [环境要求](#环境要求)
- [安装](#安装)
- [IBKR 接入方式（三选一）](#ibkr-接入方式三选一)
  - [三种方式优缺点对比](#三种方式优缺点对比)
  - [当前部署（2026-10-19 起）](#当前部署2026-10-19-起)
  - [端口自动适配（--port auto）](#端口自动适配--port-auto)
  - [方式一：IB Gateway（适合无人值守）](#方式一ib-gateway适合无人值守--服务器)
  - [方式二：TWS for Windows（当前部署）](#方式二tws-for-windows当前部署)
  - [TWS 定时登出（Auto Logoff）—— 2026-10-20 事故与处理](#tws-定时登出auto-logoff--2026-10-20-事故与处理)
  - [方式三：TWS for Linux（WSLg）—— 已弃用](#方式三tws-for-linuxwslg--已弃用仅存档)
- [账户模式（Paper / Live）](#账户模式paper--live)
- [市场数据订阅](#市场数据订阅)
- [使用方法](#使用方法)
- [命令行参数](#命令行参数)
- [目录结构](#目录结构)
- [日志](#日志)
- [隐私与安全](#隐私与安全)
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

## 目录与文件说明

| 路径 | 作用 |
| --- | --- |
| `main.py` | **程序入口**：`python main.py --symbol AAPL --clientid 31` |
| `velez_bot/` | **主交易程序（bot）全部在这里**，没有其他目录存放策略代码 |
| `velez_bot/app.py` | 启动流程：连接 TWS、加载历史、建立实时链路、盘前等待、收盘退出 |
| `velez_bot/shared.py` | 全局层：命令行参数、`STOCK_CONFIGS`（每只股票策略参数）、日志、断连重连 |
| `velez_bot/trading_context.py` | 核心交易逻辑：信号识别、下单、止损/追踪止损、风控、状态机 |
| `velez_bot/events.py` | 成交与 K 线事件回调 |
| `velez_bot/context_snapshot.py` | 持仓 / 挂单快照 |
| `log_file/` | **交易日志目录（运行时自动创建，已在 `.gitignore` 中忽略）** |
| `run_one.sh` / `run_all.sh` / `run_all_wt.sh` | 启动脚本：单只 / tmux 八只 / Windows Terminal 八只 |
| `requirements.txt` | 运行依赖 |

### log_file 目录说明

运行时自动生成，文件名带日期与品种：

| 文件 | 内容 |
| --- | --- |
| `<日期>_<品种>_Sys_log.txt` | 单品种详细日志（信号、下单、止损调整、对账） |
| `<日期>_<品种>_Trade_log.csv` | 逐笔成交：`time,symbol,action,qty,price,net_pnl,commission,cum_pnl,loss_streak,exec_id,label` |
| `Global_System_<YYYYMMDD>.log` | 全局日志（启动、断连、重连、异常） |
| `risk_state_<日期>_<品种>.json` | 当日风控状态（连损、累计盈亏、是否熔断） |

日志目录默认是仓库根目录下的 `log_file/`，可用环境变量 `VELEZ_LOG_DIR` 覆盖。

## 策略说明

策略核心是 **Oliver Velez 的"大象柱"（Elephant Bar）**：一根相对 ATR(14) 异常放大的实体 K 线，代表机构资金的爆发性介入，作为入场触发器。

每只股票有一组个性化"指纹"参数（`STOCK_CONFIGS`，见 `velez_bot/shared.py`）：

| 参数              | 含义                                   |
| ----------------- | -------------------------------------- |
| `eb_range_mult` | 实体长度相对 ATR 的倍数（爆发力门槛）  |
| `eb_vol_mult`   | 成交量相对 20 根均量的倍数（资金确认） |
| `eb_body_ratio` | 实体占整根 K 线的比例（纯度）          |
| `risk_unit`     | 每笔交易的风险单元（美元）             |
| `max_qty`       | 单只股票最大持仓股数                   |

信号由 **2 分钟 K 线**（历史数据）计算，交易循环由 **5 秒实时 bar**（`reqRealTimeBars`）驱动。

## 支持的股票

默认内置 8 只美股（均为 NASDAQ 上市），按波动性分组：

| 波动组 | 股票             | 特点                   |
| ------ | ---------------- | ---------------------- |
| 低波动 | AAPL、MSFT       | 宽松门槛，捕捉稀缺信号 |
| 中波动 | GOOG、AMZN、AMD  | 标准门槛               |
| 高波动 | META、NVDA、TSLA | 严格门槛，过滤噪音     |

## 环境要求

- **Python 3.12**（⚠️ 不可用 3.13 / 3.14：依赖 `eventkit==1.0.3` 在 3.14 下 import 即崩溃，锁定版 `pandas/numpy` 在 3.14 无预编译 wheel）
- **IBKR 账户** + TWS 或 IB Gateway（任一即可）
- 操作系统：Windows / Linux / WSL 均可（非 Windows 系统已自动禁用 `winsound` 报警）

## 安装

```bash
# 1. 克隆仓库
git clone https://github.com/zhouquan2000/velez.git
cd velez

# 2. 创建 Python 3.12 虚拟环境（3.13/3.14 未验证，部分依赖的 wheel 可能缺失）
python3.12 -m venv .venv

# 3. 激活环境
# Linux / WSL:
source .venv/bin/activate
# Windows:
.venv\Scripts\Activate.ps1

# 4. 安装依赖
pip install -r requirements.txt

# 5. 自检（不会下单，只验证依赖能导入、参数能解析）
python -c "import ib_insync, pandas, numpy; print('deps OK')"
python main.py --help
```

下一步：按 [IBKR 接入方式](#ibkr-接入方式三选一) 把 TWS / IB Gateway 的 API 端口开出来，
再执行 `python main.py --symbol AAPL --clientid 31`（单只验证），日志会写入 `log_file/`。

> 说明：`requirements.txt` 中的 `ibapi` 与 `futu` 为冗余项，`ib_insync 0.9.x` 自带 IB API 协议实现，不装也不影响运行。

## IBKR 接入方式（三选一）

程序通过 TCP 连接一个在本机运行的 IBKR 应用，**端口同时决定应用类型与账户类型**：

| 应用 | Live 端口 | Paper 端口 |
| --- | --- | --- |
| IB Gateway | 4001 | 4002 |
| TWS（GUI，Windows / Linux） | 7496 | 7497 |

### 三种方式优缺点对比

| 维度 | **TWS for Windows（当前采用）** | TWS for Linux（WSLg） | IB Gateway |
| --- | --- | --- | --- |
| 图形界面 | 完整（Mosaic / Classic / 图表） | 完整，但受 WSLg 限制 | 无，只有登录窗与配置页 |
| 多窗口 / 多图表 | 原生支持，可跨多显示器自由排布 | **几乎不可用**：WSLg 没有窗口管理器，窗口无法正常层叠/置顶/聚焦 | 无 |
| 人工看盘与干预 | 随时看盘、可手动平仓 | 同左，但窗口可能被别的窗口压住 | 不能，只能看日志 |
| 稳定性 | 好；但 GUI 崩溃会连带 API 一起断 | 中：改布局/重建主窗口会打断 TWS↔IBKR 连接（1100），并可能让 HMDS 历史数据农场掉线 | **最好**：无 GUI，专为 API 设计 |
| 资源占用 | 大（Java + 内嵌 Chromium，约 1–1.5 GB） | 同左 | 小（约 300–500 MB） |
| 无人值守 | 可以，但 Windows 不能睡眠 | 可以，但 WSLg 窗口问题拖累 | **最佳**（可配 systemd 守护） |
| 与 WSL 策略的连接 | WSL mirrored 网络，`127.0.0.1:7497` 直连（已实测） | 同机 localhost 直连 | 同左 |
| 安装 / 升级 | Windows 安装包，升级需人工点 | install4j 安装器，升级需人工 | install4j，升级需人工 |
| 主要短板 | 依赖 Windows 常开；GUI 与 API 同生共死 | **WSLg 无窗口管理器**：多窗口、模态弹窗、布局校验都出问题 | 无图形界面，出错只能靠日志；配置入口只有 GUI |

### 当前部署（2026-10-19 起）

- **TWS 跑在 Windows**，登录 Paper 账户（示例：DU1234567）。API 设置：勾选 `Enable ActiveX and Socket Clients`、取消 `Read-Only API`、`Socket port = 7497`、勾选 `Download open orders on connection`。
- **策略跑在 WSL**，通过 mirrored 网络直连 `127.0.0.1:7497`（Windows 与 WSL 共享 loopback，已实测可达）。
- Linux 版 TWS 已卸载；布局与配置备份在 `~/backup_tws_linux_20261019_essential.tar.gz`。
- 两条铁律：
  1. 同一账号同时只能有一个 TWS / Gateway 在线，否则报 `162` 会话冲突（模拟账号会拿不到行情/历史数据）。
  2. Windows 不能睡眠，否则 API 与策略一起断。
- **第三条（2026-10-20 新增）：TWS 的 Auto Logoff 必须设在盘后，且跨夜启动必须晚于该时刻。**
  当前设定 `20:45 PT`（= `23:45 ET`，已收盘）。见下方「TWS 定时登出（Auto Logoff）」。

### 端口自动适配（`--port auto`）

程序默认 `--port auto`，按下列顺序探测第一个可用端口：

`7497 TWS Paper` → `4002 Gateway Paper` → `7496 TWS Live` → `4001 Gateway Live`

启动日志会打印实际选中的端点，断线重连复用同一个端点：

```text
🔌 API 端点: 127.0.0.1:7497 (TWS 纸面 Paper)
```

需要固定端口时显式传入（如 `--port 4002`）即可覆盖自动探测。

⚠️ WSL 特有的坑：mirrored 网络模式下，WSL 里的 `ss -tln` / `netstat` **看不到** Windows 侧的监听端口，
但 TCP 连接是通的。判断 TWS 是否就绪要用实际连接（或直接看程序日志），不要用 `ss` 判断。

### 方式一：IB Gateway（适合无人值守 / 服务器）

IB Gateway 是 IBKR 官方的轻量网关，专为 API 设计，适合无人值守。

1. 从 [IB Gateway 下载页](https://www.interactivebrokers.com/en/trading/ibgateway-latest.php) 下载并安装（Windows 版默认装到 `C:\Jts\ibgateway\`）。
2. 启动后登录：
   - **API 类型**：选 `IB API`（不是 FIX CTCI）
   - **Trading Mode**：Paper 选 `Paper Trading`，Live 选 `Live Trading`
3. 登录后进入 `Configure → Settings → API → Settings`：
   - 取消勾选 **Read Only API**（首次手动登录默认只读，不取消则所有下单报 `Error 321`）
   - 确认 **Socket port** = `4002`（Paper）或 `4001`（Live）
   - **Download open orders on connection** 建议勾选（程序启动需同步在途单）
4. 优点：资源占用小、没有弹窗、最适合长期无人值守；缺点：没有图表，出错只能看日志。

### 方式二：TWS for Windows（当前部署）

1. 从 IBKR 官网下载 Windows 版 TWS 并安装。
2. 登录 Paper 账户。
3. `Global Configuration → API → Settings`：
   - 勾选 **Enable ActiveX and Socket Clients**
   - 取消 **Read-Only API**（否则所有下单报 `Error 321`）
   - **Socket port** = `7497`（Paper）/ `7496`（Live）
   - 勾选 **Download open orders on connection**（程序启动对账依赖）
   - 建议**取消** **Auto-fallback to delayed market data**：行情不可用时让它报错，而不是静默降级成 15 分钟延迟数据
   - 改完必须点 **OK / Apply**（不点等于没生效）
4. Windows 电源计划设为「从不睡眠」，否则策略会在半夜断连。

#### TWS 定时登出（Auto Logoff）—— 2026-10-20 事故与处理

**现象**：2026-10-20 凌晨 `02:45:00 ET`，8 个策略在同一秒检测到断连；TWS 在此之前 `0.7` 秒
完成了 `tws.xml` / `tws.Mon.xml` / `jts.ini` 与日志的落盘，然后进程退出。
不是崩溃、不是断电、不是网络抖动，是 TWS **主动干净退出**。策略按旧策略重试 10 次（5 分 42 秒）
后集体 `os._exit(1)`，从 02:50 到 09:30 开盘无人在场，当天前 14 分钟行情因此错过。

**根因**：`Global Configuration → Lock and Exit → Auto Log Off` 的触发时刻是 **23:45（TWS 本地时区 = Pacific）**，
即 `02:45 ET`；正好落在跨夜运行窗口中间。`jts.ini` 里 `TimeZone=America/Vancouver` 可佐证 TWS 用的是本机时区。

**处理（2026-10-20 已执行）**：把 Auto Logoff 改到 `20:45 PT`（= `23:45 ET`，盘后 7 小时 45 分）。

为什么这样就够：
- 跨夜启动时间通常在 `21:45–21:50 PT`（`00:45–00:50 ET`），此时**当天的 20:45 PT 已过去**，
  下一次触发是次日 `20:45 PT`（`23:45 ET`，远在 16:00 收盘之后）。
- 所以交易时段（`09:30–16:00 ET` / `06:30–13:00 PT`）内不会再被打断。

仍然要注意的边界：
- 若某天在 **20:45 PT 之前**启动 TWS 并让它一直开着，它仍会在 20:45 PT 登出。
- Auto Logoff 与 Auto Restart 是两个独立开关，两边都要核对；登出后**需要人工重新登录**，
  没有人按键盘时它不会自己回来 —— 这正是当晚整夜空转的原因。
- 策略侧配合改动：断连重试从「10 次指数退避（约 5 分 42 秒）后退出」改为
  **每 5 分钟重试一次、持续到当日 16:00 ET 收盘**（`shared.py: reconnect_loop`），
  这样即便 TWS 中途被重启/登出，只要在收盘前回来，8 个策略会自动接上。

### 方式三：TWS for Linux（WSLg）—— 已弃用，仅存档

Linux 版 TWS 在 WSLg 里能跑、界面也完整，但 WSLg **没有窗口管理器**，实测会踩：

- 多窗口 / 多图表无法正常层叠、置顶、聚焦；模态弹窗会被主窗口压住，表现为「界面在、鼠标点不动」
- 修改布局会重建主窗口，进而打断 TWS↔IBKR 连接（错误 `1100`），并可能让 HMDS 历史数据农场掉线，
  之后所有历史数据请求返回空（`162 HMDS query returned no data`），策略重启即集体进入「今日暂停交易」
- 判断是否真卡死要先看线程转储（`kill -3 <pid>`）：AWT 事件队列空闲即未卡死，真正原因是模态弹窗

因此本项目改用 TWS for Windows 提供图形界面，WSL 只跑策略。

## 账户模式（Paper / Live）

程序不区分账户类型——**唯一区别是端口**。切换账户只需改 `--port`（及 Gateway 登录时选的 Trading Mode）。

### Paper（模拟）账户

```bash
# 自动探测（默认）：TWS Paper 7497 → Gateway Paper 4002 → TWS Live 7496 → Gateway Live 4001
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

| 数据类型                               | 接口                  | 是否需要付费订阅                |
| -------------------------------------- | --------------------- | ------------------------------- |
| 历史数据（2 分钟 K 线、VIX 15 分钟线） | `reqHistoricalData` | 否，默认可用                    |
| 实时行情（5 秒 bar，驱动交易循环）     | `reqRealTimeBars`   | **是，需要 Level 1 订阅** |

实时行情（Level 1）订阅：登录 [Client Portal](https://ndcdyn.interactivebrokers.com) → `Settings → Market Data Subscriptions`，订阅美股三大实时行情（Non-Pro 价格）：

| 订阅项                                            | 覆盖                                 | 价格     |
| ------------------------------------------------- | ------------------------------------ | -------- |
| NASDAQ (Network C/UTP) (NPL1)                     | NASDAQ 上市股票（本项目 8 只全在此） | $1.50/月 |
| NYSE (Network A/CTA) (NPL1)                       | NYSE 上市股票                        | $1.50/月 |
| NYSE American, BATS, ARCA, IEX (Network B) (NPL1) | 区域交易所（SMART 路由可能走这些）   | $1.50/月 |

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

| 脚本                | 方式             | 说明                                           |
| ------------------- | ---------------- | ---------------------------------------------- |
| `./run_all.sh`    | tmux             | 8 只股票各占一个 tmux 窗口（Linux / WSL 推荐） |
| `./run_all_wt.sh` | Windows Terminal | 8 只股票各占一个 tab（Windows 推荐）           |

内置 8 只股票的 clientId 映射（31–38）：

```
AAPL:31  AMD:32  AMZN:33  GOOG:34  META:35  MSFT:36  NVDA:37  TSLA:38
```

> clientId 必须唯一：同一 TWS/Gateway 连接下的每个程序实例要使用不同的 clientId，否则会互相挤下线。

### 停止

`Ctrl + C` 会触发**紧急清仓**（撤销挂单并平掉持仓）后安全退出。

### 运行前检查清单

启动前请逐项确认：

| 检查项                             | 如何确认                                                        |
| ---------------------------------- | --------------------------------------------------------------- |
| tmux 已安装（`run_all.sh` 依赖） | `tmux -V`；未装则 `sudo apt install tmux`                   |
| TWS / IB Gateway 已登录            | 网关窗口 Connection Status 显示 `connected`，且能读到账户数据 |
| 美股交易时段内                     | 美东 9:30–16:00（收盘后启动会立即退出，属正常行为）            |
| live 账号未在别处登录              | 手机 app / Client Portal 网页都登出，避免 Error 162             |

### tmux 操作（run_all.sh）

`run_all.sh` 启动后会自动 `tmux attach` 进入总控台，底部显示 8 个窗口（AAPL / AMD / ... / TSLA）：

| 操作                           | 作用                             |
| ------------------------------ | -------------------------------- |
| `Ctrl+b` 然后 `d`          | 脱离（detach），程序继续后台运行 |
| `Ctrl+b` + `0`–`7`      | 跳到第 N 个窗口（对应 8 只股票） |
| `Ctrl+b` + `n` / `p`     | 下一个 / 上一个窗口              |
| `tmux attach -t velez`       | 重新进入会话                     |
| `tmux kill-session -t velez` | 全部关停                         |

### 推荐启动流程

1. 先 `./run_one.sh AAPL` 跑单只，观察约 10 分钟日志，确认信号 / 下单 / 止损全链路正常。
2. 确认无误后再 `./run_all.sh` 上 8 只全量。

单只先验证比 8 只一起上更容易定位问题。

## 命令行参数

| 参数           | 默认值        | 说明                                                                             |
| -------------- | ------------- | -------------------------------------------------------------------------------- |
| `--symbol`   | `AAPL`      | 股票代码（须在 `STOCK_CONFIGS` 中定义）                                        |
| `--clientid` | `31`        | IBKR 客户端 ID（多实例时须唯一）                                                 |
| `--host`     | `127.0.0.1` | TWS / IB Gateway 主机地址                                                        |
| `--port`     | `auto`      | 自动探测（7497 TWS Paper → 4002 Gateway Paper → 7496 TWS Live → 4001 Gateway Live）；也可显式传端口 |

## 目录结构

```
velez/
├── main.py                 # 程序入口：python main.py --symbol AAPL --clientid 31
├── velez_bot/              # 【主交易程序 / bot】全部策略代码
│   ├── app.py              # 启动流程、交易时段、实时链路
│   ├── shared.py           # 连接、命令行参数、STOCK_CONFIGS、日志、重连
│   ├── trading_context.py  # 核心交易逻辑（信号、下单、治理、追踪止损）
│   ├── context_snapshot.py # 持仓/挂单快照
│   ├── events.py           # 成交/bar 事件回调
│   └── __init__.py
├── run_one.sh              # 单只股票启动
├── run_all.sh              # tmux 启动 8 只
├── run_all_wt.sh           # Windows Terminal 启动 8 只
├── requirements.txt        # 运行依赖
├── .gitignore              # 已排除 .env、log_file/、.venv/ 等
└── log_file/               # 【运行日志】自动生成，不提交
```

## 日志

程序运行日志全部落盘在 `log_file/` 目录，日志时间戳一律采用**美东时间（America/New_York）**。

**每只股票每个交易日生成 6 个文件**（3 个日志 + 3 个行情 CSV）：

| 文件 | 命名格式 | 内容 | 主要用途 |
| ---- | -------- | ---- | -------- |
| 系统日志 | `{日期}_{股票}_Sys_log.txt` | 该股全天运行事件（初始化、行情合成、信号识别、下单、止损止盈、对账、熔断） | 调试、复盘、排查问题 |
| 成交日志 | `{日期}_{股票}_Trade_log.csv` | 每笔成交明细（开仓/平仓、价格、净盈亏、佣金、累计盈亏） | 盈亏统计、每日复盘 |
| 状态审计日志 | `{日期}_{股票}_cond_log.txt` | 状态机每一拍的对账诊断（状态代码、持仓、状态） | 审计治理流转、排查持仓不同步 |
| 5 秒 bar | `{股票}-5s-{日期}.csv` | 5 秒实时 bar 的 OHLCV（`reqRealTimeBars` 原始数据） | 最细粒度行情存档 |
| 2 分钟 K 线 | `{股票}-2m-{日期}.csv` | 2 分钟 K 线 OHLCV（策略主信号周期） | 信号复盘 |
| 15 分钟 K 线 | `{股票}-15m-{日期}.csv` | 15 分钟 K 线 OHLCV（趋势判断周期） | 趋势复盘 |

> 命名中 `{日期}` 为 `YYYY-MM-DD`（如 `2026-10-16`），`{股票}` 为大写代码（如 `AAPL`）。示例：`2026-10-16_AAPL_Sys_log.txt`、`AAPL-2m-2026-10-16.csv`。

### 各文件格式

**系统日志 `Sys_log.txt`**：每行一条事件，格式 `[HH:MM:SS.mmm] [股票] 图标 消息`：

```
[10:18:05] [AAPL] 💡 ✅ [V180L] 开仓单和止损单已提交
[10:21:43] [AAPL] ⚠️ 📉 [风险控制]本次交易亏损-68.71...连续亏损: 1次
```

**成交日志 `Trade_log.csv`**：表头 `time,symbol,action,qty,price,net_pnl,commission,cum_pnl,loss_streak,exec_id,label`。每行一笔成交：开仓单 `net_pnl=0`，平仓单 `net_pnl` 为该笔净盈亏（已扣佣金），`cum_pnl` 为当日累计盈亏。日终盈亏分析直接对 `net_pnl` 求和即可。

**状态审计日志 `cond_log.txt`**：每行格式 `[HH:MM:SS.mmm] | Cond: 代码 | Msg: 说明 | Pos: 持仓 | State: 状态`，记录状态机每一拍的结果（如 `cond_01` 标准待机、`cond_08_07` 开仓完美、`cond_12_05` 标准稳态、`cond_09` 结账归零、`cond_11_01` 裸奔补防）。

**行情 CSV（5s / 2m / 15m）**：三者表头一致 `datetime,open,high,low,close,volume`。区别：5 秒 CSV 的 `datetime` 只有时分秒（如 `12:30:20`，实时 bar 无日期）；2 分钟 / 15 分钟 CSV 的 `datetime` 为完整日期时间（如 `2026-10-16 09:30:00`），且启动时会先写入回溯数日的历史 K 线、再追加当日实时数据。

### 全局共享文件（非按股票）

| 文件 | 内容 |
| ---- | ---- |
| `Global_System_YYYYMMDD.log` | 全局系统日志（账户资金报告、VIX 调度、连接状态等跨品种信息） |
| `VIX-15m-YYYY-MM-DD.csv` | VIX 恐慌指数 15 分钟 K 线（风控环境自适应用） |
| `ib_insync_debug.log` | ib_insync 库底层日志（Error 162、request timeout 等） |
| `{日期}_交易盈亏分析报告.md` | 日终盈亏分析报告（复盘工具生成，非程序自动产出） |

> 注：`log_file/` 目录已被 `.gitignore` 忽略，日志不会进入版本库。若某天某股无任何成交，则当天不会生成对应的 `Trade_log.csv`。

## 隐私与安全

- 仓库内**不含**任何账号、密码、API Key、Token。IBKR 的身份认证由 TWS / IB Gateway 的登录界面完成，
  本程序只通过本地 socket（`127.0.0.1:7497/7496/4001/4002`）与它通信，不接触凭证。
- `.gitignore` 已排除 `.env`、`log_file/`、`.venv/`、`__pycache__/` 等，运行时产生的账户数据不会被提交。
- 文档与脚本中的账户号一律是占位符（如 `DU1234567` / `<AccountID>`）。
  如需公开自己的日志或截图，请先脱敏（净值、成交、账户号）。
- 默认只绑定 `127.0.0.1`；TWS 的 API 设置里也应保持 `Allow connections from localhost only`。

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

本项目采用 [MIT License](LICENSE) 开源。

    Copyright (c) 2026 周全 (zhouquan)
