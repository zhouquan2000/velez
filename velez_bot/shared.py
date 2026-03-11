# ======================================================================
# shared.py - 精确导入清单 (修正版)
# ======================================================================

# --- 1. 标准库 ---
import os
import sys  # 🔥 新增：sys.exit(1)
import random  # 🔥 新增：random.uniform(5, 12)
import argparse  # 🔥 新增：命令行参数解析
import asyncio  # 🔥 新增：异步函数
import time as time_module
from datetime import datetime, timezone, time, timedelta
from typing import Optional  # 🔥 新增：类型提示
import pytz

# from pathlib import Path  # ❌ 删除：未使用

# --- 2. 第三方库 ---
import pandas as pd
from ib_insync import (
    util,
    IB,
    Index,
)

# from ibapi.contract import IB, Index  # 🔥 新增：IBKR API 类

# --- 3. 可选：Windows 声音报警 ---
try:
    import winsound  # 🔥 新增：声音报警
except ImportError:
    winsound = None  # 非 Windows 系统设为 None

__all__ = [
    # 核心函数
    "sys_log",
    "ensure_log_dir",
    "load_vix_data_async",
    "get_account_available_funds",
    "show_account_detail",
    "standardize_df",
    "on_tws_error",
    "daily_closing_safety_guard",
    "contrl_c_exit",
    # 常量
    "EASTERN_TZ",
    # 全局变量
    "ENTRY_TYPE_ABBREV",
    "ENTRY_TYPE_FULL",
    "global_last_vix_close",
    "vix_change_rate",
    "contexts_placeholder",
    "loop",
    "ib",
    "STOCK_CONFIGS",
    "SYMBOLS",
    "COND_MSG_MAP",
    # 工具
    "time_module",
    "pd",
    "util",
]
# OrderRef entry_type 缩写映射表（双向映射）
ENTRY_TYPE_ABBREV = {
    "Breakout": "bkt",
    "Pullback": "pbk",
    "GiftZone": "gfz",
    "Reversal": "rvs",
    "LimitOrder": "lmt",
    "StopOrder": "sto",  # 止损单专用
    "MarketOrder": "mkt",
    "Loss": "los",
    "Profit": "prf",  # 止盈单专用
    "Force": "frc",  # 强平单专用
    "Unknown": "ukn",
}

# 反向映射（解析时用）
ENTRY_TYPE_FULL = {v: k for k, v in ENTRY_TYPE_ABBREV.items()}
ACTION_MAP = {"BOT": "BUY", "SLD": "SELL"}

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(PROJECT_ROOT, "log_file")


def ensure_log_dir() -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    return LOG_DIR


# ======================================================================
# --- 🌍 全局环境初始化 ---
# ======================================================================
# 注意：在阶段二重构中，ib 对象将作为实例参数传递给类
ib = IB()
# EASTERN_TZ = timezone(timedelta(hours=-5))
EASTERN_TZ = pytz.timezone("America/New_York")
loop = None
# ======================================================================
# --- 📈 交易品种与个性化大象柱配置 (2026 经验参数版) ---
# ======================================================================
# ----- 1.0 解析命令行参数 -----
parser = argparse.ArgumentParser(description="Velez量化交易系统 - 参数化启动")

parser.add_argument("--symbol", type=str, default="AAPL", help="股票代码 (默认: AAPL)")
parser.add_argument("--clientid", type=int, default=31, help="客户端ID (默认: 31)")
# parser.add_argument('--client_id', type=int, required=True, help='TWS 客户端 ID，如 31')
args = parser.parse_args()

# 将参数赋值给变量
symbol = args.symbol.upper()  # 统一转为大写

# 1.1 定义运行品种清单
# SYMBOLS = ['AAPL', 'AMD', 'AMZN', 'GOOG', 'META', 'MSFT', 'NVDA', 'TSLA']
# SYMBOLS = ['AAPL']
STOCK_CONFIGS: dict = {
    # 低波动组（ATR < 0.6）：宽松门槛，捕捉稀缺信号
    "AAPL": {
        "risk_unit": 120,
        "max_qty": 200,
        "eb_range_mult": 2.0,
        "eb_vol_mult": 1.2,
        "eb_body_ratio": 0.85,
    },  # 蓝筹之王，低波动需降低门槛
    "MSFT": {
        "risk_unit": 120,
        "max_qty": 160,
        "eb_range_mult": 2.0,
        "eb_vol_mult": 1.2,
        "eb_body_ratio": 0.85,
    },  # 同AAPL
    # 中波动组（0.6 ≤ ATR < 1.2）：标准门槛
    "GOOG": {
        "risk_unit": 120,
        "max_qty": 160,
        "eb_range_mult": 2.2,
        "eb_vol_mult": 1.3,
        "eb_body_ratio": 0.80,
    },
    "AMZN": {
        "risk_unit": 120,
        "max_qty": 160,
        "eb_range_mult": 2.2,
        "eb_vol_mult": 1.3,
        "eb_body_ratio": 0.80,
    },
    "AMD": {
        "risk_unit": 120,
        "max_qty": 100,
        "eb_range_mult": 2.5,
        "eb_vol_mult": 1.4,
        "eb_body_ratio": 0.75,
    },  # 高影线需降低实体占比
    # 高波动组（ATR ≥ 1.2）：严格门槛，过滤噪音
    "META": {
        "risk_unit": 120,
        "max_qty": 80,
        "eb_range_mult": 2.6,
        "eb_vol_mult": 1.5,
        "eb_body_ratio": 0.80,
    },  # 极高波动需提高门槛
    "NVDA": {
        "risk_unit": 120,
        "max_qty": 100,
        "eb_range_mult": 2.6,
        "eb_vol_mult": 1.5,
        "eb_body_ratio": 0.80,
    },  # 当前市场核心，需最严门槛
    "TSLA": {
        "risk_unit": 120,
        "max_qty": 80,
        "eb_range_mult": 2.5,
        "eb_vol_mult": 1.4,
        "eb_body_ratio": 0.75,
    },  # 高波动但需保持信号频率
}
if symbol not in STOCK_CONFIGS:
    print(f"❌ 错误：股票 {symbol} 的配置不存在，请在 STOCK_CONFIGS 中定义。")
    sys.exit(1)

SYMBOLS = [args.symbol]
cl_id = int(args.clientid)
if cl_id is None:
    print("None")
# 2. 定义每只股票的大象柱个性化“指纹”
# eb_range_mult: 实体长度相对于 ATR(14) 的倍数 (爆发力)
# eb_vol_mult:   成交量相对于过去 20 根均量的倍数 (资金确认)
# eb_body_ratio: 实体占整根 K 线长度的最小比例 (纯度)
try:
    ib.connect("127.0.0.1", 7497, clientId=cl_id)
    print(f"✅ 连接成功 | 股票代码: {args.symbol} | ClientID: {cl_id}")
except Exception as e:
    print(f"❌ 连接TWS失败: {e}")
    os._exit(0)

_last_vix_sync_unix = 0
global_last_vix_close = 20.0
vix_change_rate = 0.0
contexts_placeholder = {}
COND_MSG_MAP = {
    # --- 01-04: OPEN_STAGE (待机与自愈区) ---
    "cond_01": "标准待机：系统空闲，无意图、无头寸、无挂单。",
    "cond_02": "清理幽灵：无交易意图，但柜台残留不明挂单，执行撤单。",
    "cond_03": "僵尸持仓：系统认为没仓，但物理快照发现头寸，强制平仓。",
    "cond_04": "系统失控：无意图但有仓有单，执行全场物理肃清。",
    # --- 05-08: ORDER_SENT (推进与转正区) ---
    "cond_05": "意图丢失：已发单但柜台既没单也没仓，逻辑复位归位。",
    "cond_06_01": "发现一个入场单，等候<=60秒。",
    "cond_06_02": "发现一个入场单，等候>60 and <=90秒。",
    "cond_06_03": "发现一个入场单，等候>90 and <= 120秒。",
    "cond_06_04": "发现一个入场单，等候>120秒:一律撤单。",
    "cond_06_partial": "发现入场订单,有部分成交",
    "cond_06_05": "入场异常: 发现后台同时有2笔以上的入场订单，逻辑冲突报警。",
    "cond_06_06": "入场未知: 探测到未定义的入场挂单状态组合。",  # ✨ 补全：入场兜底
    "cond_07": "确权转正：入场单刚刚完全成交，身份由士兵转为守卫。",
    "cond_08_naked": "裸奔：正在开仓，但保护性止损离奇消失，紧急补防。",
    "cond_08_01": "开仓/加仓单等候<=60 s。",
    "cond_08_02": "开仓/加仓单等候 >60 s but <= 90s：。",
    "cond_08_03": "开仓/加仓单等候 >90 s but <= 120s。",
    "cond_08_04": "开仓/加仓单等候 >120, 一律撤单。",
    "cond_08_partial": "加仓单有部分成交",
    "cond_08_05": "开仓/加仓纠偏：开仓/加仓成交后止损数量不足，增加止损单手数。",
    "cond_08_06": "开仓/加仓完美：开仓/加仓成交且止损数量过量，减少止损单手数。",
    "cond_08_07": "开仓/加仓完美：开仓/加仓成交且止损数量对齐，稳态过渡。",
    "cond_08_08": "开仓/加仓未知：开仓/加仓过程中探测到意料之外的单据组合。",  # ✨ 补全：开仓兜底
    # --- 09-12: HOLDING_STAGE (守护与对账区) ---
    "cond_09": "结账归零：持仓已物理结清，内存状态执行最后复位。",
    "cond_10": "清场残留：持仓态，但是仓位已空,可是还有残留的挂单单，执行物理清场。",  # 根目录
    "cond_11_01": "绝对裸奔：持仓中且无任何保护单，触发最高等级补防。",
    "cond_11_02": "持仓正在被止损或者止盈，看似裸奔，但不一定，等下一个5秒",
    "cond_12_01": "防线缺失：持仓中且止损单缺失，立即补单。",
    "cond_12_02": "防线缺口：止损股数少于持仓，执行调增手术。",
    "cond_12_03": "防线过载：止损股数多于持仓，执行削减手术。",
    "cond_12_04": "止盈监控：止盈单正在护航中，监控盈利目标。",
    "cond_12_05": "标准稳态：止损单1:1完美覆盖持仓。",
    "cond_12_06": "特等稳态：止损与止盈全方位护航中。",
    "cond_12_07": "进攻维护：持仓期间有新的加仓单正在排队。",
    "cond_12_unhandled": "治理异常：探测到未定义的持仓子状态组合。",  # ✨ 补全：持仓兜底
}


def sys_log(msg, level="System"):
    """
    [系统层-全局审计] 方案 A：全局日期固定化增量落盘
    职责：系统级日志输出 + 图标对齐 + 物理同步
    """
    # 1. 构造高精度时间戳 (美东时间)
    now = datetime.now(EASTERN_TZ)
    ts = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # 2. 保留并增强系统级图标系统
    sys_icons = {
        "System": "🌐",  # 连接、断开、初始化
        "CRITICAL": "🚨",  # 崩溃、熔断
        "VIX": "📊",  # VIX 更新
        "Schedule": "⏰",  # 开收盘任务
        "INFO": "🔹",  # 普通信息
        "ALERT": "📢",  # 警报
        "DEBUG": "🛠️",  # 调试
    }
    icon = sys_icons.get(level, "🔹")

    # 3. 构造标准日志条目 (用于控制台和文件)
    log_entry = f"[{ts}] {icon} [{level}] {msg}"

    # 4. 控制台即时物理打印
    print(log_entry)

    # 5. ✨ 方案 A 加固：物理增量追加 (落地硬盘)
    try:
        # 文件名锁定：Global_System_YYYYMMDD.log
        today_str = now.strftime("%Y%m%d")
        file_name = os.path.join(ensure_log_dir(), f"Global_System_{today_str}.log")

        # 使用追加模式 ('a') 确保重启不覆盖旧日志
        with open(file_name, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")

    except Exception as e:
        # 如果系统日志写入失败，仅在控制台紧急报警 (防止死循环报错)
        print(f"🚨 [System I/O Error] 无法写入全局日志文件: {e}")


# 建议在重构测试时使用独立的 clientId，避免干扰明天的 06-9 实战


async def get_account_available_funds() -> float:
    """获取当前账户可用资金 (USD) - 07-6 编译器兼容版"""
    try:
        values = ib.accountValues()
        # 1) 优先：CashBalance (USD) —— 最像 Portfolio 里的 USD CASH
        for acct in values:
            if acct.tag == "CashBalance" and acct.currency == "USD":
                return float(acct.value)

        # 2) 兜底：TotalCashValue (USD)
        for acct in values:
            if acct.tag == "TotalCashValue" and acct.currency == "USD":
                return float(acct.value)

        # 3) 最后兜底（保证金口径，不等于 Portfolio Cash）
        for acct in values:
            if acct.tag == "AvailableFunds" and acct.currency in ("", "USD"):
                return float(acct.value)

        return 0.0
    except Exception as e:
        sys_log(f"❌ 资金对账失败: {e}", level="ERROR")
        return 0.0


async def show_account_detail():
    # 展示账目目前的各种资金指标
    try:
        values = ib.accountValues()
        financial_map = {
            "NetLiquidation": "总资产(NetLiq)            ",
            "AvailableFunds": "保证金可用资金(AvailabeFunds)      ",
            "ExcessLiquidity": "风控余量(ExcessLiqudity)  ",
            "BuyingPower": "总购买力(BuyingPower)     ",
            "UnrealizedPnL": "未实现盈亏(UnrealizedPnL) ",
            "RealizedPnL": "已实现盈亏(RealizedPnL)   ",
            "EquityWithLoanValue": "权益(Equity)              ",
            "FullInitMarginReq": "初始保证金                ",
            "TotalCashValue": "总现金折算值(TotalCashValue)",
        }
        # audit = {
        #'NetLiquidation': 0.0, #净清算价值:账户的总价值（现金 + 持仓市值）。这是衡量你盈亏最准的指标。
        #'AvailableFunds': 0.0, #可用资金:可用于新交易的资金总额（不包括持仓市值）。这是你能动用的现金。
        #'ExcessLiquidity': 0.0, #剩余流动性:反映距离“强制平仓”还有多远。若此值归零，TWS 将立即砍仓。
        #'BuyingPower': 0.0, #购买力:通常是 AvailableFunds 的 4 倍（日内）或 2 倍（隔夜）。反映你能下多大的单。
        #'UnrealizedPnL': 0.0, #当日未实现盈亏:当前持仓的浮动盈亏。
        #'RealizedPnL': 0.0, #当日已实现盈亏:今天已经平仓的盈亏总和,今天已经落袋安稳的钱。
        #'EquityWithLoanValue': 0.0, #账户权益（含贷款股权价值):账户的总权益（现金 + 持仓市值 + 借贷价值）。衡量你整体财务健康状况的指标。TWS 用它来计算你的原始保证金能力。
        #'MaintMarginReq': 0.0 #维持保证金要求:你需要维持当前持仓所需的最低保证金。如果你的账户权益跌破这个水平，TWS 会强制平仓。
        # }
        audit_results = {}
        for acct in values:
            # 过滤 USD 账户数据 (汇总数据 currency 通常为空或 USD)
            if acct.currency in ["", "USD"] and acct.tag in financial_map:
                audit_results[acct.tag] = acct.value
        # ===============================
        # 2️⃣ 新增：打印所有 Cash 类条目（按币种）
        # ===============================
        cash_details = []
        for acct in values:
            if "Cash" in acct.tag or acct.tag in [
                "CashBalance",
                "TotalCashValue",
                "TotalCashBalance",
                "AccruedCash",
            ]:
                cash_details.append(
                    f"{acct.tag:<20} | {acct.currency:<5} | {acct.value}"
                )

        # 3. 构造审计日志
        timestamp = datetime.now(EASTERN_TZ).strftime("%H:%M:%S")
        log_msg = f"💰 [账户资金报告 @ {timestamp}]\n"
        log_msg += "--------------------------------------------------\n"

        for tag, label in financial_map.items():
            val = audit_results.get(tag, "N/A")
            # 尝试格式化为千分位货币格式
            try:
                formatted_val = f"${float(val):,.3f}"
            except:
                formatted_val = val
            log_msg += f"   {label.ljust(15)}: {formatted_val}\n"

        log_msg += "--------------------------------------------------\n"
        # ===============================
        # 4️⃣ 新增现金明细区块
        # ===============================
        log_msg += "           📦 [Cash 明细 - 所有币种]\n"
        log_msg += "--------------------------------------------------\n"
        if cash_details:
            for line in cash_details:
                log_msg += f"   {line}\n"
        else:
            log_msg += "   (无 Cash 类数据)\n"
        log_msg += "--------------------------------------------------"

        sys_log(log_msg, level="INFO")
        return float(audit_results.get("AvailableFunds", 0.0))
    except Exception as e:
        sys_log(f"❌读取账户资金失败，失败code=: {e}", level="ERROR")
        return 0.0


async def load_vix_data_async(is_init=False):
    """加载芝加哥期货市场CBOE提供的恐慌指数VIX数据 - 07-18y 工业级版"""
    """
    [自律加固版] 加载/更新 VIX 数据
    1. is_init=True: 启动时加载 3 天 15min 历史，建立背景。
    2. 9:30-10:00:05: 自动避障，此时今日15min线未合拢，改读日线获取昨收。
    3. 10:00:05 后: 正常读取今日已合拢的 15min K线。
    """
    global global_last_vix_close, _last_vix_sync_unix, vix_change_rate

    # --- 1. 环境感知与频率审计 ---
    now_et = datetime.now(EASTERN_TZ)
    now_time = now_et.time()
    current_unix = time_module.time()

    # 非初始化状态下，增加 10 分钟频率保护，防止补票导致的 API 洪水
    if not is_init and (current_unix - _last_vix_sync_unix < 600):
        return True

    _last_vix_sync_unix = current_unix  # 立即占位上锁

    try:
        sys_log(
            f"📥 [VIX调度] 正在执行环境自适应提取 (模式: {'初始化' if is_init else '常规更新'})...",
            level="System",
        )
        vix_contract = Index("VIX", "CBOE", "USD")

        # --- 2. 策略分治：确定 duration 和 bar_size ---
        # 核心逻辑：9:30-10:00:05 之间今日首根15min线不存在，强读会Timeout
        if is_init:
            # 模式 A: 初始化 -> 建立 3 天背景
            duration = "3 D"
            bar_size = "15 mins"

        elif time(9, 30) <= now_time < time(10, 0, 5):
            # 模式 B: 开盘避障 -> 今日15min线未就绪，拉取日线获取昨日收盘基准
            sys_log(
                f"🛡️ [VIX避障] 当前时间 {now_time} 处于开盘初期，改读日线级数据以防止超时",
                level="INFO",
            )
            duration = "3 D"
            bar_size = "15 mins"
        else:
            # 模式 C: 正常交易期 -> 10:00:05之后，读取今日已生成的 15min 序列
            duration = "1 D"
            bar_size = "15 mins"

        # --- 3. 异步错峰：避开整点/跨线点 TWS 最忙的瞬时 ---
        if not is_init:
            # 随机延迟 5-12 秒，确保股票 15min 缓存先更新，VIX 随后跟进
            await asyncio.sleep(random.uniform(5, 12))

        # --- 4. 物理请求 ---
        bars = await ib.reqHistoricalDataAsync(
            vix_contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )

        # --- 5. 数据处理与标准化 (保留原有的审计逻辑) ---
        vix_raw_df = util.df(bars)
        if vix_raw_df is None or vix_raw_df.empty:
            sys_log(
                f"❌ VIX 读取返回空 (模式: {bar_size})，使用兜底值 18", level="ERROR"
            )
            if is_init:
                global_last_vix_close = 18.0
            return False

        vix_df = standardize_df(vix_raw_df)
        if vix_df is None or vix_df.empty:
            sys_log("❌ VIX 数据标准化后为空", level="ERROR")
            global_last_vix_close = 18.0
            return False

        # 动态探测时间列
        cols = vix_df.columns.tolist()
        target_col = next((c for c in ["datetime", "date", "time"] if c in cols), None)

        if target_col is None:
            sys_log(f"🚨 VIX 数据缺失时间列: {cols}", level="ERROR")
            return False

        # 时区转换与 RTH 过滤
        vix_df[target_col] = pd.to_datetime(vix_df[target_col], utc=True).dt.tz_convert(
            EASTERN_TZ
        )
        vix_df = (
            vix_df.set_index(target_col).between_time("09:30", "16:15").reset_index()
        )
        vix_df.rename(columns={target_col: "datetime"}, inplace=True)

        if vix_df.empty:
            sys_log("⚠️ VIX RTH 过滤后无数据，使用兜底值 20.0", level="WARN")
            global_last_vix_close = 18.0
            return False

        # --- 6. 提取结果与对账 ---
        vix_len = len(vix_df)
        last_row = vix_df.iloc[-1]
        last_close = float(last_row["close"])

        if vix_len >= 2:
            prev_row = vix_df.iloc[-2]
            prev_close = float(prev_row["close"])
            prev_time_str = prev_row["datetime"].strftime("%H:%M:%S")
        else:
            prev_close = last_close
            prev_time_str = "N/A"

        global_last_vix_close = last_close
        if prev_close == 0.0:
            vix_change_rate = 0.00
        else:
            vix_change_rate = (last_close - prev_close) / prev_close

        sys_log(
            f"📊 [VIX明细-1] 时间:{last_row['datetime'].strftime('%H:%M:%S')}, VIX:{last_close:.3f}, 模式:{bar_size}",
            level="VIX",
        )
        sys_log(
            f"📊 [VIX明细-2] 时间:{prev_time_str}, VIX:{prev_close:.3f}, VIX_Change_Rate={vix_change_rate:.4f}",
            level="VIX",
        )
        sys_log(
            f"✅ [VIX加载成功] 样本数:{vix_len}, 当前基准VIX={global_last_vix_close}",
            level="VIX",
        )
        return True

    except Exception as e:
        sys_log(f"❌ [VIX 预加载/更新失败] 错误信息: {e}", level="ERROR")
        global_last_vix_close = 18.0
        return False


def on_tws_error(reqId, errorCode, errorString, contract):
    """
    [系统级指挥部 - 2026-01-20 终极复用版]
    原则：不造轮子，复连瞬间直接激活各品种已有的“影子对账”逻辑。
    """
    global contexts_placeholder

    # 1. 过滤高频通知
    silent_codes = {2106, 2158, 2107, 2108, 2157}
    if errorCode in silent_codes:
        return
    if errorCode == 366:
        return
    # 2. 统一系统日志格式
    error_msg = f"TWS {errorCode}: {errorString} (reqId:{reqId})"

    # 3. 核心分发逻辑

    # --- A类：物理断连 (2105, 1100) ---
    if errorCode in {1100, 2103, 2105}:
        sys_log(
            f"🚨 [TWS警报] 与IBKR服务器的物理链路中断！程序进入静默等待：{error_msg}",
            level="TWS-Alert",
        )
        winsound.Beep(1000, 1000)

    # --- B类：复连自愈 (2104, 1102) ---
    elif errorCode in {2104, 1102}:
        sys_log(
            f"✅ [TWS恢复] 连接已重建！启动全品种 V5.0 物理审计...", level="TWS-Msg"
        )
        winsound.Beep(2000, 300)

        # ✨【V5.0 链式对账】：
        # 不再手动读取 positions，而是驱动每个品种的类实例去执行“自我发现”
        for symbol, ctx in contexts_placeholder.items():
            try:
                # 1. 物理采集：咔嚓一下，拍下当前柜台真相
                snapshot = ctx.take_snapshot()

                if snapshot:
                    # 2. 逻辑同步：将真相灌装进内存状态机（纠正 state, 成本, 锁等）
                    ctx._sync_position(snapshot)

                    # 3. 摇铃标记：确保 5s 后的主循环节拍再次进行二次确认
                    ctx.filled_flag = True

                    ctx.log(
                        f"🔄 [链路自愈完成] 实际持仓:{snapshot.fact_pos} | 逻辑状态已同步。"
                    )
            except Exception as e:
                ctx.log(f"❌ [自愈失败] 品种 {symbol} 对账异常: {e}", level="ERROR")

    # --- C类：常规错误记录 ---
    else:
        # 在平时保持静默。
        # 如果有一天遇到莫名其妙的问题，可以临时取消下面的注释，
        # 看看 TWS 到底在后台“嘟囔”些什么。
        # sys_log(f"⚪ TWS-Notice: {error_msg}", level="TWS-Msg")
        pass


def standardize_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """[工业级最终定稿] 强制对齐 Pandas 字段名，根除 KeyError"""

    # 1. 入口守卫：如果是 None 或 空数据，直接原样返回，不执行任何操作
    if df is None:
        return df

    if df.empty:
        return df

    # 2. 字段标准化逻辑
    # 情况 A：实时行情数据 (ib_insync RealTimeBars 默认叫 'time')
    if "time" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"time": "datetime"})

    # 情况 B：历史 K 线数据 (ib_insync HistoricalData 默认叫 'date')
    # ✨ 注意：之前我给您的代码里这里写错了，现在修正为：把 'date' 改成 'datetime'
    if "date" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"date": "datetime"})

    # 3. 统一出口：直接返回处理后的 df
    return df


async def daily_closing_safety_guard(contexts):
    """[07-4 架构] 独立守护进程：负责在 15:56 触发集体清仓并关闭程序"""
    while True:

        now_time = datetime.now(EASTERN_TZ).time()
        # 统一在 15:56分触发集体平仓
        if now_time >= time(15, 56):
            sys_log(f"🚨 [收盘清仓开始] 系统时间已到 {now_time}，各股票执行清仓操作...")

            # 1. 并行调用所有 Context 对象的清仓逻辑
            tasks = [ctx.check_and_exit() for ctx in contexts.values()]
            await asyncio.gather(*tasks)

            sys_log("🏁 [收盘清仓完毕] 所有股票清仓完毕。")
            end_funds = await show_account_detail()
            sys_log(
                f"📊 [账户资金复核] 清仓后最终可用资金余额: ${end_funds:,.3f} USD",
                level="System",
            )
            # 2. ✨【核心修复点】：执行物理断电
            await asyncio.sleep(3)  # 先等候3秒钟，让资金审核和结账函数有足够的时间处理
            # 再优雅断开连接
            if ib.isConnected():
                ib.disconnect()
                sys_log("🔌 TWS 连接已断开 。")

            # 3. 强行终止进程，终结 main() 里的 while True 死循环
            sys_log("👋 量化交易系统工作已经全部完成，可以安全关机。再见。")
            os._exit(0)
        if now_time <= time(15, 50):
            await asyncio.sleep(300)  # 如果还没到 15:50，每 5 分钟检查一次
        else:
            await asyncio.sleep(
                30
            )  # 如果快到清仓时间了（15:50 - 15:56），每 30 秒检查一次，确保准时捕捉 15:55


async def contrl_c_exit(contexts):
    """
    [紧急避险中心 - 审计加固版]
    功能：响应 Ctrl+C，确保最后一笔紧急清仓成交记账后再安全断电
    """
    sys_log("\n🚨 [强行终止] 收到 Ctrl+C 手动终止信号！启动清仓程序...")
    for ctx in contexts.values():
        if hasattr(ctx, "bars_reference"):
            ib.cancelRealTimeBars(ctx.bars_reference)
    # 1. 并行执行所有 Context 的清仓逻辑
    if contexts:
        sys_log(f"🧹 正在执行清理 {len(contexts)} 个品种的持仓...")
        # 调用已经加固过的 check_and_exit (内部已带 lambda 绑定)
        tasks = [ctx.check_and_exit() for ctx in contexts.values()]
        await asyncio.gather(*tasks)

        # ✨ [2026-01-20 核心审计补丁]：留出 2 秒成交回报写入时间
        # 理由：给 TWS 成交信息通过网络返回并触发 CSV 写入预留物理时间
        sys_log("⏳ 正在等待清仓成交回报入账...")
        await asyncio.sleep(2)

    # 2. 最终战果对账
    end_funds = await show_account_detail()
    sys_log(
        f"📊 [账户对账] 程序结束。最终可用资金余额: ${end_funds:,.3f} USD",
        level="System",
    )

    # 3. 物理断电
    # ✨ [核心改进] 异步清理防线
    # 理由：在断开连接前，把所有还在“倒计时”的幽灵任务（如 check_subscription_async）物理切断
    try:
        current_loop = asyncio.get_event_loop()
        all_tasks = [
            t
            for t in asyncio.all_tasks(current_loop)
            if t is not asyncio.current_task()
        ]
        if all_tasks:
            sys_log(
                f"🧩 [系统清理] 正在物理终结 {len(all_tasks)} 个残留异步任务（防止日志幽灵跳出）..."
            )
            for task in all_tasks:
                task.cancel()
            # 允许循环运行一瞬间以处理 CancelledError
            await asyncio.gather(*all_tasks, return_exceptions=True)
    except Exception as e:
        pass  # 退出阶段的清理异常不影响整体关闭

    if ib.isConnected():
        ib.disconnect()
        sys_log("🔌 TWS 连接已安全断开。")

    sys_log("🏁 强行终止流程结束，系统退出。")
    # 强制退出进程，终结所有异步悬挂任务
    os._exit(0)


# ======================================================================
# --- 🏛️ V5.0 响应式架构：核心事实载体 ---
# ======================================================================
