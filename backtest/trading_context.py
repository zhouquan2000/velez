# ======================================================================
# trading_context.py - 精确导入清单
# ======================================================================

# --- 1. 标准库 ---
import os
import time as time_module
import traceback
import asyncio
from datetime import datetime, timezone, time, timedelta
from typing import Optional

# --- 2. 第三方库 ---
import pandas as pd
from ib_insync import (
    Stock,
    LimitOrder,
    MarketOrder,
    StopOrder,
    StopLimitOrder,
    TagValue,
    util,
    Contract,
    Trade,
    Order,
    IB,
    Index,
)

# --- 3. 项目内部模块 ---
from . import shared
from .shared import (
    sys_log,
    ensure_log_dir,
    EASTERN_TZ,
    ENTRY_TYPE_ABBREV,
    ENTRY_TYPE_FULL,
    global_last_vix_close,
    load_vix_data_async,
    vix_change_rate,
    get_account_available_funds,
    COND_MSG_MAP,
    standardize_df,
)
from .context_snapshot import ContextSnapshot

# --- 4. 可选：Windows 声音报警 ---
try:
    import winsound
except ImportError:
    winsound = None
# ======================================================================
# --- 📦 类定义：TradingContext ---
# ======================================================================


class TradingContext:

    def __init__(self, symbol, ib, loop, **kwargs):
        self.symbol = symbol
        self.ib = ib
        self.contract = Stock(symbol, "SMART", "USD")
        self.loop = loop
        # --- 1. [风险与执行参数] ---
        self.risk_unit = float(kwargs.get("risk_unit", 120))  # 默认$120
        self.max_qty = int(kwargs.pop("max_qty", 200))  # 默认200股
        self.eb_range_mult = float(kwargs.pop("eb_range_mult", 2.0))
        self.eb_vol_mult = float(kwargs.pop("eb_vol_mult", 1.2))
        self.eb_body_ratio = float(kwargs.pop("eb_body_ratio", 0.85))

        self.static_atr = float(kwargs.pop("static_atr", 0.5))  # 初始静态ATR
        self.atr = self.static_atr  # 动态ATR缓存
        self.effective_atr = 0.0  # 用于 plan_trade 的动态 ATR 锚点
        self.min_gap_factor = 0.8  # 强制止损呼吸空间因子 (0.8 * ATR)
        self.tp1_ratio = 0.5  # TP1 默认减仓比例 (50%)
        self.risk_pyramid_factor = 0.5  # 加仓风险折减系数
        self.slippage_allowance = float(kwargs.pop("slippage_allowance", 0.05))
        self.custom_params = kwargs  # 用于扩展参数
        self.pnl_total = 0.0
        self.filled_flag = False
        self.chase_flag = False  # 追单标志
        # --- [各个law信号满足时候的变量] 用于固化探测瞬间的价格锚点 ---
        self.law1_sl = None
        self.law1_trigger_high = None
        self.law1_trigger_low = None
        self.law1_entry_type = None

        self.law2_sl = None
        self.law2_trigger_high = None
        self.law2_trigger_low = None
        self.law2_entry_type = None

        self.law3_sl = None
        self.law3_trigger_high = None
        self.law3_trigger_low = None
        self.law3_entry_type = None

        self.law4_sl = None
        self.law4_trigger_high = None
        self.law4_trigger_low = None
        self.law4_entry_type = None

        self.law5_sl = None
        self.law5_trigger_high = None
        self.law5_trigger_low = None
        self.law5_entry_type = None

        self.law6_sl = None
        self.law6_trigger_high = None
        self.law6_trigger_low = None
        self.law6_entry_type = None

        self.law8_sl = None
        self.law8_trigger_high = None
        self.law8_trigger_low = None
        self.law8_entry_type = None

        self.v180_sl = None
        self.v180_trigger_high = None
        self.v180_trigger_low = None
        self.v180_entry_type = None

        # 🔥🔥🔥【新增】Law1 大象柱关键价位（供 plan_trade 使用）
        self.law1_elephant_high = 0.0
        self.law1_elephant_low = 0.0
        self.law1_elephant_open = 0.0
        self.law1_elephant_close = 0.0
        self.law1_score = 0
        self.law1_soft_clear = False
        self.law1_hard_clear = False
        self.law1_context = None

        # 🔥🔥🔥【新增】orderRef 解析结果缓存（供 sync_position 使用）
        self.order_features = (
            {}
        )  # {order_id: {"order_type": "E", "entry_type": "Breakout", ...}}

        # --- 2. [态势感知与法则旗语] ---
        # Law #1-8 信号位 (由 _detect_market_patterns 驱动)
        self.ready_to_long_law1 = self.ready_to_short_law1 = False  # Elephant Bar
        self.ready_to_long_law2 = self.ready_to_short_law2 = False  # Color Change
        self.ready_to_long_law3 = self.ready_to_short_law3 = False  # 3-5 Bars
        self.ready_to_long_law4 = self.ready_to_short_law4 = False  # RBI/GBI
        self.ready_to_long_law5 = self.ready_to_short_law5 = False  # 20MA Cross
        self.ready_to_long_law6 = self.ready_to_short_law6 = False  # Home Run
        self.ready_to_long_law8 = self.ready_to_short_law8 = False  # Fab 42
        self.ready_to_long_v180 = self.ready_to_short_v180 = False  # v180反转

        # 辅助形态记录
        self.current_tail_type = None  # 存放当前正在探测的K线形态 (BT/TT)
        self.last_confirmed_tail = None  # 存放上一根已经收盘确认的K线形态
        self.bars_reference = None

        self.elephant_bar_log = []  # 大象柱分析日志
        self.red_bar_count = 0  # 实时红柱计数
        self.green_bar_count = 0  # 实时绿柱计数
        self.last_red_count = 0  # ✨ 新增：变绿瞬间，备份之前的红柱数
        self.last_green_count = 0  # ✨ 新增：变红瞬间，备份之前的绿柱数
        self.low_of_dip = 0.0
        self.high_of_bounce = 0.0

        self.dip_of_2min = 0.0
        self.bounce_of_2min = 0.0
        self.low_of_4min = 0.0
        self.high_of_4min = 0.0

        # --- 3.1 [状态机与影子账本] ---
        self.last_processed_time = (
            None  # 初始化为 None，确保第一次计算能顺利通过哨兵校验
        )
        self.state = "OPEN_STAGE"  # 核心状态机
        self.order_place_time = 0
        self.stop_qty_timeout = (
            15  # 主订单部分成交之后，超过15秒没有剩余订单的成交，就视为超时了
        )
        self.holding_start_time = 0
        self.partially_filled_time = 0
        self.actual_filled_qty = 0  # 当前物理持仓数量 (绝对值)
        self.avg_fill_price = 0.0  # 当前持仓的平均成本
        self.is_exiting = False  # ✨ 新增：离场/减仓专用内存锁
        self.entry_law = None  # 入场信号标签
        self.position_side = ""  # LONG / SHORT
        self.last_entry_price = 0.0  # 信号触发参考价
        self.latest_snapshot = None
        self.current_cond = "Cond_01_IDLE"
        self.last_cond = "Cond_01_IDLE"
        self._temp_order_audit = {
            "order_id": 0,  # 捕获 p_order.orderId
            "label": "IDLE",  # 捕获指令中的 label
            "trigger_price": 0.0,  # 触发时的参考价 (财务对账基准)
            "last_p_lmt": 0.0,  # 下单瞬间的主订单的 LMT 价
            "last_s_aux": 0.0,  # 下单瞬间的止损单的 Stop 价格（用于固定 Gap）
        }
        self.initial_stop_price = (
            0.00  # 主订单下单时候的止损价格，当主订单成交那一刻记录下来
        )
        self.last_stop_cond = "None"
        self.current_stop_cond = "None"
        self.current_tp_cond = "None"
        self.last_tp_cond = "None"
        self.final_stop_price = 0.0  # 动态调整之后的止损单上的止损价格
        self.trade_records = []  # 交易审计记录
        self._loss_recorded_orders = set()
        # self.in_flight_qty = 0          # 正在 TWS 柜台挂着的平仓股数 (绝对值)
        self.last_bar_close_minute = None  # 用于 15min 跨线判定
        self.last_patch_time = 0  # 用于补票频率限制
        self.last_exec_ts = 0.0
        self.exec_window_sec = 6.0
        self.live_ticker = None
        self.plan_loss = 0.00  # 下单准备的初始亏损金额
        # 3.2  影子哨兵参数 (对齐 execute_trade / manage_position)
        self.tp1 = 0.0  # 减仓目标价
        self.tp2 = 0.0  # 终极目标价

        self.tp1_filled = False  # TP1止盈单是否已经成交了
        self.last_trade_qty = 0  # 初始成交总股数
        self.is_pyramid_processed = False
        self.is_processing_order = False  # 入场/加仓锁：防止主订单重复提交
        self.tp1_qty = 0  # 计划止盈单TP1的手数
        self.currrent_stop_cond = ""
        self.currrent_tp_cond = ""
        self.last_stop_cond = "IDLE"
        self.last_tp_cond = "IDLE"

        # --- 3.3 [财务审计与对账开关] ---
        self.entry_total_count = 0  # 累计成交笔数
        self.processed_exec_ids = (
            set()
        )  # ✨ 核心加固：存储已处理的成交 ID，防止重复计账

        self.settlement_ledger = {}
        self.exec_id_map = {}
        self.order_fill_map = {}
        self.seen_exec_ids_by_oid = {}
        self.pending_commission = {}
        # --- 4. [物理隔离数据中心] ---
        # ✨ 4.1 [面粉] 5秒原始缓存：专门存放 on_bar_update 实时泵入的数据
        self.raw_5s_buffer = pd.DataFrame()
        # ✨ 4.2 [面包] 2分钟K线仓库：用于计算指标、Law探测及 CSV 存档
        self.history_2min_bars = pd.DataFrame()
        self.kline_cache_15m = pd.DataFrame()
        # ✨ 4.3. [锚点] 历史对齐指针：None 代表未对齐，由 load_history_data 激活
        self.last_hist_kline_time = None
        # --- 4.4 [存放当前最新的、待执行的信号包（包含方向、止损、目标位等）] ---
        self.active_signal = None
        # --- 5. [趋势环境技术指标] ---
        self.ma8 = 0.0  # 快速追踪均线
        self.ma20 = 0.0  # 主趋势均线
        self.ma200 = None  # 长期基准线
        self.ma20_prev = 0.0  # T-1 均线值
        self.ma20_prev2 = 0.0  # T-2 均线值
        self.is_ma20_turning_up = False
        self.is_ma20_turning_down = False
        self.is_super_uptrend = False
        self.is_super_downtrend = False

        # 15分钟高级感知
        self.ma20_15m_val = 0.0
        self.ma20_15m_prev = 0.0
        self.is_15m_trending_up = False
        self.is_15m_trending_down = False

        # --- 6. [订单对象与熔断] ---
        self.strade = None  # 止损单引用
        self.parent_trade = None  # 主单引用
        self.tp_trade = None  # 止盈单引用
        self.suspend_today = False  # 当日熔断
        self._last_vix_warn = False  # VIX 警告位
        self.consecutive_losses = 0  # 连续亏损计数
        self.margin_requirement = 0.3  # 保证金要求
        self.capital_buffer = 0.05

        # 日志配置
        today_str = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
        self.log_filename = os.path.join(
            ensure_log_dir(), f"{today_str}_{self.symbol}_Sys_log.txt"
        )
        self.trade_filename = os.path.join(
            shared.ensure_log_dir(), f"{today_str}_{self.symbol}_Trade_log.csv"
        )
        self.sys_log(f"✅{symbol}的变量初始化工作完成", level="INFO")

    def sys_log(self, message, level="INFO"):
        """
        [07-12 品种级桥接]
        职责：将品种信息封装后，递交给全局输出引擎
        """
        # 1. 业务图标定义
        biz_icons = {
            "DECISION": "🎯",
            "INFO": "💡",
            "WARN": "⚠️",
            "ERROR": "🚫",
            "FILTER": "🛡️",
            "DEBUG": "🛠️",
        }
        icon = biz_icons.get(level, "🔹")

        # 2. 构造带品种的消息
        # 例如：[AAPL] 🎯 TP1 触达
        context_msg = f"[{self.symbol}] {icon} {message}"

        # 3. 提交给全局引擎物理输出 (level 传给全局决定图标，如果全局没有则用 🔹)
        sys_log(context_msg, level=level)
        # 4. 同步写入该品种的专属文件 (06-9 优良传统)
        try:
            with open(self.log_filename, "a", encoding="utf-8") as f:
                ts = datetime.now(EASTERN_TZ).strftime("%H:%M:%S.%f")[:-3]
                f.write(f"[{ts}] {context_msg}\n")
        except:
            pass

    log = sys_log  # 别名，方便调用

    async def check_subscription_async(self, timeout=5):
        """[类方法] 行情权限审计：异步验证实时是否具有实时行情权限"""
        self.sys_log(
            f"📡 正在检查验证 {self.symbol} 是否能接收到来自TWS的实时行情 (探测时长: {timeout}s)..."
        )
        try:
            # 订阅行情流
            if not hasattr(self, "live_ticker") or self.live_ticker is None:
                self.live_ticker = self.ib.reqMktData(self.contract, "", False, False)

            # 等待指定时间，或直到有数据
            start_t = time_module.time()
            while time_module.time() - start_t < timeout:
                await asyncio.sleep(0.5)
                t = self.live_ticker
                if t and ((t.last or 0) > 0 or (t.bid or 0) > 0):
                    self.sys_log(
                        f"✅ {self.symbol} 可以实时获取到TWS的实时行情数据，验证通过。"
                    )
                    return True

            self.ib.cancelMktData(self.contract)
            self.sys_log(
                f"❌ {self.symbol} 实时获取行情验证超时！请确认今天是交易日并且确认TWS已订阅该市场实时行情。"
            )
            self.play_sound("ERROR")
            return False
        except Exception as e:
            self.sys_log(f"⚠️ 实时获取行情数据异常，错误代码: {e}")
            return False

    async def sync_initial_state(self):
        """
        [类方法/自愈对账 V5.0]
        职责：程序启动瞬间拍摄快照，强制内存与柜台事实对齐，接管实盘持仓或在途单。
        """
        try:
            # 1. 采集物理真相 (利用已有的无状态采集轮子)
            snapshot = self.take_snapshot()
            if not snapshot:
                self.sys_log(
                    f"⚠️ {self.symbol} 初始快照采集失败，跳过对账", level="WARN"
                )
                return

            # 2. 环境事实同步
            if snapshot.has_position:
                # --- 场景 A：发现实盘持仓 ---
                self.state = "HOLDING_STAGE"
                self.actual_filled_qty = snapshot.fact_pos
                self.avg_fill_price = snapshot.avg_cost
                self.last_trade_qty = snapshot.abs_pos  # 启动瞬间将实仓设为基准分母

                # 识别方向（快照自动判定）
                self.position_side = snapshot.direction

                self.sys_log(
                    f"🕵️ [启动对账] 发现{self.symbol}实盘持仓: {snapshot.fact_pos}股 | 均价: {snapshot.avg_cost:.2f}"
                )

                if snapshot.active_stop_order:
                    self.final_stop_price = snapshot.active_stop_order.auxPrice
                    self.sys_log(
                        f"🩹 [启动对账] 已找回柜台止损单，止损价: {self.final_stop_price}"
                    )
                self.filled_flag = True
            else:
                # --- 场景 B：无持仓，检查是否有在途入场单 ---
                # 寻找 parentId == 0 的非止损挂单 (即入场单)
                entry_trade = next(
                    (
                        o
                        for o in snapshot.live_orders
                        if o.parentId == 0 and o.orderType not in ["STP", "STP LMT"]
                    ),
                    None,
                )

                if entry_trade:
                    self.state = "ORDER_SENT"
                    self.sys_log(
                        f"🛰️ [启动对账] 发现入场单在途，重置状态为 ORDER_SENT | ID: {entry_trade.orderId}"
                    )
                    self.filled_flag = True
                else:
                    self.state = "OPEN_STAGE"
                    self.sys_log(
                        f"📡 [启动对账] {self.symbol} 账户空闲，处于待机状态。"
                    )

            # 3. 🛡️ 重建精算快照 (用于 log_trade 财务对账兜底)

            self._temp_order_audit = {
                "order_id": 0,
                "label": "REBOOT",
                "trigger_price": snapshot.avg_cost if snapshot.has_position else 0.0,
                "last_p_lmt": 0.0,
                "last_s_aux": self.final_stop_price,
            }

            self.sys_log(
                f"✅ {self.symbol} V5.0 逻辑链路初始化完成，当前持仓: {snapshot.fact_pos}",
                level="INFO",
            )

        except Exception as e:
            self.sys_log(f"❌ {self.symbol} 启动自愈对账异常: {e}", level="ERROR")

    def estimate_ibkr_commission(
        self, qty: float, price: float, direction: str
    ) -> float:
        """[类方法] 估算 IBKR 阶梯佣金与监管费 (对齐 06-9)"""
        qty = abs(qty)
        # 1. 基础阶梯佣金 (Tiered)
        base_comm = max(0.35, qty * 0.0035)

        # 2. 监管费 (Regulator Fees)
        sec_fee = 0.0
        finra_fee = 0.0

        if direction.upper() == "SELL":
            # SEC 费率 (估算值)
            sec_fee = (qty * price) * 0.0000229
            # FINRA 费率 (0.000119/股，最高 5.95)
            finra_fee = min(5.95, qty * 0.000119)

        total_comm = base_comm + sec_fee + finra_fee
        return round(total_comm, 2)

    def log_trade(
        self,
        time_str: str,
        action: str,
        qty: float,
        price: float,
        realized_pnl: float,
        commission: float,
        exec_id: str = "",
        label=None,
        order_id: int = None,
        order_ref: str = None,
    ):
        """
        [事实审计版] 接收 TWS 官方推送的成交数据进行归档与连损判定
        新增参数:
            commission: TWS 官方回传的准确佣金
            exec_id: 成交唯一编号，用于物理去重
        """
        try:
            # --- 1. 物理去重检查 (防止 TWS 重复推送同一笔成交) ---
            if exec_id:
                if exec_id in self.processed_exec_ids:
                    return
                self.processed_exec_ids.add(exec_id)

            # --- 2. 财务核心核算 ---
            is_closing = False
            if order_ref:
                is_closing = (
                    order_ref.startswith("S_")
                    or order_ref.startswith("ES")
                    or order_ref.startswith("TP")
                    or order_ref.startswith("CL")
                )

            if is_closing:
                # 平仓： 计算净盈亏 和累计盈亏
                net_pnl = round(realized_pnl, 2)
                self.pnl_total += net_pnl

            else:
                # 开仓：净盈亏为 手续费，不累加
                net_pnl = 0.00
                self.pnl_total += net_pnl
                self.entry_total_count += 1
                self.sys_log(
                    f"📝 [开仓佣金] commission={commission:.2f}", level="DEBUG"
                )

            # --- 3. 连损判定与熔断逻辑 ---
            if is_closing:
                # 只有在产生实际亏损时才检查是否触及底线
                half_plan_loss = 0.5 * self.plan_loss
                if net_pnl < 0 and abs(net_pnl) >= half_plan_loss:
                    self.consecutive_losses += 1
                    if order_id is not None:
                        self._loss_recorded_orders.add(order_id)
                    self.sys_log(
                        f"📉 [风险控制]本次交易亏损{net_pnl:.2f}超过了{0.5*self.plan_loss:.2f}（计划亏损额度的一半），连续亏损计数器+1，已经连续亏损: {self.consecutive_losses}次",
                        level="WARN",
                    )
                    log_msg = (
                        f"💰 [对账-{label or 'N/A'}]"
                        f"💰  {action} {qty} @ {price:.3f} | "
                        f"平仓佣金: {commission:.2f} | 本次净盈亏: {net_pnl:.2f} | "
                        f"累计PnL: {round(self.pnl_total, 2)} "
                    )
                    self.sys_log(log_msg, level="INFO")
                elif net_pnl < 0 and abs(net_pnl) < half_plan_loss:
                    self.sys_log(
                        f"🛡️ [风险管理]本次交易亏损金额{net_pnl:.2f}，但是没有超过{0.5*self.plan_loss:.2f}(计划亏损额度的一半),连续亏损计数器不变。"
                    )
                    log_msg = (
                        f"💰 [对账-{label or 'N/A'}]"
                        f"💰  {action} {qty} @ {price:.3f} | "
                        f"平仓佣金: {commission:.2f} | 本次净盈亏: {net_pnl:.2f} | "
                        f"累计PnL: {round(self.pnl_total, 2)} "
                    )
                    self.sys_log(log_msg, level="INFO")

                elif net_pnl > 0:
                    self.consecutive_losses = 0
                    self.sys_log(f"🛡️ [风险管理]本次交易盈利离场，连损记数器归零。")
                    log_msg = (
                        f"💰 [对账-{label or 'N/A'}]"
                        f"💰  {action} {qty} @ {price:.3f} | "
                        f"平仓佣金: {commission:.2f} | 本次净盈亏: {net_pnl:.2f} | "
                        f"累计PnL: {round(self.pnl_total, 2)} "
                    )
                    self.sys_log(log_msg, level="INFO")

                else:  # net_pnl =0
                    self.sys_log(f"🎉 [风险管理]净盈亏{net_pnl:.2f}，连损记数器不变。")
                    log_msg = (
                        f"💰 [对账-{label or 'N/A'}]"
                        f"💰  {action} {qty} @ {price:.3f} | "
                        f"平仓佣金: {commission:.2f} | 本次净盈亏: {net_pnl:.2f} | "
                        f"累计PnL: {round(self.pnl_total, 2)} "
                    )
                    self.sys_log(log_msg, level="INFO")

                self.plan_loss = (
                    0.00  # 交易平仓结束，对账记账也结束，本次交易的plan_loss可以归零了
                )
            # --- 4. 判定是否触发今日熔断 ---
            # 逻辑 4.1：战术熔断（原有逻辑：连续 3 次触及初始止损底线失败）
            is_tactical_suspend = self.consecutive_losses >= 3
            # 逻辑 4.2：财务熔断（新增建议：个股亏损超过 3 倍 Risk Unit）
            # 假设 risk_unit 是 120，亏损 360 就停
            max_pnl_loss = -(3 * getattr(self, "risk_unit", 120))
            is_financial_suspend = self.pnl_total <= max_pnl_loss
            # 逻辑 4.3：财务熔断（无论是连续3次止损还是亏损金额到了每日允许的上限，都高悬免战牌）
            if is_tactical_suspend or is_financial_suspend:
                self.suspend_today = True
                self.play_sound("SUSPEND")
                reason = (
                    "连续止损3次"
                    if is_tactical_suspend
                    else f"今日本股票{self.symbol}已经亏损{self.pnl_total}超过当日上限({max_pnl_loss})"
                )
                self.sys_log(
                    f"🚨 [个股熔断] {self.symbol} 触发停止交易。原因: {reason}",
                    level="ERROR",
                )

            # --- 5. 构造标准审计记录 ---
            record = {
                "time": time_str,
                "symbol": self.symbol,
                "action": action,
                "qty": qty,
                "price": round(price, 3),
                "net_pnl": net_pnl,
                "commission": round(commission, 2),
                "cum_pnl": round(self.pnl_total, 2),
                "loss_streak": self.consecutive_losses,
                "exec_id": exec_id,
                "label": label or "UNKNOWN",  # ✨ 建议加上：让 CSV 有灵魂
            }
            self.trade_records.append(record)

            # --- 6. 物理增量落盘 (CSV) ---
            df_item = pd.DataFrame([record])
            file_exists = os.path.isfile(self.trade_filename)
            df_item.to_csv(
                self.trade_filename,
                mode="a",
                index=False,
                header=not file_exists,
                encoding="utf-8",
            )

        except Exception as e:
            self.sys_log(f"❌ log_trade 财务对账或物理落盘异常: {e}", level="ERROR")

    def _save_cond_log(self, cond_code):
        """
        [审计持久化] 将每一拍的对账结果存入物理文件
        """
        try:
            # 获取通俗解释
            cond_msg = COND_MSG_MAP.get(cond_code, "未定义异常状态")

            # 构造审计行
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            log_line = (
                f"[{timestamp}] | Cond: {cond_code.ljust(12)} | Msg: {cond_msg} | "
                f"Pos: {self.actual_filled_qty} | State: {self.state}\n"
            )

            # 文件路径：例如 log_file/2026-03-04_AAPL_cond_log.txt
            file_path = os.path.join(
                ensure_log_dir(),
                f"{datetime.now().strftime('%Y-%m-%d')}_{self.symbol}_cond_log.txt",
            )

            with open(file_path, "a", encoding="utf-8") as f:
                f.write(log_line)

        except Exception as e:
            print(f"❌ 审计日志写入失败: {e}")

    def play_sound(self, sound_type: str):
        """[类方法] 感官报警系统：为不同交易事件分配独立音色 (移植自 06-9)"""
        try:
            if sound_type == "ENTRY":
                winsound.Beep(1200, 300)  # 入场：高音 (1200Hz)
            elif sound_type == "EXIT":
                winsound.Beep(800, 500)  # 出场：中音 (800Hz)
            elif sound_type == "ELEPHANT":
                # 大象柱发现：急促三连音
                for _ in range(3):
                    winsound.Beep(1500, 100)
            elif sound_type == "SUSPEND":
                # 熔断警报：长低音 (400Hz)
                winsound.Beep(400, 1000)
            elif sound_type == "ALERT":
                winsound.Beep(1000, 500)  # 一般预警
        except Exception:
            pass  # 容错处理

    async def load_history_data(self):
        """
        [数据中心-物理隔离重塑版 07-18]
        职责：
        1. 抓取历史 2min K线并存入物理隔离仓库 history_2min_bars（面包）。
        2. 严禁污染 raw_5s_buffer（面粉）。
        3. 锚定最后时刻为 last_hist_kline_time，作为实时补票的基准。
        """
        self.sys_log(f"🚀 正在加载{self.symbol}的2分钟K线历史数据...", level="INFO")

        try:
            # --- 1. 环境准备：锚定 UTC 时间 ---
            market_open_time = time(9, 30)
            now_utc = datetime.now(timezone.utc)
            end_time_str = now_utc.strftime("%Y%m%d %H:%M:%S UTC")
            now_et = datetime.now(EASTERN_TZ)

            # --- 2. 原始数据抓取 (强制 RTH 纯净) ---
            bars = await self.ib.reqHistoricalDataAsync(
                self.contract,
                endDateTime=end_time_str,
                durationStr="3 D",
                barSizeSetting="2 mins",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )

            # --- 3. 原子级审计区 ---
            raw_df = util.df(bars)
            if raw_df is None or raw_df.empty:
                self.sys_log("❌ TWS 返回历史数据为空，尝试兜底...", level="ERROR")
                self.static_atr = 0.20
                return False

            raw_df = standardize_df(raw_df)
            if raw_df is None or raw_df.empty:
                return False

            # --- 4. 数据清洗与时区转换 ---
            target_col = next(
                (c for c in ["datetime", "date", "time"] if c in raw_df.columns), None
            )
            if not target_col:
                self.sys_log("🚨 无法找到datetime 时间列名", level="ERROR")
                return False

            # 强制转换与物理级 RTH 过滤 (09:30-15:58)
            raw_df[target_col] = pd.to_datetime(
                raw_df[target_col], utc=True
            ).dt.tz_convert(EASTERN_TZ)
            raw_df = (
                raw_df.set_index(target_col)
                .between_time("09:30", "15:58")
                .reset_index()
            )
            raw_df.rename(columns={target_col: "datetime"}, inplace=True)

            if raw_df.empty:
                self.sys_log("⚠️ RTH 过滤后无有效历史数据", level="WARN")
                return False

            # --- 5. 静态 ATR 计算 (逻辑保持不变) ---
            raw_df["date_str"] = pd.to_datetime(raw_df["datetime"]).dt.strftime(
                "%Y-%m-%d"
            )
            all_dates = sorted(raw_df["date_str"].unique())
            curr_date_str = now_et.strftime("%Y-%m-%d")

            if curr_date_str in all_dates and now_et.time() >= market_open_time:
                prev_date = all_dates[-2] if len(all_dates) >= 2 else all_dates[-1]
            else:
                prev_date = all_dates[-1]

            prev_day_data = raw_df[raw_df["date_str"] == prev_date]
            self.static_atr = (
                round((prev_day_data["high"] - prev_day_data["low"]).mean(), 4)
                if len(prev_day_data) >= 30
                else 0.20
            )

            # --- 6. 物理落地 CSV ---
            safe_time_str = end_time_str.replace(":", "-")
            K_LINE_FILE = os.path.join(
                shared.ensure_log_dir(), f"{self.symbol}-2min-Kline-{safe_time_str}.csv"
            )
            try:
                raw_df.to_csv(K_LINE_FILE, index=False)
            except Exception as e:
                self.sys_log(f"⚠️ CSV 写入失败，错误代码: {e}", level="WARN")

            # --- 7. ✨ 核心物理隔离操作 ✨ ---

            # A. 填充 2min 成品仓库 (面包库)
            self.history_2min_bars = raw_df.copy()

            # B. 绝对排空 5s 缓冲区 (确保启动瞬间补票逻辑触发)
            self.raw_5s_buffer = pd.DataFrame()

            # C. 锚定最后时刻 (去掉秒数，确保与 2min 节点严格对齐)
            last_bar_time = raw_df["datetime"].iloc[-1].replace(second=0, microsecond=0)
            # ✨ [开盘对齐补丁]：如果历史数据停留在昨日，且当前已接近或处于今日交易时段
            # 则强制将锚点对齐到今日 09:30，物理跳过隔夜空档，防止合成器刷屏
            now_et = datetime.now(EASTERN_TZ)
            today_market_open = now_et.replace(
                hour=9, minute=30, second=0, microsecond=0
            )

            if last_bar_time < today_market_open:
                self.last_hist_kline_time = today_market_open
                self.sys_log(
                    f"⏰ 历史数据加载到({last_bar_time.strftime('%m-%d %H:%M')})，系统自动对齐至今日开盘锚点: {self.last_hist_kline_time.strftime('%H:%M')}",
                    level="INFO",
                )
            else:
                self.last_hist_kline_time = last_bar_time

            # D. 指标初始化对齐
            self.effective_atr = self.static_atr

            # ==========================================================
            # ✨ [热机逻辑 - 插入此处] ✨
            # 职责：在正式开启实时探测前，将今日已发生的连涨/连跌趋势补齐到计数器中
            # ==========================================================
            today_str = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
            dt_series = pd.to_datetime(raw_df["datetime"])
            # 仅提取今日 RTH 时段的数据进行模拟回放
            today_bars = raw_df[dt_series.dt.strftime("%Y-%m-%d") == today_str]

            # 初始化状态
            self.red_bar_count = 0
            self.green_bar_count = 0
            self.last_red_count = 0
            self.last_green_count = 0
            self.high_of_bounce = 0.0
            self.low_of_dip = 0.0

            if not today_bars.empty:
                for _, bar in today_bars.iterrows():
                    if bar["close"] > bar["open"]:
                        if self.red_bar_count > 0:
                            self.last_red_count = self.red_bar_count
                            self.green_bar_count = 1
                            self.high_of_bounce = bar["high"]
                            self.low_of_dip = 0.0
                        else:
                            self.green_bar_count += 1
                            self.high_of_bounce = max(self.high_of_bounce, bar["high"])
                        self.red_bar_count = 0
                    elif bar["close"] < bar["open"]:
                        if self.green_bar_count > 0:
                            self.last_green_count = self.green_bar_count
                            self.red_bar_count = 1
                            self.low_of_dip = bar["low"]
                            self.high_of_bounce = 0.0
                        else:
                            self.red_bar_count += 1
                            self.low_of_dip = (
                                min(self.low_of_dip, bar["low"])
                                if self.low_of_dip > 0
                                else bar["low"]
                            )
                        self.green_bar_count = 0
                self.last_processed_time = today_bars.iloc[-1]["datetime"]
                self.sys_log(
                    f"🔥 热机完成：🔴红:{self.red_bar_count} | 🟢绿:{self.green_bar_count}",
                    level="INFO",
                )

            await self.update_15m_cache(is_init=True)
            self.calculate_indicators()

            # ✨ [热机终点线：在此处插入] ✨
            if not today_bars.empty:
                # 热机结束后，将指针推移到今日历史 Bar 的终点，防止合成器二次处理
                # 这行代码是解决“开盘刷屏”和“中途重启重复合成”的核武级加固
                self.last_hist_kline_time = today_bars.iloc[-1]["datetime"].replace(
                    second=0, microsecond=0
                )
                self.sys_log(
                    f"🔥 态势感知热机完成：当前指针对齐至 {self.last_hist_kline_time}",
                    level="INFO",
                )

            # --- 8. 隔离效果审计日志 ---
            self.sys_log(
                f"✅ {self.symbol}历史数据加载成功，加载{len(self.history_2min_bars)}根K线",
                level="INFO",
            )

            return True

        except Exception as e:
            self.sys_log(
                f"🚨 [Fatal Error] 2分钟K线历史数据加载失败，错误代码: {e}",
                level="CRITICAL",
            )
            return False

    async def update_15m_cache(self, is_init=False):
        """[类方法-自律版] 彻底解决数据覆盖、Timeout与指标刷新逻辑"""
        now_et = datetime.now(EASTERN_TZ)
        now_time = now_et.time()

        # --- 🛡️ 1. 避障门槛 (这是解决 9:30-9:45 报错的关键) ---
        # 如果是盘中调用(非Init) 且 处于首根线未闭合期，直接退出，不发请求
        if not is_init and time(9, 30) <= now_time < time(9, 45, 5):
            return True

        # --- 🛡️ 2. 请求参数决策 ---
        # 初始化拉3天背景；盘中更新拉1天(约26根)进行增量缝合
        duration = "3 D" if is_init else "1 D"

        try:
            # 错峰与请求 (保持 UTC 锚点对齐)
            end_time_str = datetime.now(timezone.utc).strftime("%Y%m%d %H:%M:%S UTC")
            bars = await self.ib.reqHistoricalDataAsync(
                self.contract,
                endDateTime=end_time_str,
                durationStr=duration,
                barSizeSetting="15 mins",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )

            raw_df = util.df(bars)
            if raw_df is None or raw_df.empty:
                return False

            df = standardize_df(raw_df)
            if df is None or df.empty:
                self.sys_log(
                    "⚠️ [15m同步] 标准化后的数据为空，跳过本次更新", level="WARN"
                )
                return False  # 安全退出，不执行后续报错代码

            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(
                EASTERN_TZ
            )

            # --- 🛠️ 3. 核心改进：无损增量缝合 (防止数据失血) ---
            if not hasattr(self, "kline_cache_15m") or is_init:
                # 初始态或强制初始化：全量覆盖
                self.kline_cache_15m = df.copy()
            else:
                # 运行态：将 1D 新数据缝合进旧的 3D 背景
                self.kline_cache_15m = pd.concat(
                    [self.kline_cache_15m, df], ignore_index=True
                )
                # 物理去重并排序
                self.kline_cache_15m.drop_duplicates(
                    subset=["datetime"], keep="last", inplace=True
                )
                self.kline_cache_15m.sort_values("datetime", inplace=True)
                # 保持 100 根左右的长度，足以应付 MA20/50 计算
                self.kline_cache_15m = self.kline_cache_15m.tail(100)

            # --- 🛡️ 4. 指标驱动：确保 15min 趋势实时更新 ---
            v_count = len(self.kline_cache_15m)
            if v_count >= 20:
                # 刷新 15min MA20 斜率、大象柱状态等
                self.check_15m_elephant_status()
                self.calculate_15m_trend(now_et)
                self.sys_log(
                    f"✅ 15min同步成功 | 模式:{'Init' if is_init else 'Live'} | 总样本:{v_count}",
                    level="INFO",
                )

            return True

        except Exception as e:
            self.sys_log(f"🚨 15min同步异常: {e}", level="ERROR")
            return False

    def check_15m_elephant_status(self):
        """[Law #8 核心组件-加固版] 增加了数值审计日志，防止灵异判定"""
        # 1. 物理防御：严防 NoneType 崩溃
        self.is_15m_elephant = False
        self.is_15m_elephant_bull = False
        if self.kline_cache_15m is None:
            return
        if self.kline_cache_15m.empty or len(self.kline_cache_15m) < 10:
            return

        last_15m = self.kline_cache_15m.iloc[-1]

        # 计算 15min 波动审计
        range_15m = round(last_15m["high"] - last_15m["low"], 2)
        avg_range_15m = round(
            (self.kline_cache_15m["high"] - self.kline_cache_15m["low"])
            .tail(20)
            .mean(),
            2,
        )
        body_15m = round(abs(last_15m["close"] - last_15m["open"]), 2)

        # 严格门槛：实体占比 > 70%，波动 > 1.8倍平均
        threshold_range = round(1.8 * avg_range_15m, 2)
        ratio = round(body_15m / range_15m if range_15m != 0 else 0, 2)

        is_eb_15m = (range_15m > threshold_range) and (ratio > 0.7)

        if is_eb_15m:
            side = "BULL" if last_15m["close"] > last_15m["open"] else "BEAR"
            self.is_15m_elephant = True  # ← 新增标志位
            self.is_15m_elephant_bull = side == "BULL"

            self.sys_log(
                f"🔥 [15min 大象柱] {side}! | 实体:{body_15m} | 门槛:{threshold_range} | 占比:{ratio}"
            )
            self.last_15m_eb_time = last_15m["datetime"]
            self.sys_log(
                f"🔥 [Law #8 审计通过] 15min {side} 大象! | 实体:{body_15m} | 门槛:{threshold_range} | 占比:{ratio}"
            )
        else:
            # 可选：Debug 时输出为什么没触发
            # self.sys_log(f"🔍 [Law #8 审计未触发] 15min 实体:{body_15m} (门槛:{threshold_range})")
            pass

    def calculate_15m_trend(self, ts: datetime):
        """[类方法] 计算 15min 大周期的趋势方向"""
        # 提示：
        # 1. 检查 self.kline_cache_15m 是否满足 20 根
        # 2. 将结果存入 self.is_15m_trending_up 和 self.is_15m_trending_down
        self.is_15m_trending_up = False
        self.is_15m_trending_down = False
        if (
            isinstance(self.kline_cache_15m, pd.DataFrame)
            and len(self.kline_cache_15m) >= 20
        ):
            last_sync_time = self.kline_cache_15m["datetime"].iloc[-1]
            # 计算当前 5s Bar 时间与最后一根 15min 柱时间的差值（秒）
            time_diff = (ts - last_sync_time).total_seconds()

            # 🛡️ 风险控制：如果 15min 数据延迟超过 22 分钟（1320秒），判定数据失效
            # 正常情况下，在 XX:15:05 同步后，time_diff 应该是 5-600 秒左右
            if time_diff > 1320:
                if ts.minute % 5 == 0:  # 减少日志频率，每 5 分钟提醒一次
                    self.sys_log(
                        f"🚨 [Data Stale] 15min 官方数据过期! 延迟: {time_diff/60:.1f}min。共振判定将失效。"
                    )
                self.is_15m_trending_up = False
                self.is_15m_trending_down = False
            else:
                # 1. 计算均线序列
                self.ma20_15m_series = (
                    self.kline_cache_15m["close"].rolling(window=20).mean()
                )

                # 2. 提取归一化数值 (统一使用 ma20_15m_ 前缀)
                self.ma20_15m_val = self.ma20_15m_series.iloc[-1]  # 当前 15min MA20
                self.ma20_15m_prev = self.ma20_15m_series.iloc[-2]  # 前一根 15min MA20

                # 3. 状态判定 (使用归一化后的变量进行比对)
                # 增强版：MA20 斜率 + 价格位置
                last_price = self.kline_cache_15m["close"].iloc[-1]
                self.is_15m_trending_up = (
                    self.ma20_15m_val > self.ma20_15m_prev  # MA20 上升
                    and last_price > self.ma20_15m_val  # 价格在 MA20 之上
                )
                self.is_15m_trending_down = (
                    self.ma20_15m_val < self.ma20_15m_prev  # MA20 下跌
                    and last_price < self.ma20_15m_val  # 价格在 MA20 之下
                )

                # ✨ 增加 Debug 日志，便于实时监控大周期态势
                trend_str = (
                    "UP 📈"
                    if self.is_15m_trending_up
                    else ("DOWN 📉" if self.is_15m_trending_down else "SIDE")
                )
                self.sys_log(
                    f"ℹ️ 15分钟K线MA20趋势判断完成: {trend_str} (MA20: {self.ma20_15m_val:.3f})",
                    level="DEBUG",
                )

    def process_new_2min_bar(self, bar):
        """[中央处理器] 无论合成还是补票，每当得到一根新的2min K线之后，都调用这个函数"""
        # 🛡️ 物理防重放机制：如果传入的 K 线时间并不比现有的新，则直接无视
        if not self.history_2min_bars.empty:
            if bar["datetime"] <= self.history_2min_bars["datetime"].max():
                # 这种情况通常是重复合成或补票冲突，必须拦截，否则计数器会炸
                return
        # 1. 归档与排序
        new_row = pd.DataFrame([bar])
        self.history_2min_bars = pd.concat(
            [self.history_2min_bars, new_row], ignore_index=True
        )
        self.history_2min_bars.drop_duplicates(
            subset=["datetime"], keep="last", inplace=True
        )
        self.history_2min_bars.sort_values("datetime", inplace=True)

        if len(self.history_2min_bars) >= 2:
            last_two = self.history_2min_bars.iloc[-2:]
            self.low_of_4min = last_two["low"].min()
            self.high_of_4min = last_two["high"].max()
        else:
            # 只有一根K线时，暂时用当前K线的值
            self.low_of_4min = bar["low"]
            self.high_of_4min = bar["high"]

        # 2. 跨线探测 (这是您最关心的 15min 对齐)
        current_bar_close_time = bar["datetime"] + timedelta(minutes=2)
        new_minute = current_bar_close_time.astimezone(EASTERN_TZ).minute
        is_cross_15 = (self.last_bar_close_minute is not None) and (
            self.last_bar_close_minute // 15 != new_minute // 15
        )

        self.last_bar_close_minute = new_minute  # 更新锚点

        if is_cross_15:
            # 💡 跨线了！启动异步链条：同步大周期 -> 重新算指标 -> 重新审信号
            async def sync_and_recalc(snapshot_bar=bar):
                await load_vix_data_async(is_init=False)
                success_k = await self.update_15m_cache(is_init=False)
                if success_k:
                    self.calculate_indicators()  # 数据拿到了再算，没拿到不瞎算
                    self._detect_market_patterns(snapshot_bar)
                    self.active_signal = self.analyze_signals(
                        snapshot_bar["close"], shared.global_last_vix_close
                    )
                    self.sys_log(
                        f"🔄 [跨15分钟K线同步] 15min K线数据刷新，已注入决策流"
                    )

            asyncio.create_task(sync_and_recalc(bar))
        else:
            # 💡 常规时刻：直接计算即可
            self.calculate_indicators()
            self._detect_market_patterns(bar)
            self.active_signal = self.analyze_signals(
                bar["close"], shared.global_last_vix_close
            )

    async def async_patch_ticket(self, st_time, ed_time):
        """[类方法] 异步补票并触发审计与对账"""
        try:
            # 1. 物理买票 (注意 ctx 变 self)
            patch_bars = await self.ib.reqHistoricalDataAsync(
                self.contract,
                endDateTime=ed_time.strftime("%Y%m%d %H:%M:%S"),
                durationStr="600 S",
                barSizeSetting="2 mins",
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1,
            )

            if patch_bars:
                target = next((b for b in patch_bars if b.date == st_time), None)
                if target:
                    new_bar = {
                        "datetime": st_time,
                        "open": target.open,
                        "high": target.high,
                        "low": target.low,
                        "close": target.close,
                        "volume": target.volume,
                    }

                    self.sys_log(
                        f"✅ [补票成功] 读取 {st_time.strftime('%H:%M')} 2分钟K线操作完成",
                        level="INFO",
                    )

                    # 2. 调用中央处理器
                    self.process_new_2min_bar(new_bar)

                    # 3. 执行物理对账
                    await self.sync_initial_state()
                    self.sys_log(f"[2分钟K线] 补票完成并已对账", level="INFO")

        except Exception as e:
            self.sys_log(f"🚨 [异步补票失败]，错误代码: {e}", level="ERROR")

    def calculate_indicators(self):
        """
        [第一次加载历史数据之后，以及每次合成了一根2分钟K线之后，都会调用本函数，计算各种指标MA8,MA20,MA200等]
        职责：
        1. 基于 history_2min_bars (仓库) 实现纯净计算。
        2. 严格保留 SuperTrend、MA20 转折日志及三级 ATR 融合逻辑。
        """
        # --- 1. 安全审计：确保仓库面包已就位 ---
        if self.history_2min_bars is None or self.history_2min_bars.empty:
            return

        last_bar = self.history_2min_bars.iloc[-1]
        current_bar_time = last_bar["datetime"]

        # 🛡️ 时间戳哨兵：防止同一根 K 线被热机逻辑和实时逻辑重复计算
        if (
            hasattr(self, "last_processed_time")
            and self.last_processed_time == current_bar_time
        ):
            return
        # 必须至少有 20 根 K 线才能开始计算 MA20
        data_len = len(self.history_2min_bars)
        if data_len < 20:
            return

        df = self.history_2min_bars

        try:
            # --- 2. 均线分级计算 (对齐 07-18 逻辑) ---
            ma20_series = df["close"].rolling(window=20).mean()
            ma8_series = df["close"].rolling(window=8).mean()

            self.ma20 = ma20_series.iloc[-1]
            self.ma8 = ma8_series.iloc[-1]
            self.ma20_prev = ma20_series.iloc[-2]
            self.ma20_prev2 = ma20_series.iloc[-3]

            # --- 3. MA200 与超级趋势判定 (SuperTrend) ---
            if data_len >= 200:
                self.ma200 = df["close"].rolling(window=200).mean().iloc[-1]
                curr_price = df["close"].iloc[-1]
                # A. 基础微观判定 (2min 三线顺排)
                micro_uptrend = (
                    (curr_price > self.ma8)
                    and (self.ma8 > self.ma20)
                    and (self.ma20 > self.ma200)
                )
                micro_downtrend = (
                    (curr_price < self.ma8)
                    and (self.ma8 < self.ma20)
                    and (self.ma20 < self.ma200)
                )

                # B. 引入战略过滤器 (15min 趋势同步)
                # 获取 15min 标志位，如果没有数据（None/False）则默认为不通过（保守策略）
                macro_up_confirm = getattr(self, "is_15m_trending_up", False)
                macro_down_confirm = getattr(self, "is_15m_trending_down", False)

                # C. 最终共振判定
                self.is_super_uptrend = micro_uptrend and macro_up_confirm
                self.is_super_downtrend = micro_downtrend and macro_down_confirm

                # ✨ 可选：增加审计日志，便于观察为何 SuperTrend 没激活
                if micro_uptrend and not macro_up_confirm:
                    self.sys_log(
                        f"⚠️ [趋势背离] 2min已排布，但15min未转向向上，SuperUptrend拦截",
                        level="DEBUG",
                    )
            else:
                self.ma200 = None
                self.is_super_uptrend = False
                self.is_super_downtrend = False

            # --- 4. MA20 方向转折实证日志 ---
            self.is_ma20_turning_up = (self.ma20 > self.ma20_prev) and (
                self.ma20_prev >= self.ma20_prev2
            )
            if self.is_ma20_turning_up:
                self.sys_log(
                    f"🔼 MA20 正在向上 | 当前: {self.ma20:.3f} | 前一: {self.ma20_prev:.3f} | 前二: {self.ma20_prev2:.3f}",
                    level="INFO",
                )

            self.is_ma20_turning_down = (self.ma20 < self.ma20_prev) and (
                self.ma20_prev <= self.ma20_prev2
            )
            if self.is_ma20_turning_down:
                self.sys_log(
                    f"🔽 MA20 正在向下 | 当前: {self.ma20:.3f} | 前一: {self.ma20_prev:.3f} | 前二: {self.ma20_prev2:.3f}",
                    level="INFO",
                )

            # --- 5. [重构版] 三级 ATR 融合算法 (防止大象柱污染) ---
            base_atr = getattr(self, "static_atr", 0.5)  # 昨日基准
            dynamic_atr = (
                (df["high"] - df["low"]).rolling(window=14).mean().iloc[-1]
            )  # 瞬时体温

            now_time = datetime.now(EASTERN_TZ).time()

            if now_time < time(10, 0):
                # 🛡️ 最佳实践：在 10:00 之前，限制动态 ATR 的“破坏力”
                # 强制动态值不能超过基准值的 1.5 倍，防止大象柱瞬间拉大止损空间
                capped_dynamic = min(dynamic_atr, base_atr * 1.5)
                # 使用 7:3 比例混合，基准占大头
                self.effective_atr = round((base_atr * 0.7) + (capped_dynamic * 0.3), 4)
                self.sys_log(
                    f"🧬 [ATR平滑] 早盘防御模式: 物理上限拦截生效", level="DEBUG"
                )
            elif now_time < time(10, 30):
                # 过渡期：逐步放开限制，改为 5:5 比例
                self.effective_atr = round((base_atr * 0.5) + (dynamic_atr * 0.5), 4)
            else:
                # 10:30 之后：市场已定型，完全采用实时动态 ATR，并设置 0.20 物理下限
                self.effective_atr = max(round(dynamic_atr, 4), 0.20)

            self.atr = self.effective_atr

            # --- 6. 大象柱辅助数据 (对齐原有 06-9 逻辑) ---
            # 提取倒数第21到第2根（不含当前根）用于计算平均波动
            prev_20 = df.iloc[-21:-1]
            self.avg_range_20 = (prev_20["high"] - prev_20["low"]).mean()
            self.avg_volume = prev_20["volume"].mean()

            # --- 7. 实证日志 ---
            self.sys_log(
                f"📊 前14根K线的动态ATR=:{dynamic_atr:.3f} |前一交易日195根K线的静态ATR=:{base_atr:.3f} | 最终采用值:{self.effective_atr}",
                level="DEBUG",
            )

            # --- 8. 计算连续红绿柱计数 ---
            if not self.history_2min_bars.empty:
                last_bar = self.history_2min_bars.iloc[-1]
                if last_bar["close"] > last_bar["open"]:  # 🟢 变绿
                    if self.red_bar_count > 0:  # 瞬时转折：由红转绿第一根
                        self.last_red_count = self.red_bar_count  # 备份压抑红柱数
                        self.green_bar_count = 1  # ✨ 恢复：确认为第一根反转
                        self.high_of_bounce = last_bar["high"]  # 初始化上涨极值
                        self.low_of_dip = 0.0  # 结束下跌，重置锚点
                        self.sys_log(
                            f"📊 红绿柱记数器|🔴连续下跌:{self.last_red_count}根红柱之后，出现第1根上涨🟢绿柱"
                        )
                    else:  # 持续上涨
                        self.last_red_count = 0
                        self.green_bar_count += 1
                        # ✨ 动态维护：确保记录波段最高点
                        self.high_of_bounce = (
                            max(self.high_of_bounce, last_bar["high"])
                            if self.high_of_bounce > 0
                            else last_bar["high"]
                        )

                    self.red_bar_count = 0

                elif last_bar["close"] < last_bar["open"]:  # 🔴 变红
                    if self.green_bar_count > 0:  # 瞬时转折：由绿转红第一根
                        self.last_green_count = self.green_bar_count  # 备份上涨绿柱数
                        self.red_bar_count = 1  # ✨ 对称恢复：确认为第一根反转
                        self.low_of_dip = last_bar["low"]  # 初始化下跌极值
                        self.high_of_bounce = 0.0  # 结束上涨，重置锚点
                        self.sys_log(
                            f"📊 红绿柱记数器|🟢连续上涨:{self.last_green_count}根绿柱之后，出现第1根下跌🔴红柱"
                        )
                    else:  # 持续下跌
                        self.last_green_count = 0
                        self.red_bar_count += 1
                        # ✨ 动态维护：确保记录波段最低点
                        self.low_of_dip = (
                            min(self.low_of_dip, last_bar["low"])
                            if self.low_of_dip > 0
                            else last_bar["low"]
                        )

                    self.green_bar_count = 0

                else:
                    # 平盘（十字星）：通常 Velez 建议维持现状或计数停滞
                    self.sys_log(
                        f"📊 红绿柱记数器|这是一根十字星K线，之前有:🔴连续下跌红柱:{self.red_bar_count}根 | 🟢连续上涨绿柱:{self.green_bar_count}根"
                    )
                    pass
                self.last_processed_time = current_bar_time

        except Exception as e:
            self.sys_log(f"🚨 [指标计算异常] {self.symbol}: {e}", level="ERROR")

    def _detect_tail_bars(self, new_bar):
        """
        [信号层-辅助工具] 影线形态识别器 (BT-Bottom Tail/TT-Top Tail )
        职责：分析单根K线的物理形态，识别纯净的底部拒绝(BT)或顶部压力(TT)。

        标准：
        1. 比例：主影线 >= 2 * 实体 且 主影线 >= 60% 全长。
        2. 纯净度：主影线必须是反向影线的 3 倍以上 (排除震荡十字星)。
        3. 强度：主影线绝对长度 > 0.3 * effective_atr (排除微观噪音)。
        """
        # 1. 初始重置状态 (保持即时性)
        self.current_tail_type = None
        # 2. 物理审计
        if new_bar is None:
            return
        if not hasattr(self, "effective_atr"):
            return

        high, low = new_bar["high"], new_bar["low"]
        open_p, close_p = new_bar["open"], new_bar["close"]

        total_range = high - low
        body_size = abs(close_p - open_p)

        # 极端情况保护
        if total_range <= 0.01 or body_size <= 0.01:
            return

        # 3. 影线分项计算
        lower_shadow = min(open_p, close_p) - low
        upper_shadow = high - max(open_p, close_p)

        # 门槛参数
        SHADOW_TO_BODY_RATIO = 2.0
        SHADOW_TO_TOTAL_RATIO = 0.6
        ABS_STRENGTH_LIMIT = self.effective_atr * 0.3
        PURITY_RATIO = 3.0

        # 4. 判定逻辑

        # --- A. 底部影线 BT (Bottoming Tail) 判定 ---
        # 必须满足：比例够 + 绝对长度够 + 纯净度够 (下影远大于上影)
        if (
            lower_shadow >= (body_size * SHADOW_TO_BODY_RATIO)
            and lower_shadow >= (total_range * SHADOW_TO_TOTAL_RATIO)
            and lower_shadow > ABS_STRENGTH_LIMIT
        ):

            # 纯净度审计：下影线必须是上影线的 3 倍以上
            if lower_shadow >= (max(upper_shadow, 0.001) * PURITY_RATIO):
                self.current_tail_type = "BT"
                shadow_pct = (lower_shadow / total_range) * 100
                self.sys_log(
                    f"🕵️ [Tail-BT] 发现下影反转 | 占比:{shadow_pct:.1f}% | 纯度确认",
                    level="DEBUG",
                )
                return

        # --- B. 顶部影线 TT (Topping Tail) 判定 ---
        if (
            upper_shadow >= (body_size * SHADOW_TO_BODY_RATIO)
            and upper_shadow >= (total_range * SHADOW_TO_TOTAL_RATIO)
            and upper_shadow > ABS_STRENGTH_LIMIT
        ):

            # 纯净度审计：上影线必须是下影线的 3 倍以上
            if upper_shadow >= (max(lower_shadow, 0.001) * PURITY_RATIO):
                self.current_tail_type = "TT"
                shadow_pct = (upper_shadow / total_range) * 100
                self.sys_log(
                    f"🕵️ [Tail-TT] 发现上影压力 | 占比:{shadow_pct:.1f}% | 纯度确认",
                    level="DEBUG",
                )
                return

    def _detect_law1(self, new_bar):
        """
        [Law #1 - Velez Orthodox Version]
        职责：
            识别最贴近 Oliver Velez 原教旨定义的 Elephant Bar / Clearing Elephant Bar。

        核心思想：
        1. 不再使用 ATR 作为 Elephant 主判定
        2. 使用“左侧局部相对显著性”替代 ATR 倍数
        3. 引入 Clearing / 20MA附近 / Breakout / Trend Restart
        4. 成交量仅作为加分项，不再是硬门槛
        """

        # =========================================================
        # 0) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law1 = False
        self.ready_to_short_law1 = False

        self.law1_sl = None
        self.law1_trigger_high = None
        self.law1_trigger_low = None
        self.law1_entry_type = None
        self.law1_elephant_high = None
        self.law1_elephant_low = None
        self.law1_elephant_open = None
        self.law1_elephant_close = None

        # 审计辅助字段
        self.law1_score = 0
        self.law1_soft_clear = False
        self.law1_hard_clear = False
        self.law1_context = None

        # =========================================================
        # 1) 物理审计：确保基础数据已就绪
        # =========================================================
        if self.history_2min_bars is None or self.history_2min_bars.empty:
            return

        if len(self.history_2min_bars) < 25:
            return

        if (
            not hasattr(self, "avg_volume")
            or self.avg_volume is None
            or self.avg_volume <= 0
        ):
            return

        if (
            not hasattr(self, "ma20")
            or not hasattr(self, "ma20_prev")
            or not hasattr(self, "ma20_prev2")
        ):
            return

        bars_df = self.history_2min_bars
        left_df = bars_df.iloc[
            :-1
        ].copy()  # 当前 bar 已经 append 到 history_2min_bars，左侧必须排除最后一根

        if len(left_df) < 20:
            return

        # =========================================================
        # 2) 当前 bar 基础几何
        # =========================================================
        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])
        v = float(new_bar["volume"])

        bar_range = h - l
        body_size = abs(c - o)

        if bar_range <= 0 or body_size <= 0:
            return

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_ratio = body_size / bar_range if bar_range > 0 else 0.0
        vol_ratio = v / self.avg_volume if self.avg_volume > 0 else 0.0

        is_bull = c > o
        is_bear = c < o

        if not (is_bull or is_bear):
            return

        # =========================================================
        # 3) 左侧相对显著性（替代 ATR）
        #    compare_n = 7：更贴近“周围 bars”的视觉判断
        # =========================================================
        compare_n = 7
        prev7 = left_df.tail(compare_n).copy()
        if len(prev7) < compare_n:
            return

        prev7_ranges = (prev7["high"] - prev7["low"]).astype(float)
        prev7_bodies = (prev7["close"] - prev7["open"]).abs().astype(float)

        is_body_majority_big = body_size >= prev7_bodies.quantile(0.80)
        is_range_majority_big = bar_range >= prev7_ranges.quantile(0.80)
        is_body_near_local_max = body_size >= 0.90 * prev7_bodies.max()

        is_prominent = (
            is_body_majority_big and is_range_majority_big and is_body_near_local_max
        )

        # =========================================================
        # 4) 实体厚实 + 少影线
        # =========================================================
        body_ok = body_ratio >= 0.70

        # Bull Elephant：上影线要短，下影线可稍放宽
        bull_wick_ok = upper_wick <= 0.15 * body_size and lower_wick <= 0.35 * body_size

        # Bear Elephant：下影线要短，上影线可稍放宽
        bear_wick_ok = lower_wick <= 0.15 * body_size and upper_wick <= 0.35 * body_size

        quality_ok = body_ok and (
            (is_bull and bull_wick_ok) or (is_bear and bear_wick_ok)
        )

        # =========================================================
        # 5) Clearing Elephant Bar
        #    至少清掉左侧连续 3 根反色 bars
        # =========================================================
        max_clearing_scan = 5
        min_clearing_bars = 3

        recent_left = left_df.tail(max_clearing_scan).copy()
        opposite_rows = []

        for i in range(len(recent_left) - 1, -1, -1):
            row = recent_left.iloc[i]
            row_open = float(row["open"])
            row_close = float(row["close"])

            row_is_green = row_close > row_open
            row_is_red = row_close < row_open

            if is_bull:
                if row_is_red:
                    opposite_rows.append(row)
                else:
                    break
            else:
                if row_is_green:
                    opposite_rows.append(row)
                else:
                    break

        opposite_count = len(opposite_rows)
        soft_clear = False
        hard_clear = False

        if opposite_count >= min_clearing_bars:
            opp_df = pd.DataFrame(opposite_rows)

            opp_high_max = float(opp_df["high"].max())
            opp_low_min = float(opp_df["low"].min())
            opp_body_top_max = float(opp_df[["open", "close"]].max(axis=1).max())
            opp_body_bottom_min = float(opp_df[["open", "close"]].min(axis=1).min())

            if is_bull:
                # Soft clear：收盘越过反色实体区，且高点刺穿对方高点
                soft_clear = (c > opp_body_top_max) and (h > opp_high_max)
                # Hard clear：收盘直接越过反色bars最高点
                hard_clear = c > opp_high_max
            else:
                # Soft clear：收盘跌破反色实体区，且低点刺穿对方低点
                soft_clear = (c < opp_body_bottom_min) and (l < opp_low_min)
                # Hard clear：收盘直接跌破反色bars最低点
                hard_clear = c < opp_low_min

        # =========================================================
        # 5.1 Clearing 记忆交叉校验
        # opposite_count = 当前回扫事实
        # last_red_count / last_green_count = 状态机记忆事实
        # =========================================================
        last_opposite_count = 0
        if is_bull:
            last_opposite_count = getattr(self, "last_red_count", 0)
        else:
            last_opposite_count = getattr(self, "last_green_count", 0)

        clearing_memory_confirmed = last_opposite_count >= min_clearing_bars
        clear_consistency_ok = (
            opposite_count >= min_clearing_bars and clearing_memory_confirmed
        )

        # =========================================================
        # 6) 位置语境
        # =========================================================
        ma20 = float(self.ma20)
        ma20_prev = float(self.ma20_prev)
        ma20_prev2 = float(self.ma20_prev2)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # ---------------------------------------------------------
        # 6.1 20MA附近
        # 不再用 ATR，用当前 bar 自身 range 归一化
        # ---------------------------------------------------------
        if is_bull:
            is_near_ma20 = (
                (l <= ma20 <= h)
                or (abs(c - ma20) <= 0.35 * bar_range)
                or (abs(l - ma20) <= 0.25 * bar_range)
            )
        else:
            is_near_ma20 = (
                (l <= ma20 <= h)
                or (abs(c - ma20) <= 0.35 * bar_range)
                or (abs(h - ma20) <= 0.25 * bar_range)
            )

        # ---------------------------------------------------------
        # 6.2 局部突破点
        # ---------------------------------------------------------
        breakout_lookback = 5
        prev5 = left_df.tail(breakout_lookback).copy()
        if len(prev5) < breakout_lookback:
            return

        prev5_high_max = float(prev5["high"].max())
        prev5_low_min = float(prev5["low"].min())
        prev5_body_top_max = float(prev5[["open", "close"]].max(axis=1).max())
        prev5_body_bottom_min = float(prev5[["open", "close"]].min(axis=1).min())

        if is_bull:
            is_breakout_point = (h > prev5_high_max) and (c > prev5_body_top_max)
        else:
            is_breakout_point = (l < prev5_low_min) and (c < prev5_body_bottom_min)

        # ---------------------------------------------------------
        # 6.3 趋势重启（Trend Restart）
        # 直接利用 calculate_indicators() 的结果
        # ---------------------------------------------------------
        restart_lookback = 4
        prev4 = left_df.tail(restart_lookback).copy()
        if len(prev4) < restart_lookback:
            return

        # 最近 4 根里是否有 pullback 接近 20MA
        touched_ma20 = False
        for _, row in prev4.iterrows():
            rh = float(row["high"])
            rl = float(row["low"])
            rr = max(rh - rl, 1e-8)

            if is_bull:
                if (rl <= ma20 <= rh) or (abs(rl - ma20) <= 0.25 * rr):
                    touched_ma20 = True
                    break
            else:
                if (rl <= ma20 <= rh) or (abs(rh - ma20) <= 0.25 * rr):
                    touched_ma20 = True
                    break

        # 趋势条件
        bull_trend_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        )
        bear_trend_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        )

        if is_bull:
            is_restart = (
                bull_trend_ok and touched_ma20 and (c > float(prev4["high"].max()))
            )
        else:
            is_restart = (
                bear_trend_ok and touched_ma20 and (c < float(prev4["low"].min()))
            )

        # =========================================================
        # 7) 评分制：至少 2 分
        # =========================================================
        score = 0
        if soft_clear:
            score += 1
        if hard_clear:
            score += 1
        if is_near_ma20:
            score += 1
        if is_breakout_point:
            score += 1
        if is_restart:
            score += 1
        if vol_ratio >= 1.20:
            score += 1
        if clearing_memory_confirmed:
            score += 1
        if clear_consistency_ok:
            score += 1

        self.law1_score = score
        self.law1_soft_clear = soft_clear
        self.law1_hard_clear = hard_clear

        if is_restart:
            self.law1_context = "TrendRestart"
        elif is_near_ma20:
            self.law1_context = "Near20MA"
        elif is_breakout_point:
            self.law1_context = "BreakoutPoint"
        else:
            self.law1_context = "Generic"

        # =========================================================
        # 8) 最终触发：硬门槛 + 评分门槛
        # =========================================================
        hard_gates_pass = is_prominent and quality_ok

        if not hard_gates_pass:
            return

        if score < 2:
            return

        # =========================================================
        # 9) 生成 Law1 信号
        # 止损：象柱另一端 + 当前 bar range 的 10% buffer
        # =========================================================
        buffer = 0.10 * bar_range

        self.law1_elephant_high = h
        self.law1_elephant_low = l
        self.law1_elephant_open = o
        self.law1_elephant_close = c

        if is_bull:
            self.ready_to_long_law1 = True
            self.law1_sl = l - buffer
            self.law1_trigger_high = self.law1_elephant_high
            self.law1_trigger_low = self.law1_elephant_low
            self.law1_entry_type = "Breakout"
            self.sys_log(
                f"🐘 [Law#1-Velez]🟢Bull Elephant"
                f"|score:{score}"
                f"|量比:{vol_ratio:.2f}"
                f"|实体占比:{body_ratio:.2f}"
                f"|opp_count:{opposite_count}"
                f"|last_opp:{last_opposite_count}"
                f"|mem_ok:{clearing_memory_confirmed}"
                f"|clear_ok:{clear_consistency_ok}"
                f"|soft_clear:{soft_clear}"
                f"|hard_clear:{hard_clear}"
                f"|ctx:{self.law1_context}"
                f"|SL:{self.law1_sl:.2f}",
                level="INFO",
            )

        elif is_bear:
            self.ready_to_short_law1 = True
            self.law1_sl = h + buffer
            self.law1_trigger_high = self.law1_elephant_high
            self.law1_trigger_low = self.law1_elephant_low
            self.law1_entry_type = "Breakout"
            self.sys_log(
                f"🐘 [Law#1-Velez]🔴Bear Elephant"
                f"|score:{score}"
                f"|量比:{vol_ratio:.2f}"
                f"|实体占比:{body_ratio:.2f}"
                f"|opp_count:{opposite_count}"
                f"|last_opp:{last_opposite_count}"
                f"|mem_ok:{clearing_memory_confirmed}"
                f"|clear_ok:{clear_consistency_ok}"
                f"|soft_clear:{soft_clear}"
                f"|hard_clear:{hard_clear}"
                f"|ctx:{self.law1_context}"
                f"|SL:{self.law1_sl:.2f}",
                level="INFO",
            )

    def _detect_law2(self, new_bar):
        """
        [Law #2] Color Change (Velez Orthodox Version)
        职责：
            识别在趋势语境中，连续 3~5 根同色压抑后的第一根高质量反色 K 线。

        设计原则：
        1. 保留 Velez 核心：3~5 根同色压抑 + 第一根反色柱
        2. 去掉 ATR 实体门槛，改为反色柱自身形态质量审计
        3. 引入趋势语境：顺 MA20/MA200 大方向
        4. 引入位置语境：优先要求发生在 20MA 附近
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law2 = False
        self.ready_to_short_law2 = False
        self.law2_sl = None
        self.law2_reversal_high = None
        self.law2_reversal_low = None
        self.law2_trigger_high = None
        self.law2_trigger_low = None
        self.law2_entry_type = None

        # =========================================================
        # 2) 物理审计：确保计算基础已就绪
        # =========================================================
        if not hasattr(self, "ma20") or self.ma20 is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return

        # =========================================================
        # 3) 基础数值提取
        # =========================================================
        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])

        is_green = c > o
        is_red = c < o

        bar_range = h - l
        body_size = abs(c - o)

        if bar_range <= 0 or body_size <= 0:
            return

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_ratio = body_size / bar_range if bar_range > 0 else 0.0

        ma20 = float(self.ma20)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # =========================================================
        # 4) Law2 反色柱自身质量判定（替代 ATR 过滤）
        #    Bull: 上影线不要太长
        #    Bear: 下影线不要太长
        # =========================================================
        bull_reversal_quality_ok = body_ratio >= 0.55 and upper_wick <= 0.35 * body_size

        bear_reversal_quality_ok = body_ratio >= 0.55 and lower_wick <= 0.35 * body_size

        # =========================================================
        # 5) 位置语境：当前反色柱是否发生在 20MA 附近
        #    不再用 ATR，改用当前 bar 自身 range 归一化
        # =========================================================
        bull_near_ma20 = (
            (l <= ma20 <= h)
            or (abs(l - ma20) <= 0.25 * bar_range)
            or (abs(c - ma20) <= 0.35 * bar_range)
        )

        bear_near_ma20 = (
            (l <= ma20 <= h)
            or (abs(h - ma20) <= 0.25 * bar_range)
            or (abs(c - ma20) <= 0.35 * bar_range)
        )

        # =========================================================
        # 6) 趋势语境：Law2 更贴近 Velez 的核心
        #    Bull: 上升趋势中的 3~5 红后第一根绿柱
        #    Bear: 下降趋势中的 3~5 绿后第一根红柱
        # =========================================================
        bull_trend_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        )

        bear_trend_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        )

        # =========================================================
        # 7) 多头判定：3~5 红后第一根绿柱
        # =========================================================
        if is_green:
            if 3 <= self.last_red_count <= 5 and self.green_bar_count == 1:
                if bull_reversal_quality_ok and bull_trend_ok and bull_near_ma20:
                    self.ready_to_long_law2 = True

                    # Law2 结构止损：反色柱低点
                    self.law2_sl = l

                    # 记录关键价位，供 plan_trade 使用
                    self.law2_reversal_high = h
                    self.law2_reversal_low = l
                    self.law2_trigger_high = self.law2_reversal_high
                    self.law2_trigger_low = self.law2_reversal_low
                    self.law2_entry_type = "Breakout"
                    self.sys_log(
                        f"🌈 [Law#2-Velez]🟢Bull Color Change"
                        f"|压抑红柱:{self.last_red_count}根"
                        f"|实体占比:{body_ratio:.2f}"
                        f"|near20:{bull_near_ma20}"
                        f"|trend_ok:{bull_trend_ok}"
                        f"|SL:{self.law2_sl:.2f}",
                        level="INFO",
                    )

            elif self.last_red_count == 2:
                pass
                # 可选审计日志：
                # self.sys_log(
                #     f"👀 [Law#2观察] 仅2根红柱后的变色，不符合 Velez 3~5 根压抑原则，跳过。",
                #     level="DEBUG",
                # )

        # =========================================================
        # 8) 空头判定：3~5 绿后第一根红柱
        # =========================================================
        elif is_red:
            if 3 <= self.last_green_count <= 5 and self.red_bar_count == 1:
                if bear_reversal_quality_ok and bear_trend_ok and bear_near_ma20:
                    self.ready_to_short_law2 = True

                    # Law2 结构止损：反色柱高点
                    self.law2_sl = h

                    # 记录关键价位，供 plan_trade 使用
                    self.law2_reversal_high = h
                    self.law2_reversal_low = l
                    self.law2_trigger_high = self.law2_reversal_high
                    self.law2_trigger_low = self.law2_reversal_low
                    self.law2_entry_type = "Breakout"
                    self.sys_log(
                        f"🌈 [Law#2-Velez]🔴Bear Color Change"
                        f"|压抑绿柱:{self.last_green_count}根"
                        f"|实体占比:{body_ratio:.2f}"
                        f"|near20:{bear_near_ma20}"
                        f"|trend_ok:{bear_trend_ok}"
                        f"|SL:{self.law2_sl:.2f}",
                        level="INFO",
                    )

            elif self.last_green_count == 2:
                pass
                # 可选审计日志：
                # self.sys_log(
                #     f"👀 [Law#2观察] 仅2根绿柱后的变色，不符合 Velez 3~5 根压抑原则，跳过。",
                #     level="DEBUG",
                # )

    def _detect_law3(self, new_bar):
        """
        [Law #3] 3-5 Bars (Velez Orthodox Version)
        职责：
            识别在明确趋势中，价格对 20MA 进行 3~5 根 bars 的正常回调/反弹后，
            第一根恢复趋势控制权的反色 K 线。

        设计原则：
        1. 保留 Velez 核心：3~5 根同色回调/反弹 + 第一根反色柱
        2. 补上大趋势语境：顺 MA20 / MA200 大方向
        3. 将“靠近 20MA”从 ATR 距离改成 bar 自身几何关系
        4. 保留“越前高/前低”作为工程化触发器
        5. 保留 low_of_dip / high_of_bounce 作为结构止损
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law3 = False
        self.ready_to_short_law3 = False

        # 建议一并清空 Law3 关键字段，避免旧值残留
        self.law3_sl = None
        self.law3_trigger_high = None
        self.law3_trigger_low = None
        self.law3_entry_type = None

        # =========================================================
        # 2) 物理审计：确保基础数据已就绪
        # =========================================================
        if self.ma20 is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return
        if self.history_2min_bars is None or len(self.history_2min_bars) < 2:
            return

        prev_bar = self.history_2min_bars.iloc[-2]

        # =========================================================
        # 3) 当前 bar 基础数值
        # =========================================================
        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])

        prev_h = float(prev_bar["high"])
        prev_l = float(prev_bar["low"])

        is_green = c > o
        is_red = c < o

        bar_range = h - l
        body_size = abs(c - o)

        if bar_range <= 0 or body_size <= 0:
            return

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_ratio = body_size / bar_range if bar_range > 0 else 0.0

        ma20 = float(self.ma20)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # =========================================================
        # 4) 大趋势语境：更贴近 Velez 原教旨
        # =========================================================
        bull_trend_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        )

        bear_trend_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        )

        # =========================================================
        # 5) 位置语境：当前 bar 是否发生在 20MA 附近
        #    不再使用 ATR 距离，改用当前 bar 自身几何关系
        # =========================================================
        bull_near_ma20 = (
            (l <= ma20 <= h)
            or (abs(l - ma20) <= 0.25 * bar_range)
            or (abs(c - ma20) <= 0.35 * bar_range)
        )

        bear_near_ma20 = (
            (l <= ma20 <= h)
            or (abs(h - ma20) <= 0.25 * bar_range)
            or (abs(c - ma20) <= 0.35 * bar_range)
        )

        # =========================================================
        # 6) 反色柱自身质量审计
        #    Law3 不需要像 Law1 那么强，但至少要避免很差的反色柱
        # =========================================================
        bull_reversal_quality_ok = body_ratio >= 0.50 and upper_wick <= 0.50 * body_size

        bear_reversal_quality_ok = body_ratio >= 0.50 and lower_wick <= 0.50 * body_size

        # =========================================================
        # 7) Bull Law3：3~5 红后第一根绿柱
        # =========================================================
        if is_green and self.green_bar_count == 1 and 3 <= self.last_red_count <= 5:
            # 工程触发器：当前 bar 恢复趋势控制权，越过前一根高点
            bull_trigger_ok = h > prev_h

            if (
                bull_reversal_quality_ok
                and bull_trend_ok
                and bull_near_ma20
                and bull_trigger_ok
            ):
                self.ready_to_long_law3 = True

                # Law3 结构止损：使用回调波段最低点
                self.law3_sl = self.low_of_dip
                self.law3_trigger_high = h
                self.law3_trigger_low = l
                self.law3_entry_type = "Breakout"

                self.sys_log(
                    f"🎯 [Law#3-Velez] 🟢Bull Pullback"
                    f"|确认回调:{self.last_red_count}根红柱"
                    f"|实体占比:{body_ratio:.2f}"
                    f"|near20:{bull_near_ma20}"
                    f"|trend_ok:{bull_trend_ok}"
                    f"|trigger:{bull_trigger_ok}"
                    f"|当前高点:{h:.3f}>{prev_h:.3f}"
                    f"|SL:{self.law3_sl:.2f}",
                    level="INFO",
                )

        # =========================================================
        # 8) Bear Law3：3~5 绿后第一根红柱
        # =========================================================
        elif is_red and self.red_bar_count == 1 and 3 <= self.last_green_count <= 5:
            # 工程触发器：当前 bar 恢复空头控制权，跌破前一根低点
            bear_trigger_ok = l < prev_l

            if (
                bear_reversal_quality_ok
                and bear_trend_ok
                and bear_near_ma20
                and bear_trigger_ok
            ):
                self.ready_to_short_law3 = True

                # Law3 结构止损：使用反弹波段最高点
                self.law3_sl = self.high_of_bounce
                self.law3_trigger_high = h
                self.law3_trigger_low = l
                self.law3_entry_type = "Breakout"
                self.sys_log(
                    f"🎯 [Law#3-Velez] 🔴Bear Rally"
                    f"|确认反弹:{self.last_green_count}根绿柱"
                    f"|实体占比:{body_ratio:.2f}"
                    f"|near20:{bear_near_ma20}"
                    f"|trend_ok:{bear_trend_ok}"
                    f"|trigger:{bear_trigger_ok}"
                    f"|当前低点:{l:.3f}<{prev_l:.3f}"
                    f"|SL:{self.law3_sl:.2f}",
                    level="INFO",
                )

    def _detect_law4(self, new_bar):
        """
        [Law #4] RBI / GBI (Velez Orthodox Version)

        职责：
            在明确趋势中，识别 1~2 根很浅的反向 bars 被主趋势“忽略”后，
            第一根恢复趋势控制权的延续信号。

        设计原则：
        1. 保留 RBI / GBI 原始骨架：1~2 根反向柱 + 第一根恢复趋势方向的 bar
        2. 补上大趋势语境：顺 MA20 / MA200 大方向
        3. 增加“ignored bars 很浅”的质量审计
        4. 保留“越前高 / 前低”作为工程化触发器
        5. 保留 low_of_dip / high_of_bounce 作为结构止损
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law4 = False
        self.ready_to_short_law4 = False
        self.law4_sl = None
        self.law4_trigger_high = None
        self.law4_trigger_low = None
        self.law4_entry_type = None

        # =========================================================
        # 2) 物理审计
        # =========================================================
        if self.ma20 is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return
        if not hasattr(self, "history_2min_bars") or self.history_2min_bars is None:
            return
        if len(self.history_2min_bars) < 2:
            return

        prev_bar = self.history_2min_bars.iloc[-2]
        if not (pd.notna(new_bar["high"]) and pd.notna(prev_bar["high"])):
            return

        # =========================================================
        # 3) 当前 bar 基础数值
        # =========================================================
        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])

        prev_o = float(prev_bar["open"])
        prev_h = float(prev_bar["high"])
        prev_l = float(prev_bar["low"])
        prev_c = float(prev_bar["close"])

        is_green = c > o
        is_red = c < o

        bar_range = h - l
        body_size = abs(c - o)

        if bar_range <= 0 or body_size <= 0:
            return

        ma8 = float(self.ma8) if getattr(self, "ma8", None) is not None else None
        ma20 = float(self.ma20)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # =========================================================
        # 4) 大趋势语境：比原版更贴近 Velez
        # =========================================================
        bull_trend_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        )

        bear_trend_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        )

        # =========================================================
        # 5) “ignored bars 很浅”的质量审计
        #    关键思想：1~2 根反向柱必须只是 pause，不应深度破坏趋势
        # =========================================================

        # 5.1 当前前一根 bar 的基本质量（因为最终 trigger 是针对 prev_bar）
        prev_bar_range = prev_h - prev_l
        prev_body_size = abs(prev_c - prev_o)

        if prev_bar_range <= 0:
            return

        prev_body_ratio = prev_body_size / prev_bar_range if prev_bar_range > 0 else 0.0

        # 5.2 Bull RBI:
        #     左侧那 1~2 根红柱应该是很浅的小回撤，不应明显跌穿 20MA 太多
        bull_ignored_shallow_ok = (
            self.low_of_dip > 0
            and (
                # 回撤低点不要明显深破 20MA
                (self.low_of_dip >= ma20 - 0.35 * prev_bar_range)
                # 或者前一根反向柱本身仍然围绕 20MA 附近
                or (prev_l <= ma20 <= prev_h)
                or (abs(prev_l - ma20) <= 0.30 * prev_bar_range)
            )
            # 反向柱自身不要太“重”
            and prev_body_ratio <= 0.65
        )

        # 5.3 Bear GBI:
        #     左侧那 1~2 根绿柱应该是很浅的小反弹，不应明显突破 20MA 太多
        bear_ignored_shallow_ok = (
            self.high_of_bounce > 0
            and (
                (self.high_of_bounce <= ma20 + 0.35 * prev_bar_range)
                or (prev_l <= ma20 <= prev_h)
                or (abs(prev_h - ma20) <= 0.30 * prev_bar_range)
            )
            and prev_body_ratio <= 0.65
        )

        # =========================================================
        # 6) 当前恢复趋势方向 bar 的基本质量
        #    Law4 不需要像 Law1 那样苛刻，但至少当前恢复 bar 不能太弱
        # =========================================================
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_ratio = body_size / bar_range if bar_range > 0 else 0.0

        bull_reclaim_quality_ok = body_ratio >= 0.45 and upper_wick <= 0.60 * body_size

        bear_reclaim_quality_ok = body_ratio >= 0.45 and lower_wick <= 0.60 * body_size

        # =========================================================
        # 7) 多头判定：RBI
        # =========================================================
        if (
            bull_trend_ok
            and is_green
            and 1 <= self.last_red_count <= 2
            and self.green_bar_count == 1
        ):
            # 工程触发器：当前恢复 bar 突破前一根反向 bar 的高点
            ignored_red_bars = self.history_2min_bars.iloc[
                -(self.last_red_count + 1) : -1
            ]
            ignored_red_high = (
                float(ignored_red_bars["high"].max())
                if not ignored_red_bars.empty
                else prev_h
            )
            bull_trigger_ok = h > ignored_red_high

            # 保留你原版短周期过滤思想，但降级为辅助，不作为唯一趋势定义
            bull_short_term_ok = ma8 is not None and c > ma8 and ma8 > ma20

            if (
                bull_trigger_ok
                and bull_ignored_shallow_ok
                and bull_reclaim_quality_ok
                and bull_short_term_ok
            ):
                self.ready_to_long_law4 = True
                self.law4_sl = self.low_of_dip
                self.law4_trigger_high = h
                self.law4_trigger_low = l
                self.law4_entry_type = "Breakout"
                trend_type = (
                    "SuperTrend"
                    if getattr(self, "is_super_uptrend", False)
                    else "NormalTrend"
                )

                self.sys_log(
                    f"⚡ [Law#4-Velez-RBI] 🟢 Red Bar Ignored"
                    f"|趋势:{trend_type}"
                    f"|压抑:{self.last_red_count}根红柱"
                    f"|ignored_ok:{bull_ignored_shallow_ok}"
                    f"|reclaim_ok:{bull_reclaim_quality_ok}"
                    f"|trigger:{bull_trigger_ok}"
                    f"|突破:{h:.3f}>{ignored_red_high:.3f}"
                    f"|SL:{self.law4_sl:.2f}",
                    level="INFO",
                )

        # =========================================================
        # 8) 空头判定：GBI
        # =========================================================
        elif (
            bear_trend_ok
            and is_red
            and 1 <= self.last_green_count <= 2
            and self.red_bar_count == 1
        ):
            # 工程触发器：当前恢复 bar 跌破前一根反向 bar 的低点
            ignored_green_bars = self.history_2min_bars.iloc[
                -(self.last_green_count + 1) : -1
            ]
            ignored_green_low = (
                float(ignored_green_bars["low"].min())
                if not ignored_green_bars.empty
                else prev_l
            )
            bear_trigger_ok = l < ignored_green_low

            # 保留你原版短周期过滤思想，但降级为辅助
            bear_short_term_ok = ma8 is not None and c < ma8 and ma8 < ma20

            if (
                bear_trigger_ok
                and bear_ignored_shallow_ok
                and bear_reclaim_quality_ok
                and bear_short_term_ok
            ):
                self.ready_to_short_law4 = True
                self.law4_sl = self.high_of_bounce
                self.law4_trigger_high = h
                self.law4_trigger_low = l
                self.law4_entry_type = "Breakout"

                trend_type = (
                    "SuperTrend"
                    if getattr(self, "is_super_downtrend", False)
                    else "NormalTrend"
                )

                self.sys_log(
                    f"⚡ [Law#4-Velez-GBI] 🔴 Green Bar Ignored"
                    f"|趋势:{trend_type}"
                    f"|压抑:{self.last_green_count}根绿柱"
                    f"|ignored_ok:{bear_ignored_shallow_ok}"
                    f"|reclaim_ok:{bear_reclaim_quality_ok}"
                    f"|trigger:{bear_trigger_ok}"
                    f"|跌破:{l:.3f}<{ignored_green_low:.3f}"
                    f"|SL:{self.law4_sl:.2f}",
                    level="INFO",
                )

    def _detect_law5(self, new_bar):
        """
        [Law #5] 20MA Cross (Velez Orthodox Version)

        职责：
            识别在明确趋势语境中，价格重新夺回/失守 20MA 的有效穿越信号。

        设计原则：
        1. 保留 Law5 核心：价格从 20MA 一侧物理穿越到另一侧
        2. 补上大趋势语境：顺 MA20 / MA200 大方向
        3. 保留 ATR 噪音过滤，但不再单靠 ATR
        4. 加入当前穿越柱自身质量审计（实体占比 + wick 质量）
        5. 保留 Elephant Power Cross 作为强度标签
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law5 = False
        self.ready_to_short_law5 = False
        self.law5_sl = None
        self.law5_trigger_high = None
        self.law5_trigger_low = None
        self.law5_entry_type = None

        # =========================================================
        # 2) 物理审计
        # =========================================================
        if not hasattr(self, "effective_atr"):
            return
        if self.ma20 is None:
            return
        if self.ma20_prev is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return
        if len(self.history_2min_bars) < 2:
            return

        # =========================================================
        # 3) 基础数据提取
        # =========================================================
        prev_close = float(self.history_2min_bars["close"].iloc[-2])

        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])

        curr_close = c
        body_size = abs(c - o)
        bar_range = h - l

        if bar_range <= 0 or body_size <= 0:
            return

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_ratio = body_size / bar_range if bar_range > 0 else 0.0

        ma20 = float(self.ma20)
        ma20_prev = float(self.ma20_prev)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # =========================================================
        # 4) 门槛定义
        # =========================================================
        noise_threshold = self.effective_atr * 0.5
        elephant_threshold = self.effective_atr * 2.0

        # =========================================================
        # 5) 大趋势语境（更贴近 Velez）
        # =========================================================
        bull_trend_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        )

        bear_trend_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        )

        # =========================================================
        # 6) 当前穿越柱自身质量审计
        #    Bull：上影线不能太重
        #    Bear：下影线不能太重
        # =========================================================
        bull_cross_quality_ok = (
            body_size > noise_threshold
            and body_ratio >= 0.50
            and upper_wick <= 0.50 * body_size
        )

        bear_cross_quality_ok = (
            body_size > noise_threshold
            and body_ratio >= 0.50
            and lower_wick <= 0.50 * body_size
        )

        # =========================================================
        # 7) 核心判定逻辑
        # =========================================================

        # ---------------------------------------------------------
        # 7.1 多头穿越 (Bull Cross)
        # 物理定义：前收盘在 MA20 下方/等于，当前收盘站上 MA20
        # ---------------------------------------------------------
        bull_cross_ok = (prev_close <= ma20_prev) and (curr_close > ma20)

        if bull_cross_ok and bull_trend_ok and bull_cross_quality_ok:
            self.ready_to_long_law5 = True
            self.law5_sl = l  # 固化穿越柱低点
            self.law5_trigger_high = h
            self.law5_trigger_low = l
            self.law5_entry_type = "Breakout"
            if body_size >= elephant_threshold:
                tag = "🔥 [Law#5-Velez] 🟢 Elephant Power 20MA Cross!"
            else:
                tag = "🎯 [Law#5-Velez] 🟢 Standard 20MA Cross"

            self.sys_log(
                f"{tag}"
                f" | 实体:{body_size:.3f}"
                f" | 实体占比:{body_ratio:.2f}"
                f" | MA20:{ma20:.3f}"
                f" | trend_ok:{bull_trend_ok}"
                f" | 穿越点对账:{prev_close:.3f}->{curr_close:.3f}"
                f" | SL:{self.law5_sl:.2f}",
                level="INFO",
            )

        # ---------------------------------------------------------
        # 7.2 空头穿越 (Bear Cross)
        # 物理定义：前收盘在 MA20 上方/等于，当前收盘跌回 MA20 下方
        # ---------------------------------------------------------
        elif (
            (prev_close >= ma20_prev)
            and (curr_close < ma20)
            and bear_trend_ok
            and bear_cross_quality_ok
        ):
            self.ready_to_short_law5 = True
            self.law5_sl = h  # 固化穿越柱高点
            self.law5_trigger_high = h
            self.law5_trigger_low = l
            self.law5_entry_type = "Breakout"

            if body_size >= elephant_threshold:
                tag = "🔥 [Law#5-Velez] 🔴 Elephant Power 20MA Cross!"
            else:
                tag = "🎯 [Law#5-Velez] 🔴 Standard 20MA Cross"

            self.sys_log(
                f"{tag}"
                f" | 实体:{body_size:.3f}"
                f" | 实体占比:{body_ratio:.2f}"
                f" | MA20:{ma20:.3f}"
                f" | trend_ok:{bear_trend_ok}"
                f" | 穿越点对账:{prev_close:.3f}->{curr_close:.3f}"
                f" | SL:{self.law5_sl:.2f}",
                level="INFO",
            )

    def _detect_law6(self, new_bar):
        """
        [Law #6] Home Run (Velez Orthodox Version)

        职责：
            识别在明确趋势中，价格回踩/反弹到 20MA 一带后，
            形成 BT / TT（Bottoming Tail / Topping Tail）的强烈拒绝信号。

        设计原则：
        1. 优先复用 _detect_tail_bars() 的形态识别结果（BT / TT）
        2. 补上大趋势语境：顺 MA20 / MA200 大方向
        3. 保留 “穿透 20MA + 收回另一侧” 的核心逻辑
        4. 用“收盘位置质量”替代“实体 > 0.5 ATR”硬门槛
        5. 保留 tail bar 极端点作为结构止损
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law6 = False
        self.ready_to_short_law6 = False
        self.law6_sl = None
        self.law6_trigger_high = None
        self.law6_trigger_low = None
        self.law6_entry_type = None

        # =========================================================
        # 2) 物理审计
        # =========================================================
        if self.ma20 is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return
        if not hasattr(self, "current_tail_type"):
            return

        # =========================================================
        # 3) 基础形态数据计算
        # =========================================================
        high = float(new_bar["high"])
        low = float(new_bar["low"])
        open_p = float(new_bar["open"])
        close = float(new_bar["close"])

        total_range = high - low
        body_size = abs(close - open_p)

        if total_range <= 0:
            return

        upper_tail = high - max(open_p, close)
        lower_tail = min(open_p, close) - low

        ma20 = float(self.ma20)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # =========================================================
        # 4) 大趋势语境（更贴近 Velez 原教旨）
        # =========================================================
        bull_trend_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        )

        bear_trend_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        )

        # =========================================================
        # 5) 收盘位置质量审计（替代“实体必须 > 0.5 ATR”）
        #    Bull BT: 收盘必须明显回到 bar 上半区
        #    Bear TT: 收盘必须明显压到 bar 下半区
        # =========================================================
        close_position_from_low = (close - low) / total_range
        close_position_from_high = (high - close) / total_range

        bull_close_quality_ok = close_position_from_low >= 0.60
        bear_close_quality_ok = close_position_from_high >= 0.60

        # =========================================================
        # 6) 核心判定逻辑
        # =========================================================

        # ---------------------------------------------------------
        # 6.1 多头判定：Bottoming Tail (BT)
        # 条件：
        #   A. _detect_tail_bars 已识别为 BT
        #   B. Low 必须穿透 20MA
        #   C. Close 必须收回 20MA 之上
        #   D. 处于上涨趋势语境中
        #   E. 收盘位置足够强
        # ---------------------------------------------------------
        if self.current_tail_type == "BT":
            has_penetrated = low < ma20
            has_recovered = close > ma20

            if (
                bull_trend_ok
                and has_penetrated
                and has_recovered
                and bull_close_quality_ok
            ):
                self.ready_to_long_law6 = True
                self.law6_sl = low  # 固化长下影尖端
                self.law6_trigger_high = high
                self.law6_trigger_low = low
                self.law6_entry_type = "Breakout"

                ratio = lower_tail / max(body_size, 0.001)

                self.sys_log(
                    f"⚾ [Law#6-Velez] 🟢 Home Run (BT)"
                    f"|影线比:{ratio:.1f}x"
                    f"|收盘位置:{close_position_from_low:.2f}"
                    f"|trend_ok:{bull_trend_ok}"
                    f"|Low:{low:.3f} < MA20:{ma20:.3f} < Close:{close:.3f}"
                    f"|SL:{self.law6_sl:.2f}",
                    level="INFO",
                )

        # ---------------------------------------------------------
        # 6.2 空头判定：Topping Tail (TT)
        # 条件：
        #   A. _detect_tail_bars 已识别为 TT
        #   B. High 必须穿透 20MA
        #   C. Close 必须压回 20MA 之下
        #   D. 处于下跌趋势语境中
        #   E. 收盘位置足够弱
        # ---------------------------------------------------------
        elif self.current_tail_type == "TT":
            has_penetrated = high > ma20
            has_rejected = close < ma20

            if (
                bear_trend_ok
                and has_penetrated
                and has_rejected
                and bear_close_quality_ok
            ):
                self.ready_to_short_law6 = True
                self.law6_sl = high  # 固化长上影尖端
                self.law6_trigger_high = high
                self.law6_trigger_low = low
                self.law6_entry_type = "Breakout"

                ratio = upper_tail / max(body_size, 0.001)

                self.sys_log(
                    f"⚾ [Law#6-Velez] 🔴 Home Run (TT)"
                    f"|影线比:{ratio:.1f}x"
                    f"|收盘位置:{close_position_from_high:.2f}"
                    f"|trend_ok:{bear_trend_ok}"
                    f"|High:{high:.3f} > MA20:{ma20:.3f} > Close:{close:.3f}"
                    f"|SL:{self.law6_sl:.2f}",
                    level="INFO",
                )

    def _detect_law7(self, new_bar):
        """
        [Phase 2] Law #7: 200MA Reversion (预留占位)
        评价：高成长股常年偏离 200MA，逆势风险大，目前仅做架构保留，不输出信号。
        """
        # 如果以后需要激活，在这里编写价格与 MA200 偏离度的判定逻辑
        self.ready_to_long_law7 = False
        self.ready_to_short_law7 = False
        pass

    def _detect_law8(self, new_bar):
        """
        [Law #8] Fab 42 (Velez Orthodox Version)

        职责：
            识别 15m 大周期趋势方向与 2m 小周期恢复控制权动作之间的共振信号。

        设计原则：
        1. 保留 Fab42 核心：15m 趋势先行，2m 顺势触发
        2. 将 2m MA20 状态从日志信息升级为真实判定条件
        3. 将“第一根变色柱”升级为“第一根恢复控制权的 bar”
        4. 保留 8MA 偏离过滤，避免追涨杀跌
        5. 用 bar 自身质量审计替代单纯 ATR 力度门槛
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_law8 = False
        self.ready_to_short_law8 = False
        self.law8_sl = None
        self.law8_trigger_high = None
        self.law8_trigger_low = None
        self.law8_entry_type = None

        # =========================================================
        # 2) 物理审计
        # =========================================================
        if not hasattr(self, "effective_atr"):
            return
        if self.ma20 is None:
            return
        if self.ma8 is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return
        if self.history_2min_bars is None or len(self.history_2min_bars) < 2:
            return

        prev_bar = self.history_2min_bars.iloc[-2]
        if not (pd.notna(new_bar["high"]) and pd.notna(prev_bar["high"])):
            return

        # =========================================================
        # 3) 基础数据计算
        # =========================================================
        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])

        prev_h = float(prev_bar["high"])
        prev_l = float(prev_bar["low"])

        curr_price = c
        body_size = abs(c - o)
        bar_range = h - l

        if bar_range <= 0 or body_size <= 0:
            return

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_ratio = body_size / bar_range if bar_range > 0 else 0.0

        dist_to_ma8 = abs(curr_price - self.ma8)
        ma20 = float(self.ma20)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # 门槛设定
        extension_limit = self.effective_atr * 1.5  # 偏离极限
        power_threshold = self.effective_atr * 0.5  # 最低实体力度门槛（保留）

        is_green = c > o
        is_red = c < o

        # =========================================================
        # 4) 2m 小周期语境：从“日志信息”升级为真实条件
        # =========================================================
        bull_2m_context_ok = (
            ma200 is not None
            and ma20 > ma200
            and getattr(self, "is_ma20_turning_up", False)
        ) or (getattr(self, "is_ma20_turning_up", False) and curr_price > ma20)

        bear_2m_context_ok = (
            ma200 is not None
            and ma20 < ma200
            and getattr(self, "is_ma20_turning_down", False)
        ) or (getattr(self, "is_ma20_turning_down", False) and curr_price < ma20)

        # =========================================================
        # 5) 当前 bar 自身质量审计
        #    不再只看 ATR，加入 body_ratio + wick 质量
        # =========================================================
        bull_bar_quality_ok = (
            body_size > power_threshold
            and body_ratio >= 0.50
            and upper_wick <= 0.50 * body_size
        )

        bear_bar_quality_ok = (
            body_size > power_threshold
            and body_ratio >= 0.50
            and lower_wick <= 0.50 * body_size
        )

        # =========================================================
        # 6) 恢复控制权触发器
        #    第一根变色柱 + 突破前一根极值
        # =========================================================
        bull_reclaim_ok = is_green and self.green_bar_count == 1 and h > prev_h
        bear_reclaim_ok = is_red and self.red_bar_count == 1 and l < prev_l

        # =========================================================
        # 7) 核心判定逻辑
        # =========================================================

        # ---------------------------------------------------------
        # 7.1 多头共振判定 (Bull Resonance)
        # ---------------------------------------------------------
        if self.is_15m_trending_up:
            # A. 空间审计：检查是否过度拉升
            if dist_to_ma8 <= extension_limit:
                # B. 2m 语境 + 恢复控制权 + bar 质量
                if bull_2m_context_ok and bull_reclaim_ok and bull_bar_quality_ok:
                    self.ready_to_long_law8 = True
                    self.law8_sl = l
                    self.law8_trigger_high = h
                    self.law8_trigger_low = l
                    self.law8_entry_type = "Breakout"
                    ma20_status = (
                        "2m-MA20↑" if self.is_ma20_turning_up else "2m-MA20→/↓"
                    )

                    self.sys_log(
                        f"🚀 [Law#8-Velez] 🟢 Fab42 Resonance"
                        f" | 15m:UP"
                        f" | {ma20_status}"
                        f" | reclaim:{bull_reclaim_ok}"
                        f" | 实体:{body_size:.3f}"
                        f" | 实体占比:{body_ratio:.2f}"
                        f" | 偏离8MA:{dist_to_ma8:.3f}"
                        f" | 突破:{h:.3f}>{prev_h:.3f}"
                        f" | SL:{self.law8_sl:.2f}",
                        level="INFO",
                    )

        # ---------------------------------------------------------
        # 7.2 空头共振判定 (Bear Resonance)
        # ---------------------------------------------------------
        elif self.is_15m_trending_down:
            if dist_to_ma8 <= extension_limit:
                if bear_2m_context_ok and bear_reclaim_ok and bear_bar_quality_ok:
                    self.ready_to_short_law8 = True
                    self.law8_sl = h
                    self.law8_trigger_high = h
                    self.law8_trigger_low = l
                    self.law8_entry_type = "Breakout"

                    ma20_status = (
                        "2m-MA20↓" if self.is_ma20_turning_down else "2m-MA20→/↑"
                    )

                    self.sys_log(
                        f"🚀 [Law#8-Velez] 🔴 Fab42 Resonance"
                        f" | 15m:DOWN"
                        f" | {ma20_status}"
                        f" | reclaim:{bear_reclaim_ok}"
                        f" | 实体:{body_size:.3f}"
                        f" | 实体占比:{body_ratio:.2f}"
                        f" | 偏离8MA:{dist_to_ma8:.3f}"
                        f" | 跌破:{l:.3f}<{prev_l:.3f}"
                        f" | SL:{self.law8_sl:.2f}",
                        level="INFO",
                    )

    def _detect_v180(self, new_bar):
        """
        [V180] Power Shift Reversal (Velez Orthodox Version)

        职责：
            识别两根 K 线构成的控制权翻转（180 度反转）信号：
            前一根由一方控制，当前一根被另一方强势夺回主导权。

        设计原则：
        1. 保留 V180 核心：前红后绿 / 前绿后红 + 当前实体不小于前一实体
        2. 增加当前 bar 的收盘控制力审计（更贴近 power shift）
        3. 加入适度趋势语境，但不把 v180 压成纯 continuation
        4. 保留 MA20 位置过滤
        5. 将 washout 从硬门槛降级为增强标签
        """

        # =========================================================
        # 1) 初始化标志位，防止旧状态污染
        # =========================================================
        self.ready_to_long_v180 = False
        self.ready_to_short_v180 = False
        self.v180_sl = None
        self.v180_trigger_high = None
        self.v180_trigger_low = None
        self.v180_entry_type = None

        # =========================================================
        # 2) 物理审计
        # =========================================================
        if not hasattr(self, "effective_atr"):
            return
        if self.ma20 is None:
            return
        if not hasattr(self, "ma200"):
            return
        if not hasattr(self, "is_ma20_turning_up") or not hasattr(
            self, "is_ma20_turning_down"
        ):
            return
        if len(self.history_2min_bars) < 2:
            return

        prev_bar = self.history_2min_bars.iloc[-2]

        # =========================================================
        # 3) 基础数据计算
        # =========================================================
        prev_open = float(prev_bar["open"])
        prev_high = float(prev_bar["high"])
        prev_low = float(prev_bar["low"])
        prev_close = float(prev_bar["close"])

        curr_open = float(new_bar["open"])
        curr_high = float(new_bar["high"])
        curr_low = float(new_bar["low"])
        curr_close = float(new_bar["close"])

        body_prev = abs(prev_close - prev_open)
        body_curr = abs(curr_close - curr_open)
        total_range_curr = curr_high - curr_low

        if total_range_curr <= 0 or body_curr <= 0:
            return

        dist_to_ma20 = abs(curr_close - self.ma20)

        upper_wick = curr_high - max(curr_open, curr_close)
        lower_wick = min(curr_open, curr_close) - curr_low

        close_position_from_low = (curr_close - curr_low) / total_range_curr
        close_position_from_high = (curr_high - curr_close) / total_range_curr

        ma20 = float(self.ma20)
        ma200 = float(self.ma200) if self.ma200 is not None else None

        # =========================================================
        # 4) 门槛设定
        # =========================================================
        near_ma_limit = self.effective_atr * 1.5
        min_body_threshold = self.effective_atr * 0.4

        # =========================================================
        # 5) 基础 V180 形态：颜色翻转 + 当前实体 >= 前实体
        # =========================================================
        is_bull_v180_shape = (
            (prev_close < prev_open)
            and (curr_close > curr_open)
            and (body_curr >= body_prev)
        )

        is_bear_v180_shape = (
            (prev_close > prev_open)
            and (curr_close < curr_open)
            and (body_curr >= body_prev)
        )

        # =========================================================
        # 6) 当前 bar 的“收盘控制力”审计
        #    Bull: 收盘应靠近高位，且上影线不能太重
        #    Bear: 收盘应靠近低位，且下影线不能太重
        # =========================================================
        bull_close_control_ok = (
            close_position_from_low >= 0.60 and upper_wick <= 0.50 * body_curr
        )

        bear_close_control_ok = (
            close_position_from_high >= 0.60 and lower_wick <= 0.50 * body_curr
        )

        # =========================================================
        # 7) 趋势语境：适度加入，不把 v180 压得过死
        # =========================================================
        bull_context_ok = (
            getattr(self, "is_super_uptrend", False)
            or (ma200 is not None and ma20 > ma200 and curr_close >= ma20)
            or (getattr(self, "is_ma20_turning_up", False) and curr_close >= ma20)
        )

        bear_context_ok = (
            getattr(self, "is_super_downtrend", False)
            or (ma200 is not None and ma20 < ma200 and curr_close <= ma20)
            or (getattr(self, "is_ma20_turning_down", False) and curr_close <= ma20)
        )

        # =========================================================
        # 8) washout：由硬门槛降级为增强标签
        # =========================================================
        bull_has_washout = curr_low <= prev_low
        bear_has_washout = curr_high >= prev_high

        # =========================================================
        # 9) 位置审计：靠近 MA20，SuperTrend 可放宽
        # =========================================================
        bull_location_ok = getattr(self, "is_super_uptrend", False) or (
            dist_to_ma20 <= near_ma_limit
        )
        bear_location_ok = getattr(self, "is_super_downtrend", False) or (
            dist_to_ma20 <= near_ma_limit
        )

        # =========================================================
        # 10) Bull V180
        # =========================================================
        if (
            is_bull_v180_shape
            and body_curr > min_body_threshold
            and bull_close_control_ok
            and bull_context_ok
            and bull_location_ok
        ):
            self.ready_to_long_v180 = True
            self.v180_sl = min(curr_low, prev_low)
            self.v180_trigger_high = curr_high
            self.v180_trigger_low = curr_low
            self.v180_entry_type = "Breakout"

            strength_tag = "Washout+" if bull_has_washout else "Standard"
            engulf_ratio = (body_curr / max(body_prev, 0.001)) * 100

            self.sys_log(
                f"🔄 [V180-Velez] 🟢 Bull V180"
                f" | 类型:{strength_tag}"
                f" | 吞没率:{engulf_ratio:.0f}%"
                f" | 收盘位置:{close_position_from_low:.2f}"
                f" | 距MA20:{dist_to_ma20:.3f}"
                f" | context_ok:{bull_context_ok}"
                f" | location_ok:{bull_location_ok}"
                f" | washout:{bull_has_washout}"
                f" | SL:{self.v180_sl:.2f}",
                level="INFO",
            )

        # =========================================================
        # 11) Bear V180
        # =========================================================
        elif (
            is_bear_v180_shape
            and body_curr > min_body_threshold
            and bear_close_control_ok
            and bear_context_ok
            and bear_location_ok
        ):
            self.ready_to_short_v180 = True
            self.v180_sl = max(curr_high, prev_high)
            self.v180_trigger_high = curr_high
            self.v180_trigger_low = curr_low
            self.v180_entry_type = "Breakout"

            strength_tag = "Washout+" if bear_has_washout else "Standard"
            engulf_ratio = (body_curr / max(body_prev, 0.001)) * 100

            self.sys_log(
                f"🔄 [V180-Velez] 🔴 Bear V180"
                f" | 类型:{strength_tag}"
                f" | 吞没率:{engulf_ratio:.0f}%"
                f" | 收盘位置:{close_position_from_high:.2f}"
                f" | 距MA20:{dist_to_ma20:.3f}"
                f" | context_ok:{bear_context_ok}"
                f" | location_ok:{bear_location_ok}"
                f" | washout:{bear_has_washout}"
                f" | SL:{self.v180_sl:.2f}",
                level="INFO",
            )

    def _reset_all_ready_flags(self):
        """[07-12 信号自愈] 在每一根 2min 柱开始探测前清空旧信号"""
        # 法则信号清空
        self.ready_to_long_law1 = self.ready_to_short_law1 = False
        self.ready_to_long_law2 = self.ready_to_short_law2 = False
        self.ready_to_long_law3 = self.ready_to_short_law3 = False
        self.ready_to_long_law4 = self.ready_to_short_law4 = False
        self.ready_to_long_law5 = self.ready_to_short_law5 = False
        self.ready_to_long_law6 = self.ready_to_short_law6 = False
        self.ready_to_long_law7 = self.ready_to_short_law7 = False  # 预留
        self.ready_to_long_law8 = self.ready_to_short_law8 = False
        self.ready_to_long_v180 = self.ready_to_short_v180 = False
        # 辅助信号清空
        self.current_tail_type = None
        self.law1_sl = None
        self.law1_trigger_high = None
        self.law1_trigger_low = None
        self.law1_entry_type = None
        self.law1_elephant_high = 0.0
        self.law1_elephant_low = 0.0
        self.law1_elephant_open = 0.0
        self.law1_elephant_close = 0.0
        self.law1_score = 0
        self.law1_soft_clear = False
        self.law1_hard_clear = False
        self.law1_context = None

        self.law2_sl = None
        self.law2_trigger_high = None
        self.law2_trigger_low = None
        self.law2_entry_type = None

        self.law3_sl = None
        self.law3_trigger_high = None
        self.law3_trigger_low = None
        self.law3_entry_type = None

        self.law4_sl = None
        self.law4_trigger_high = None
        self.law4_trigger_low = None
        self.law4_entry_type = None

        self.law5_sl = None
        self.law5_trigger_high = None
        self.law5_trigger_low = None
        self.law5_entry_type = None

        self.law6_sl = None
        self.law6_trigger_high = None
        self.law6_trigger_low = None
        self.law6_entry_type = None

        self.law8_sl = None
        self.law8_trigger_high = None
        self.law8_trigger_low = None
        self.law8_entry_type = None

        self.v180_sl = None
        self.v180_trigger_high = None
        self.v180_trigger_low = None
        self.v180_entry_type = None

        self.sys_log(
            "🔄 [信号复位]_reset_all_ready_flags 已重置所有 Law 信号旗语",
            level="DEBUG",
        )

    def _detect_market_patterns(self, new_bar):
        """
        [信号层-总闸] 态势感知雷达：全量法则探测
        职责：调用所有形态探测器，更新信号旗语 (ready_to_xxx_lawX)
        """
        if new_bar is None:
            return
        if hasattr(new_bar, "empty") and new_bar.empty:
            return

        self.last_confirmed_tail = self.current_tail_type
        self._reset_all_ready_flags()
        self._detect_tail_bars(new_bar)

        self._detect_law1(new_bar)  # Elephant Bar (大象柱)
        self._detect_law2(new_bar)  # Color Change (颜色改变)
        self._detect_law3(new_bar)  # 3-5 Bars (回调反转)
        self._detect_law4(new_bar)  # RBI/GBI (忽略柱/影线延续)
        self._detect_law5(new_bar)  # 20MA Cross (价格均线穿越 - 无需传参)
        self._detect_law6(new_bar)  # Home Run (本垒打)
        # self._detect_law7(new_bar)        # 200MA Reversion (预留占位 - 无需传参)
        self._detect_law8(new_bar)  # Fabulous 42 (Fab 42)
        self._detect_v180(new_bar)  # v180 反转识别 (无需传参)

    def _select_signal(self):
        """
        [决策层-A1] 按优先级选出当前最高等级信号
        返回:
            side, label, raw_sl, trigger_high, trigger_low, entry_type
        """
        side, label = None, ""
        raw_sl, trigger_high, trigger_low, entry_type = None, None, None, None

        # 梯队 I
        if self.ready_to_long_law1 or self.ready_to_short_law1:
            side = "LONG" if self.ready_to_long_law1 else "SHORT"
            label = "Law1L" if self.ready_to_long_law1 else "Law1S"
            raw_sl = self.law1_sl
            trigger_high = self.law1_trigger_high
            trigger_low = self.law1_trigger_low
            entry_type = self.law1_entry_type

        elif self.ready_to_long_law2 or self.ready_to_short_law2:
            side = "LONG" if self.ready_to_long_law2 else "SHORT"
            label = "Law2L" if self.ready_to_long_law2 else "Law2S"
            raw_sl = self.law2_sl
            trigger_high = self.law2_trigger_high
            trigger_low = self.law2_trigger_low
            entry_type = self.law2_entry_type

        elif self.ready_to_long_v180 or self.ready_to_short_v180:
            side = "LONG" if self.ready_to_long_v180 else "SHORT"
            label = "V180L" if self.ready_to_long_v180 else "V180S"
            raw_sl = self.v180_sl
            trigger_high = self.v180_trigger_high
            trigger_low = self.v180_trigger_low
            entry_type = self.v180_entry_type

        elif self.ready_to_long_law4 or self.ready_to_short_law4:
            side = "LONG" if self.ready_to_long_law4 else "SHORT"
            label = "Law4L" if self.ready_to_long_law4 else "Law4S"
            raw_sl = self.law4_sl
            trigger_high = self.law4_trigger_high
            trigger_low = self.law4_trigger_low
            entry_type = self.law4_entry_type

        # 梯队 II
        elif self.ready_to_long_law6 or self.ready_to_short_law6:
            side = "LONG" if self.ready_to_long_law6 else "SHORT"
            label = "Law6L" if self.ready_to_long_law6 else "Law6S"
            raw_sl = self.law6_sl
            trigger_high = self.law6_trigger_high
            trigger_low = self.law6_trigger_low
            entry_type = self.law6_entry_type

        elif self.ready_to_long_law3 or self.ready_to_short_law3:
            side = "LONG" if self.ready_to_long_law3 else "SHORT"
            label = "Law3L" if self.ready_to_long_law3 else "Law3S"
            raw_sl = self.law3_sl
            trigger_high = self.law3_trigger_high
            trigger_low = self.law3_trigger_low
            entry_type = self.law3_entry_type

        # 梯队 III
        elif self.ready_to_long_law8 or self.ready_to_short_law8:
            side = "LONG" if self.ready_to_long_law8 else "SHORT"
            label = "Law8L" if self.ready_to_long_law8 else "Law8S"
            raw_sl = self.law8_sl
            trigger_high = self.law8_trigger_high
            trigger_low = self.law8_trigger_low
            entry_type = self.law8_entry_type

        elif self.ready_to_long_law5 or self.ready_to_short_law5:
            side = "LONG" if self.ready_to_long_law5 else "SHORT"
            label = "Law5L" if self.ready_to_long_law5 else "Law5S"
            raw_sl = self.law5_sl
            trigger_high = self.law5_trigger_high
            trigger_low = self.law5_trigger_low
            entry_type = self.law5_entry_type

        return side, label, raw_sl, trigger_high, trigger_low, entry_type

    def _compute_entry_price(self, side, trigger_high, trigger_low, entry_type):
        """
        [决策层-A2] 统一入场价格计算器
        """
        if side not in ("LONG", "SHORT"):
            return None

        if trigger_high is None or trigger_low is None:
            return None

        if not entry_type:
            return None

        min_tick = 0.01  # 当前仅针对美股科技股

        if entry_type == "Breakout":
            if side == "LONG":
                return round(float(trigger_high) + min_tick, 2)
            else:
                return round(float(trigger_low) - min_tick, 2)

        # 未来如需支持 GiftZone / Retest，可在这里扩展
        return None

    def analyze_signals(self, current_price, vix):
        """
        [决策层-A] 信号分拣与宏观审计
        职责：
        1. 按优先级选出唯一有效信号
        2. 做趋势 / MA20 / 15m 背离过滤
        3. 统一计算 entry_price
        """
        # 1) 熔断直接返回
        if self.suspend_today:
            self.sys_log(
                f"🚫 {self.symbol} 暂停交易。不再进行信号分析，只是观察记录信号",
                level="ERROR",
            )
            return None

        # 2) 选信号
        (
            side,
            label,
            raw_sl,
            trigger_high,
            trigger_low,
            entry_type,
        ) = self._select_signal()

        # 3) 基础合法性检查
        if side not in ("LONG", "SHORT"):
            return None
        if not label:
            return None
        if raw_sl is None:
            return None
        if trigger_high is None or trigger_low is None:
            return None
        if not entry_type:
            return None

        # 4) MA20 过滤
        if side == "LONG" and current_price < self.ma20:
            self.sys_log(
                f"🚫 [拒绝信号] {label} 原因：多头信号在 20MA 下方 "
                f"(价格:{current_price:.2f} < MA20:{self.ma20:.2f})",
                level="WARN",
            )
            return None

        if side == "SHORT" and current_price > self.ma20:
            self.sys_log(
                f"🚫 [拒绝信号] {label} 原因：空头信号在 20MA 上方 "
                f"(价格:{current_price:.2f} > MA20:{self.ma20:.2f})",
                level="WARN",
            )
            return None

        # 5) 15m 背离过滤
        is_15m_up = getattr(self, "is_15m_trending_up", None)
        is_15m_down = getattr(self, "is_15m_trending_down", None)

        if side == "LONG" and is_15m_up is not None and not is_15m_up:
            last_update = getattr(self, "last_15m_update_time", "N/A")
            self.sys_log(
                f"🛠️ ⚠️ [趋势背离] 2min已排布，但15min未转向向上，信号拦截"
                f" | {label} | 15m趋势:DOWN/SIDE | 缓存更新:{last_update}",
                level="WARN",
            )
            return None

        if side == "SHORT" and is_15m_down is not None and not is_15m_down:
            last_update = getattr(self, "last_15m_update_time", "N/A")
            self.sys_log(
                f"🛠️ ⚠️ [趋势背离] 2min已排布，但15min未转向向下，信号拦截"
                f" | {label} | 15m趋势:UP/SIDE | 缓存更新:{last_update}",
                level="WARN",
            )
            return None

        # 6) MA200 警告
        if getattr(self, "ma200", None) is not None:
            if side == "LONG" and current_price < self.ma200:
                self.sys_log(
                    "⚠️ [MA200警告] 多头信号在200MA下方，但微观结构健康，允许执行",
                    level="DEBUG",
                )
            if side == "SHORT" and current_price > self.ma200:
                self.sys_log(
                    "⚠️ [MA200警告] 空头信号在200MA上方，但微观结构健康，允许执行",
                    level="DEBUG",
                )

        # 7) 统一计算 entry_price
        entry_price = self._compute_entry_price(
            side=side,
            trigger_high=trigger_high,
            trigger_low=trigger_low,
            entry_type=entry_type,
        )

        if entry_price is None:
            self.sys_log(
                f"🚫 [拒绝信号] {label} 原因：无法计算统一入场价格",
                level="ERROR",
            )
            return None

        # 8) 风控方向检查
        if side == "LONG" and raw_sl >= entry_price:
            self.sys_log(
                f"🚫 [拒绝信号] {label} 原因：多头止损不合法 "
                f"(SL:{raw_sl:.2f} >= Entry:{entry_price:.2f})",
                level="ERROR",
            )
            return None

        if side == "SHORT" and raw_sl <= entry_price:
            self.sys_log(
                f"🚫 [拒绝信号] {label} 原因：空头止损不合法 "
                f"(SL:{raw_sl:.2f} <= Entry:{entry_price:.2f})",
                level="ERROR",
            )
            return None

        # 9) 输出 packet
        self.sys_log(
            f"🧭 [信号确认] {label}"
            f" | side:{side}"
            f" | entry_type:{entry_type}"
            f" | triggerH:{trigger_high:.2f}"
            f" | triggerL:{trigger_low:.2f}"
            f" | entry:{entry_price:.2f}"
            f" | raw_sl:{raw_sl:.2f}",
            level="INFO",
        )

        return {
            "side": side,
            "label": label,
            "raw_sl": raw_sl,
            "entry_price": entry_price,
            "entry_type": entry_type,
            "trigger_high": trigger_high,
            "trigger_low": trigger_low,
        }

    def _get_vix_multiplier(self):
        """
        [决策层-B1] VIX 风险折扣系数
        返回:
            vix_multiplier: float
        """
        vix_value = shared.global_last_vix_close
        vix_multiplier = 1.0

        if vix_value is not None:
            if vix_value < 15:
                vix_multiplier = 1.0
            elif vix_value < 20:
                vix_multiplier = 0.8
            elif vix_value < 25:
                vix_multiplier = 0.6
            elif vix_value < 30:
                vix_multiplier = 0.4
            else:
                vix_multiplier = 0.0
                self.sys_log(
                    f"🚫 [VIX 熔断] VIX={vix_value:.2f} ≥ 30，禁止入场",
                    level="WARN",
                )
                return None

            self.sys_log(
                f"📊 [VIX 仓位调节] VIX={vix_value:.2f} → 仓位系数={vix_multiplier*100:.0f}%",
                level="DEBUG",
            )

        vix_change_rate = getattr(shared, "vix_change_rate", 0.0)
        if vix_change_rate > 0.10:
            vix_multiplier *= 0.9
            self.sys_log(
                f"⚡ [VIX 加速上升] 变化率={vix_change_rate*100:.2f}%，额外降仓 10%",
                level="DEBUG",
            )

        return vix_multiplier

    def _get_execution_offset(self, label, entry_type):
        """
        [决策层-B2] 执行层挂单偏移
        说明:
            entry_price 是策略确认价
            lmt_price  = entry_price +/− execution_offset
        """
        is_high_priority = any(k in label for k in ["Law1", "V180"])

        # 当前先保持你的旧风格：高优先级更激进
        if entry_type == "Breakout":
            return 0.05 if is_high_priority else 0.02

        # 未来可扩展 GiftZone / Retest / CloseEntry
        return 0.02

    async def plan_trade(self, packet, snapshot: ContextSnapshot):
        """
        [决策层-B] 战术精算与工单拟定 V6.0 (分层纯净版)

        职责：
        1. 只消费 analyze_signals() 输出的标准 packet
        2. 进行风险、仓位、资金与收益比精算
        3. 生成 execution blueprint（instruction）
        4. 不再重算策略层 entry 逻辑
        """
        if not packet:
            return None

        # --- 1. 基础维度提取 ---
        side = packet["side"]
        label = packet["label"]
        entry = float(packet["entry_price"])
        raw_sl = float(packet["raw_sl"])
        trigger_high = float(packet["trigger_high"])
        trigger_low = float(packet["trigger_low"])
        entry_type = packet.get("entry_type", "Breakout")

        if side not in ["LONG", "SHORT"]:
            return None

        action = "BUY" if side == "LONG" else "SELL"

        # --- 2. 风险预算与空间核算 ---
        is_pyramid = (
            snapshot.has_position
            and getattr(self, "tp1_filled", False)
            and snapshot.abs_pos < self.max_qty
        )
        current_risk_money = self.risk_unit * 0.5 if is_pyramid else self.risk_unit

        current_holding_abs = snapshot.abs_pos
        gap_qty = max(0, self.max_qty - current_holding_abs)

        # --- 3. VIX 系数 ---
        vix_multiplier = self._get_vix_multiplier()

        # --- 4. ATR 与参考价 ---
        atr = self.effective_atr
        min_gap = round(1.0 * atr, 2)

        reference_price = (
            snapshot.avg_cost
            if (snapshot.has_position and snapshot.avg_cost > 0)
            else entry
        )

        # --- 5. 止损与止盈计算 ---
        if side == "LONG":
            sl_candidate = raw_sl
            sl_tightened = reference_price - min_gap
            sl = round(max(sl_candidate, sl_tightened), 2)
            r = abs(entry - sl)
            tp1 = reference_price + 1.5 * r
            tp2 = reference_price + 3.0 * r

        else:  # SHORT
            sl_candidate = raw_sl
            sl_tightened = reference_price + min_gap
            sl = round(min(sl_candidate, sl_tightened), 2)
            r = abs(sl - entry)
            tp1 = reference_price - 1.5 * r
            tp2 = reference_price - 3.0 * r

        # --- 6. 风险距离与收益比审计 ---
        risk_dist = round(abs(entry - sl), 2)
        if risk_dist <= 0:
            self.sys_log(
                f"🚫 [风控拦截] {label} 止损距离无效 (risk_dist={risk_dist:.3f})",
                level="ERROR",
            )
            return None

        self.sys_log(
            f"📊 [风险精算] label={label} | is_pyramid={is_pyramid} | "
            f"risk_unit={self.risk_unit} | current_risk_money={current_risk_money}",
            level="DEBUG",
        )

        reward_potential = round(abs(tp1 - entry), 2)
        rr_ratio = reward_potential / risk_dist if risk_dist > 0 else 0

        min_rr_ratio = 1.5
        if rr_ratio < min_rr_ratio:
            self.sys_log(
                f"⚠️ [放弃] {label} 盈亏比{rr_ratio:.2f}:1 < {min_rr_ratio}:1 | "
                f"risk:{risk_dist/atr:.1f}ATR, reward:{reward_potential/atr:.1f}ATR",
                level="WARN",
            )
            return None
        # --- 7. 股数计算 ---
        suggested_shares = int(current_risk_money // risk_dist)
        suggested_shares = int(suggested_shares * vix_multiplier)

        if not snapshot.has_position:
            shares = min(suggested_shares, self.max_qty)
        else:
            shares = min(suggested_shares, gap_qty, int(current_holding_abs * 0.5))

        if shares < 10:
            return None

        # --- 8. 保证金/可用资金审计 ---
        try:
            av_funds = await get_account_available_funds()
            safe_funds = av_funds * (1 - getattr(self, "capital_buffer", 0.05))
            required_margin = (entry * shares) * getattr(
                self, "margin_requirement", 0.3
            )

            if required_margin > safe_funds:
                self.sys_log(
                    f"🚨 [精算拦截] 资金不足! 需:${required_margin:.2f}",
                    level="WARN",
                )
                return None
        except Exception as e:
            self.sys_log(f"⚠️ [精算异常] 无法验证保证金: {e}", level="ERROR")
            return None

        # --- 9. 执行层挂单价格 ---
        exec_offset = self._get_execution_offset(label, entry_type)

        if side == "LONG":
            lmt_price = round(entry + exec_offset, 2)
        else:
            lmt_price = round(entry - exec_offset, 2)

        # --- 10. 理论 / 执行盈亏比审计 ---
        theoretical_loss = abs(entry - sl)
        theoretical_profit = abs(tp1 - entry)
        theoretical_rr = (
            theoretical_profit / theoretical_loss if theoretical_loss > 0 else 0
        )

        if side == "LONG":
            plan_loss_per_share = abs(round(lmt_price - sl, 2))
            plan_profit_per_share = abs(round(tp1 - lmt_price, 2))
        else:
            plan_loss_per_share = abs(round(sl - lmt_price, 2))
            plan_profit_per_share = abs(round(lmt_price - tp1, 2))

        plan_loss_total = abs(round(plan_loss_per_share * shares, 2))
        self.plan_loss = plan_loss_total
        execution_rr = (
            round(plan_profit_per_share / plan_loss_per_share, 2)
            if plan_loss_per_share > 0
            else None
        )

        # --- 11. 优先级 ---
        is_high_priority = any(k in label for k in ["Law1", "V180"])

        # --- 12. 封装 instruction ---
        instruction = {
            "side": side,
            "action": action,
            "shares": shares,
            "lmt_price": lmt_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp1_qty": int((current_holding_abs + shares) * 0.5),
            "priority": "Urgent" if is_high_priority else "Normal",
            "label": label,
            "trigger_price": entry,
            "entry_type": entry_type,
            "trigger_high": trigger_high,
            "trigger_low": trigger_low,
            "exec_offset": exec_offset,
            "risk_dist": risk_dist,
            "theoretical_rr": theoretical_rr,
            "execution_rr": execution_rr,
            "plan_loss_total": plan_loss_total,
        }

        # --- 13. 审计日志 ---
        self.sys_log(
            f"📝 [入场指令(0)] 信号:{label} | MA200={self.ma200:.2f} | MA20={self.ma20:.2f} | MA8={self.ma8:.2f}",
            level="INFO",
        )
        self.sys_log(
            f"📝 [入场指令(1)] {action} {shares}股 | trigger={entry:.2f} | lmt={lmt_price:.2f} | SL={sl:.2f}",
            level="INFO",
        )
        self.sys_log(
            f"📝 [入场指令(2)] 预计总风险:{plan_loss_total:.2f} | TP1={tp1:.2f} ",
            level="INFO",
        )
        self.sys_log(
            f"📊 [入场指令(3)] 理论盈亏比:{theoretical_rr:.2f}:1 (基准:entry={entry:.2f})"
            f" | 执行盈亏比:{execution_rr if execution_rr is not None else 'N/A'}:1 (基准:lmt={lmt_price:.2f})",
            level="INFO",
        )

        return instruction

    async def run_decision_pipeline(
        self, current_price, vix, snapshot: ContextSnapshot
    ):
        """
        [决策驱动器 V5.0]
        职责：
        1. 物理准入：利用 snapshot 事实持仓和计划持仓计算，确保绝不超卖或重复下单。
        2. 逻辑隔离：analyze -> plan -> execute 全流程共享同一份 snapshot 快照。
        3. 自动加仓判定：基于快照股数事实决定是否允许 Pyramid。
        """
        # --- 1. 信号提取与即时释放 (原子操作) ---
        packet = self.active_signal
        self.active_signal = None
        if not packet:
            return

        try:
            # --- 2. 虚拟持仓与并发拦截 (V5.0 核心) ---
            # A. 检查柜台是否有正在执行的“进场/加仓”挂单
            # 逻辑：只要 live_orders 里有非止损单，说明意图正在执行，拒绝产生新意图
            has_intent_in_flight = any(
                o.parentId == 0 and o.orderType not in ["STP", "STP LMT"]
                for o in snapshot.live_orders
            )
            if has_intent_in_flight:
                # self.sys_log(f"🛰️ [决策拦截] 柜台已有在途指令，放弃信号 {packet['label']}", level="DEBUG")
                return

            # B. 准入逻辑重塑：空仓入场 或 符合条件的加仓
            # 逻辑：利用快照判定
            can_enter_new = not snapshot.has_position and self.state == "OPEN_STAGE"

            # 加仓准入：已减仓(tp1_filled) 且 当前持仓 < 上限
            can_pyramid = (
                snapshot.has_position
                and getattr(self, "tp1_filled", False)
                and snapshot.abs_pos < self.max_qty
            )

            if not (can_enter_new or can_pyramid):
                return

            # --- 3. 交易精算 (Snapshot 注入) ---
            # 注意：plan_trade 内部也需要同步适配 snapshot 参数（下一步重塑）
            instruction = await self.plan_trade(packet, snapshot)
            if not instruction:
                self.sys_log(
                    f"⚠️ [放弃入场机会] {packet['label']} 风险收益比不佳，不下单",
                    level="DEBUG",
                )
                return

            # --- 4. 时间闸门拦截 ---
            now_et = datetime.now(EASTERN_TZ).time()
            if not (time(10, 0) <= now_et <= time(15, 30)):
                self.sys_log(
                    f"🚫 [时间禁令] 当前时间{now_et}，程序在9:30-10:00以及15:30-15:58两个时间段禁止入场交易，仅记录信号或者被动止盈止损。",
                    level="INFO",
                )
                return

            # --- 5. 物理执行 (原子发射) ---
            # execute_trade 内部不再绑定复杂回调，只管发射，由节拍器闭环
            await self.execute_trade(instruction)

        except Exception as e:
            self.sys_log(f"💥 [决策中心崩溃] 指令传导中断: {e}", level="ERROR")

    async def _trailing_stop(self, current_price, snapshot: ContextSnapshot):
        """
        [策略层 - 追踪引擎] Velez 移动止损追踪函数
        """
        if not snapshot.active_stop_order:
            return None

        if (getattr(snapshot.active_stop_order, "orderRef", "") or "").startswith(
            ("E_", "A_")
        ):
            return None
        try:
            side = snapshot.direction
            cost = snapshot.avg_cost
            atr = self.effective_atr
            current_sl = None
            r = abs(cost - self.initial_stop_price)

            if snapshot.active_stop_order:
                current_sl = snapshot.active_stop_order.auxPrice

            if current_price <= 100.00:
                buf = 0.02
            elif current_price > 100.00 and current_price <= 200.00:
                buf = 0.03
            elif current_price > 200.00 and current_price <= 300.00:
                buf = 0.04
            elif current_price > 300.00 and current_price <= 400.00:
                buf = 0.05
            elif current_price > 400.00 and current_price <= 500.00:
                buf = 0.06
            elif current_price > 500.00 and current_price <= 600.00:
                buf = 0.07
            elif current_price > 600.00 and current_price <= 700.00:
                buf = 0.08
            elif current_price > 700.00 and current_price <= 800.00:
                buf = 0.09
            else:
                buf = 0.10

            if side == "LONG":
                breakeven = round(cost + buf, 2)
                r1 = cost + r
            elif side == "SHORT":
                breakeven = round(cost - buf, 2)
                r1 = cost - r
            else:
                self.sys_log(
                    f"🔍 [_trailing_stop Return] side =:{side} | ",
                    level="DEBUG",
                )
                return None

            if self.dip_of_2min > 0:
                dip = self.dip_of_2min
            else:
                dip = current_sl

            if self.bounce_of_2min > 0:
                bounce = self.bounce_of_2min
            else:
                bounce = current_sl

            if self.low_of_4min > 0:
                low_of_4min = self.low_of_4min
            else:
                low_of_4min = current_sl

            if self.high_of_4min > 0:
                high_of_4min = self.high_of_4min
            else:
                high_of_4min = current_sl

            if self.holding_start_time > 0:
                elapsed = int(time_module.time() - self.holding_start_time)
            else:
                elapsed = int(0)

            if side == "LONG":
                # 1. 先检查 当前价格current_price是否 涨过了r1价格线
                if current_price < r1:
                    if elapsed <= 240:  # 持仓没有超过4分钟，保持现有止损价格不动
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L1"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🔍 [_trailing_stop 保本] breakeven={breakeven:.3f} | "
                                f"buf={buf:.3f} | 盈利幅度={breakeven - current_price if side == 'SHORT' else current_price - breakeven:.3f}",
                                level="DEBUG",
                            )
                            self.sys_log(
                                f"🧭 [追踪止损L1] 持仓时间={elapsed},不调整止损价 ",
                                level="DEBUG",
                            )
                        return None
                    elif elapsed > 240 and current_price <= round(
                        cost + 0.5 * r, 2
                    ):  # 持仓超过4分钟，但是浮动盈利还没有超过0.5*R
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L2"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🔍 [_trailing_stop 保本] breakeven={breakeven:.3f} | "
                                f"buf={buf:.3f} | 盈利幅度={breakeven - current_price if side == 'SHORT' else current_price - breakeven:.3f}",
                                level="DEBUG",
                            )
                            self.sys_log(
                                f"🧭 [追踪止损L2] 持仓时间{elapsed}秒, 浮动盈利还没有超过0.5*R{round(0.5*r,2)},主动平仓离场",
                                level="DEBUG",
                            )
                            await self.clear_pos(snapshot)
                        return None
                    else:  # 持仓时间超过 4min，而且浮动盈利在 0.5R和 1R之间，保持静默
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L3"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🔍 [_trailing_stop 保本] breakeven={breakeven:.3f} | "
                                f"buf={buf:.3f} | 盈利幅度={breakeven - current_price if side == 'SHORT' else current_price - breakeven:.3f}",
                                level="DEBUG",
                            )
                            self.sys_log(
                                f"🧭 [追踪止损L3] 持仓时间{elapsed}, 浮动盈利<1R 但是 >0.5R,止损价格不动",
                                level="DEBUG",
                            )
                        return None

                elif current_price >= r1 and current_price < round(cost + 1.5 * r, 2):
                    # current_price >= r1 但是还没到 TP1, 已经大幅盈利了
                    new_sl = max(current_sl, low_of_4min)
                    if new_sl >= current_sl + 0.05:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L4"
                        self.sys_log(
                            f"🧭 [追踪止损L4]准备把止损价从{current_sl:.2f} 抬升到{new_sl:.2f}，当前市价{current_price:.2f}",
                            level="DEBUG",
                        )
                        return new_sl

                    else:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L5"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🧭 [追踪止损L5]当前止损价{current_sl:.2f},暂时不调整,当前市价{current_price:.2f}",
                                level="DEBUG",
                            )
                        return None
                elif current_price >= round(cost + 1.5 * r, 2):  # 当前价格已经在TP1之上
                    new_sl = max(current_sl, breakeven, dip)
                    if new_sl >= current_sl + 0.05:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L6"
                        self.sys_log(
                            f"🧭 [追踪止损L6]准备把止损价从{current_sl:.2f} 抬升到{new_sl:.2f}，保本价={breakeven:.2f}",
                            level="DEBUG",
                        )
                        return new_sl
                    else:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "L7"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🧭 [追踪止损L7]当前止损价{current_sl:.2f},暂时不调整",
                                level="DEBUG",
                            )
                        return None
                else:  # 应该永远不会到这个分支
                    self.last_stop_cond = self.current_stop_cond
                    self.current_stop_cond = "L8"
                    if self.current_stop_cond != self.last_stop_cond:
                        self.sys_log(
                            f"🧭 [追踪止损L8]当前止损价{current_sl:.2f},当前市价{current_price:.2f},逻辑判断出现问题，请检查程序",
                            level="DEBUG",
                        )
                    return None

            # === 做空场景（对称逻辑）===
            elif side == "SHORT":
                # 1. 先检查 当前价格current_price是否下跌低于了r1价格线
                if current_price > r1:
                    if elapsed <= 240:  # 持仓没有超过4分钟，保持现有止损价格不动
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S1"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🔍 [_trailing_stop 保本] breakeven={breakeven:.3f} | "
                                f"buf={buf:.3f} | 盈利幅度={breakeven - current_price if side == 'SHORT' else current_price - breakeven:.3f}",
                                level="DEBUG",
                            )
                            self.sys_log(
                                f"🧭 [追踪止损S1] 持仓时间={elapsed},不调整止损价 ",
                                level="DEBUG",
                            )
                        return None
                    elif elapsed > 240 and current_price >= round(
                        cost - 0.5 * r, 2
                    ):  # 持仓超过4分钟，但是浮动盈利还没有超过0.5*R
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S2"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🔍 [_trailing_stop 保本] breakeven={breakeven:.3f} | "
                                f"buf={buf:.3f} | 盈利幅度={breakeven - current_price if side == 'SHORT' else current_price - breakeven:.3f}",
                                level="DEBUG",
                            )
                            self.sys_log(
                                f"🧭 [追踪止损S2] 持仓时间{elapsed}秒, 浮动盈利还没有超过0.5*R{round(0.5*r,2)},主动平仓离场",
                                level="DEBUG",
                            )
                            await self.clear_pos(snapshot)
                        return None
                    else:  # 持仓时间超过 4min，而且浮动盈利在 0.5R和 1R之间，保持静默
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S3"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🔍 [_trailing_stop 保本] breakeven={breakeven:.3f} | "
                                f"buf={buf:.3f} | 盈利幅度={breakeven - current_price if side == 'SHORT' else current_price - breakeven:.3f}",
                                level="DEBUG",
                            )
                            self.sys_log(
                                f"🧭 [追踪止损S3] 持仓时间{elapsed}, 浮动盈利<1R 但是 >0.5R,止损价格不动",
                                level="DEBUG",
                            )
                        return None

                elif current_price <= r1 and current_price > round(cost - 1.5 * r, 2):
                    # current_price <= r1 但是还没到 TP1, 已经大幅盈利了
                    new_sl = min(current_sl, high_of_4min)
                    if new_sl <= current_sl - 0.05:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S4"
                        self.sys_log(
                            f"🧭 [追踪止损S4]准备把止损价从{current_sl:.2f} 下降到{new_sl:.2f}，当前市价{current_price:.2f}",
                            level="DEBUG",
                        )
                        return new_sl

                    else:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S5"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🧭 [追踪止损S5]当前止损价{current_sl:.2f},暂时不调整,当前市价{current_price:.2f}",
                                level="DEBUG",
                            )
                        return None
                elif current_price <= round(cost - 1.5 * r, 2):  # 当前价格已经在TP1之下
                    new_sl = min(current_sl, breakeven, bounce)
                    if new_sl <= current_sl - 0.05:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S6"
                        self.sys_log(
                            f"🧭 [追踪止损S6]准备把止损价从{current_sl:.2f} 下降到{new_sl:.2f}，保本价={breakeven:.2f}",
                            level="DEBUG",
                        )
                        return new_sl
                    else:
                        self.last_stop_cond = self.current_stop_cond
                        self.current_stop_cond = "S7"
                        if self.current_stop_cond != self.last_stop_cond:
                            self.sys_log(
                                f"🧭 [追踪止损S7]当前止损价{current_sl:.2f},暂时不调整",
                                level="DEBUG",
                            )
                        return None
                else:  # 应该永远不会到这个分支
                    self.last_stop_cond = self.current_stop_cond
                    self.current_stop_cond = "S8"
                    if self.current_stop_cond != self.last_stop_cond:
                        self.sys_log(
                            f"🧭 [追踪止损S8]当前止损价{current_sl:.2f},当前市价{current_price:.2f},逻辑判断出现问题，请检查程序",
                            level="DEBUG",
                        )
                    return None

            else:
                # === 异常情况 ===
                self.sys_log(
                    f"❌ [跟踪止损引擎] 未知方向,SIDE=={side}",
                    level="ERROR",
                )
                return None
        except Exception as e:
            self.sys_log(
                f"❌ [trailing_stop]追踪止损单异常，错误代码: {e}", level="ERROR"
            )
            return None  # 🔥 明确返回

    async def _update_stop(
        self,
        price=None,
        volume=None,
        force=False,
        snapshot: Optional[ContextSnapshot] = None,
    ):
        """
        [肢体层-物理阀门 V7.7]
        集成点：1. 股数对账 2. 影子变量同步 3. 原子性锁 4. 单向防呆
        """

        if snapshot is None:
            self.sys_log(
                "❌ [_update_stop] 关键错误：未传入快照，拒绝执行止损单修改",
                level="ERROR",
            )
            return
        try:
            # --- 1. 定位物理对象 ---
            order = snapshot.active_stop_order if snapshot else None
            if not order:
                # 即使没有活跃止损单，如果实仓还在，也要转入 add_stop 补防
                if snapshot and snapshot.abs_pos > 0:
                    await self.add_stop(snapshot)
                return
            tp_in_flight = 0.0
            old_qty = order.totalQuantity
            old_sl = order.auxPrice
            needs_update = False
            # 纠偏(force)时使用极小阈值，追踪时使用 0.02 防抖
            STEP_THRESHOLD = 0.001 if force else 0.02
            side = snapshot.direction

            # --- 2. 价格精算与单向防呆 ---
            price_modified = False  # 🔥 标记：是否有价格修改
            if price is not None:
                new_sl = round(price, 2)
                # 严禁止损向亏损方向移动（做多只能上移，做空只能下移）
                if side == "LONG" and new_sl >= old_sl + STEP_THRESHOLD:
                    order.auxPrice = new_sl
                    needs_update = True
                    price_modified = True  # 🔥 标记价格已修改
                elif side == "SHORT" and new_sl <= old_sl - STEP_THRESHOLD:
                    order.auxPrice = new_sl
                    needs_update = True
                    price_modified = True  # 🔥 标记价格已修改

            # --- 3. 股数强制对账 ---
            volume_modified = False
            if volume is not None:
                if snapshot.entry_orders:  # 有未完成的入场单（主单在途）
                    # 🔥 新增：检查距离上次成交是否超过 30 秒
                    time_since_exec = time_module.time() - getattr(
                        self, "last_exec_ts", 0
                    )
                    if time_since_exec > self.stop_qty_timeout:
                        # ✅ 超过 self.stop_qty_timeout（目前=15 秒），认为主订单不会再成交了，允许修改止损股数
                        target_qty = int(volume)
                        if target_qty > 0 and order.totalQuantity != target_qty:
                            order.totalQuantity = target_qty
                            needs_update = True
                            volume_modified = True
                            self.sys_log(
                                f"✅ [_update_stop] 在途订单超时{time_since_exec:.0f}秒，"
                                f"允许修改止损单股数：{old_qty} → {target_qty}",
                                level="INFO",
                            )
                    else:
                        #  self.stop_qty_timeout超时阈值内（15 秒内），继续拦截，等待主订单成交
                        self.sys_log(
                            f"🛡️ [拦截修改止损数量] 检测到{len(snapshot.entry_orders)}笔在途入场的开仓单或者加仓单)，"
                            f"距离上次成交{time_since_exec:.0f}秒 < {self.stop_qty_timeout} 秒，暂不修改止损单股数",
                            level="DEBUG",
                        )
                else:  # 主单已完全成交
                    target_qty = int(volume)
                    if target_qty > 0 and order.totalQuantity != target_qty:
                        order.totalQuantity = target_qty
                        needs_update = True
                        volume_modified = True

            # --- 4. 冲突拦截与物理提交 ---
            if needs_update:
                # 计算在途的止盈单股数（排除止损单）
                tp_orders = [
                    o
                    for o in snapshot.closing_orders
                    if getattr(o, "orderRef", "").startswith(("TP1_", "TP2_"))
                ]

                tp_in_flight = sum(o.totalQuantity for o in tp_orders)

                # 🔥 修复：只拦截数量修改，允许价格修改
                if (
                    volume_modified
                    and tp_in_flight >= snapshot.abs_pos
                    and snapshot.abs_pos > 0
                ):
                    # 恢复原数量（避免重复平仓）
                    order.totalQuantity = old_qty
                    needs_update = price_modified  # 仅当有价格修改时才提交
                    self.sys_log(
                        f"⚠️ [止损数量修改] 发现止盈单在途 ({tp_in_flight}股) >= 持仓 ({snapshot.abs_pos}股)，"
                        f"恢复原数量{old_qty}股 | 价格修改：{'允许' if price_modified else '无'}",
                        level="DEBUG",
                    )

            if needs_update:
                # 🔥 新增：记录剩余仓位信息（便于调试）
                if tp_in_flight > 0 and tp_in_flight < snapshot.abs_pos:
                    remaining_pos = snapshot.abs_pos - tp_in_flight
                    self.sys_log(
                        f"✅ [允许止损价格修改] TP 在途:{tp_in_flight}股 | 剩余仓位:{remaining_pos}股 | "
                        f"止损价格从{old_sl} 变更为{order.auxPrice}",
                        level="INFO",
                    )

                # 提交修改止损单的指令给TWS
                # 物理锁定
                self.is_exiting = True
                self.ib.placeOrder(self.contract, order)
                self.sys_log(
                    f"⚡ [止损单调整成功] {order.action} "
                    f"{'数量从' + str(old_qty) + '变更为' + str(order.totalQuantity) + '股 @ ' if volume_modified else ''}"
                    f"价格从{old_sl} 变更为 {order.auxPrice} | "
                    f"Force:{force} | TP 在途:{tp_in_flight}股",
                    level="INFO",
                )

                self.final_stop_price = order.auxPrice  # 记忆最新物理止损价

                self._temp_order_audit.update(
                    {
                        # "label": "Update_Stop_Final",
                        "order_id": order.orderId,
                        "last_s_aux": order.auxPrice,
                    }
                )
            else:
                pass
                # self.sys_log(
                #    f"⏭️ [止损单无需更新] 价格修改:{price_modified} | 数量修改:{volume_modified} | TP 在途:{tp_in_flight}股",
                #    level="DEBUG",
                # )

        except Exception as e:
            self.sys_log(
                f"❌ [_update_stop] 止损单更新异常，错误代码: {e}", level="ERROR"
            )

    async def _submit_tp(
        self,
        qty: int,
        price: Optional[float] = None,
        snapshot: Optional[ContextSnapshot] = None,
    ):
        """
        [执行层-平仓执行器]
        职责：
        1. 基于快照核算 TP1 可平余量
        2. 提交 TP1 止盈单
        """
        try:
            if snapshot is None:
                snapshot = getattr(self, "latest_snapshot", None)
            if not snapshot or not snapshot.has_position:
                return None

            # 仅核算 TP1 在途单
            in_flight_closing_qty = sum(
                o.totalQuantity
                for o in snapshot.closing_orders
                if getattr(o, "orderRef", "").startswith("TP1")
            )
            available_to_close = snapshot.abs_pos - in_flight_closing_qty

            self.sys_log(
                f"🔍 [TP1核算] 持仓:{snapshot.abs_pos} | TP1在途:{in_flight_closing_qty} | "
                f"可平余量:{available_to_close} | 请求量:{qty}",
                level="DEBUG",
            )

            if qty <= 0 or available_to_close <= 0:
                self.sys_log(
                    f"⚠️ [TP1拦截] 可平余量不足 | 持仓:{snapshot.abs_pos} | TP1在途:{in_flight_closing_qty}",
                    level="DEBUG",
                )
                return None

            self.is_exiting = True
            final_qty = int(min(qty, available_to_close))

            action = "SELL" if snapshot.direction == "LONG" else "BUY"
            abv_action = "BOT" if action == "BUY" else "SLD"

            timestr = datetime.now(EASTERN_TZ).strftime("%H%M%S")
            if price is not None:
                final_price = (
                    round(price - 0.01, 2)
                    if action == "SELL"
                    else round(price + 0.01, 2)
                )
                m_order = LimitOrder(action, final_qty, final_price)
                m_order.algoStrategy = "Adaptive"
                m_order.algoParams = [TagValue("adaptivePriority", "Normal")]
                m_order.orderRef = (
                    f"TP1_{self.symbol[:4]}_Tkpft_{abv_action[:3]}_lmt_{timestr[:6]}"[
                        :32
                    ]
                )
            else:
                m_order = MarketOrder(action, final_qty)
                m_order.algoStrategy = "Adaptive"
                m_order.algoParams = [TagValue("adaptivePriority", "Urgent")]
                m_order.orderRef = (
                    f"TP1_{self.symbol[:4]}_Tkpft_{abv_action[:3]}_mkt_{timestr[:6]}"[
                        :32
                    ]
                )

            trade = self.ib.placeOrder(self.contract, m_order)
            self.filled_flag = True

            self.sys_log(
                f"📉 [TP1止盈单] 提交给TWS-{action}-{final_qty}股，Ref={m_order.orderRef} | "
                f"剩余可平余量:{available_to_close - final_qty}",
                level="DECISION",
            )
            return trade

        except Exception as e:
            self.is_exiting = False
            self.sys_log(f"❌ [_submit_tp] 严重异常: {e}", level="ERROR")
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
            return None

    async def _detect_take_profit(
        self, current_price: float, snapshot: Optional[ContextSnapshot]
    ):
        """
        [指挥部-止盈决策]
        职责：仅判定 TP1 触碰事实，并下达一次部分止盈指令。
        """
        if snapshot is None:
            self.sys_log(
                "❌ [_detect_take_profit] 关键错误：未传入快照，拒绝执行止盈探测",
                level="ERROR",
            )
            return

        # 只保留 TP1 在途检查
        if snapshot.tp1_active:
            self.last_tp_cond = self.current_tp_cond
            self.current_tp_cond = "T1"
            if self.last_tp_cond != self.current_tp_cond:
                self.sys_log(
                    "🛡️ [TP1探测] TP1在途，继续监控止盈和止损追踪",
                    level="DEBUG",
                )
            return

        if getattr(self, "tp1_filled", False):
            self.last_tp_cond = self.current_tp_cond
            self.current_tp_cond = "T0"
            if self.last_tp_cond != self.current_tp_cond:
                self.sys_log(
                    "🛡️ [TP1探测] TP1已提交并且成交了，本轮不再重复提交",
                    level="DEBUG",
                )
            return

        try:
            curr_low = current_price
            curr_high = current_price

            bars = getattr(self, "bars_reference", None)
            if bars and hasattr(bars, "__len__") and len(bars) > 0:
                try:
                    curr_low = min(curr_low, bars[-1].low)
                    curr_high = max(curr_high, bars[-1].high)
                except Exception as e:
                    self.sys_log(f"⚠️ [TP1] bars_reference异常: {e}", level="DEBUG")

            target_price = self.tp1

            tp_buffer = max(self.effective_atr * 0.5, 0.05)
            if snapshot.direction == "LONG":
                is_hit = curr_high >= target_price - tp_buffer
            else:
                is_hit = curr_low <= target_price + tp_buffer

            if not is_hit:
                return

            # 只提交 TP1：默认减半
            tp_qty = max(int(snapshot.abs_pos * 0.5), 1)

            self.last_tp_cond = self.current_tp_cond
            self.current_tp_cond = "T3"
            if self.last_tp_cond != self.current_tp_cond:
                self.sys_log(
                    f"🎯 [提交TP1] 目标价{target_price:.2f} | 计划止盈{tp_qty}股 | 当前持仓{snapshot.abs_pos}",
                    level="DECISION",
                )

            await self._submit_tp(
                qty=tp_qty,
                price=target_price,
                snapshot=snapshot,
            )

        except Exception as e:
            self.sys_log(f"❌ [_detect_take_profit] 函数运行异常: {e}", level="ERROR")
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")

    async def execute_trade(self, instruction):
        """
        [执行层 V5.0 执行下单交易指令函数 - 彻底去状态版]
        职责：
        1. 物理发射：将下单指令推送到 TWS，利用局部变量承载订单引用。
        2. 摇铃触发：设置 filled_flag，驱动下一节拍通过柜台事实自动接管订单。
        3. 异常熔断：若发射失败，通过物理扫描撤回该品种所有未确认单据。
        """
        if self.is_processing_order:
            return
        if not instruction:
            return
        self.is_processing_order = True
        current_active_trade = None

        try:
            # --- 1. 参数解构 ---
            action = instruction["action"]
            qty = instruction["shares"]
            lmt_price = instruction["lmt_price"]
            entry_ref = instruction[
                "trigger_price"
            ]  # 给StopLimitOrder使用，目前没有用了
            sl, tp1, tp2 = instruction["sl"], instruction["tp1"], instruction["tp2"]
            tp1_qty = instruction["tp1_qty"]
            label, priority = instruction["label"], instruction["priority"]
            entry_type = instruction.get("entry_type", "Breakout")
            entry_type_abbrev = ENTRY_TYPE_ABBREV.get(
                entry_type, entry_type[:3].lower()
            )  # Breakout → bkt
            rev_action = "SELL" if action == "BUY" else "BUY"
            p_action = "BOT" if action == "BUY" else "SLD"
            s_action = "BOT" if rev_action == "BUY" else "SLD"

            # 计算辅助价 (StopLimit 专用：触碰即发单)
            timestamp = int(time_module.time())  # 秒级时间戳（避免毫秒重复风险）
            timestr = datetime.now(EASTERN_TZ).strftime("%H%M%S")
            safe_label = label.replace(" ", "_").replace("-", "_")[
                :5
            ]  # 清理特殊字符+截断
            safe_entry_type = entry_type_abbrev
            # --- 2. 意图备份 (决策层必须的锚点) ---
            self.tp1, self.tp2 = tp1, tp2
            self.tp1_qty = tp1_qty
            self.final_stop_price = round(sl, 2)
            self.last_trade_qty = qty
            self.state = "ORDER_SENT"
            self.order_place_time = time_module.time()

            # --- 3. 止损单逻辑处理 (SSOT 物理扫描) ---
            # 判定是否为加仓：快照显示已有持仓且柜台有活跃止损单
            active_stop = None
            if self.actual_filled_qty != 0:
                active_stop = next(
                    (
                        t
                        for t in self.ib.openTrades()
                        if t.contract.symbol == self.symbol
                        and t.order.orderType in ["STP", "STP LMT"]
                        and t.isActive()
                    ),
                    None,
                )
            if active_stop and active_stop.order.totalQuantity == 0:
                self.sys_log(
                    f"⚠️ [加仓异常] 检测到无效止损单(股数=0)，强制重建", level="WARN"
                )
                active_stop = None  # 回退到开新仓逻辑
            # --- 4. 构造主订单 ---
            p_order = LimitOrder(action, qty, lmt_price)
            p_order.algoStrategy = "Adaptive"
            p_order.algoParams = [TagValue("adaptivePriority", priority)]
            # ✅ 开新仓：父 transmit False，等子单带着一起发
            # ✅ 加仓：没有新子单挂钩父单，所以父必须 transmit True
            p_order.transmit = True if active_stop else False

            # ✅ 为主订单设置orderRef（开仓/加仓区分）
            # orderRef格式---开仓主单:E_{symbol}_{label[:6]}_{entry_type[:3]}_{hhmmss},E=Entry，时间戳秒级
            # orderRef格式---加仓主单:A_{symbol}_{label[:6]}_{entry_type[:3]}_{hhmmss},A=Add,
            # orderRef格式---新开仓配对的止损单:S_{symbol}_{label[:6]}_{entry_type[:3]}_{hhmmss},S=Stop
            # orderRef格式---加仓止损单保留原orderRef,IBKR禁止修改已提交订单的orderRef
            # orderRef格式---止盈单 TP1_{symbol} / TP2_{symbol}
            # 新格式：E_{symbol}_{label[:6]}_{entry_type[:3]}_{hhmmss}

            if active_stop:
                # 加仓单：A=Add
                p_order.orderRef = f"A_{self.symbol[:4]}_{safe_label[:5]}_{p_action[:3]}_{safe_entry_type[:3]}_{timestr[:6]}"[
                    :32
                ]
            else:
                # 开仓单：E=Entry
                p_order.orderRef = f"E_{self.symbol[:4]}_{safe_label[:5]}_{p_action[:3]}_{safe_entry_type[:3]}_{timestr[:6]}"[
                    :32
                ]

            self.sys_log(
                f"🔖 [订单标识] 主订单#{p_order.orderId},Ref: {p_order.orderRef}",
                level="DEBUG",
            )
            # 执行物理下单，立即拿到 trade 对象及其 OrderId
            current_active_trade = self.ib.placeOrder(self.contract, p_order)

            p_id = getattr(current_active_trade.order, "orderId", 0) or 0
            if p_id <= 0:
                await asyncio.sleep(0)  # 让出一个事件循环节拍，等待回填
                p_id = getattr(current_active_trade.order, "orderId", 0) or 0

            if p_id <= 0:
                p_id = (
                    getattr(p_order, "orderId", 0) or 0
                )  # 兜底：有时回填在 p_order 上

            if p_id <= 0:
                raise RuntimeError(
                    "IB orderId not assigned (p_id<=0), abort bracket to avoid orphan stop."
                )

            if active_stop:
                # A. 【加仓单状态】：调增现有止损单股数，并同步价格
                s_order = active_stop.order
                old_sl_qty = s_order.totalQuantity
                new_sl_qty = old_sl_qty + qty
                s_order.totalQuantity = new_sl_qty
                s_order.auxPrice = round(sl, 2)
                s_order.transmit = True
                self.ib.placeOrder(self.contract, s_order)
                # ✅ 日志增强：记录原止损单orderRef（用于追溯）
                self.initial_stop_price = s_order.auxPrice
                original_ref = getattr(s_order, "orderRef", "N/A")[:32]
                self.sys_log(
                    f"🧱 [止损保护]已有止损单#{s_order.orderId},Ref:{original_ref} | 止损股数{old_sl_qty}→{new_sl_qty}",
                    level="INFO",
                )
                self.sys_log(
                    f"📦[加仓单提交] ID:{p_order.orderId} | Ref:{p_order.orderRef} | {action} {qty}股 @ {lmt_price}",
                    level="INFO",
                )
                self.sys_log(
                    f"🛡️[止损单同步] ID:{s_order.orderId} | 原Ref:{original_ref} | {rev_action} {new_sl_qty}股 @ {s_order.auxPrice}",
                    level="INFO",
                )

            else:
                # B. 【开新仓状态】：新建随动止损单，挂钩主单 ID
                s_order = StopOrder(rev_action, qty, round(sl, 2))
                s_order.parentId = p_id
                s_order.transmit = True
                s_order.orderRef = f"S_{self.symbol[:4]}_{safe_label[:5]}_{s_action[:3]}_sto_{timestr}"[
                    :32
                ]  # S=Stop

                self.ib.placeOrder(self.contract, s_order)
                self.initial_stop_price = round(sl, 2)
                self.sys_log(
                    f"🛡️ [止损保护] 止损单#{s_order.orderId} 与主订单#{p_id}建立Bracket关系",
                    level="INFO",
                )
                self.sys_log(f"✅ [{label}] 开仓单和止损单已提交", level="INFO")
                self.sys_log(
                    f"📦[开仓单提交] ID:{p_order.orderId} | Ref:{p_order.orderRef} | {action} {qty}股 @ {lmt_price}",
                    level="INFO",
                )
                self.sys_log(
                    f"🛡️[止损单同步] ID:{s_order.orderId} | Ref:{s_order.orderRef} | {rev_action} {qty}股 @ {s_order.auxPrice}",
                    level="INFO",
                )

            if p_order.transmit:
                self.sys_log(
                    f"📡 [订单发送] 主单#{p_id} 独立发送给TWS (加仓订单)", level="DEBUG"
                )
            else:
                self.sys_log(
                    f"📡 [订单发送] 主单#{p_id} 与止损单#{s_order.orderId}同时发送给TWS (开新仓订单)",
                    level="DEBUG",
                )
            # --- 5. 【核心】原子化审计指纹刻录 ---
            # 这一步是 feed 给 _sync_position 的唯一真相源
            self._temp_order_audit = {
                "order_id": p_id,
                "label": label,
                "trigger_price": entry_ref,
                "last_p_lmt": p_order.lmtPrice,
                "last_s_aux": s_order.auxPrice,
            }

            # --- 6. 状态跳变与计时开始 ---

            self.filled_flag = True  # 摇铃，驱动下一秒进行物理确认

        except Exception as e:
            # 异常时逻辑自愈：回归待机，释放锁
            self.state = "OPEN_STAGE"
            if current_active_trade and current_active_trade.isActive():
                self.ib.cancelOrder(current_active_trade.order)
                self.sys_log(
                    "⚠️ [下单故障] 下单指令无法送达IBKR服务器，尝试撤回下单指令",
                    level="WARN",
                )
            self.is_processing_order = False
            self.sys_log(f"❌ [execute_trade 崩溃] 原因: {e}", level="ERROR")
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
            self.reset_context()

        finally:
            # 保证锁的释放
            await asyncio.sleep(0.1)
            self.is_processing_order = False

    async def clear_pos(self, snapshot):
        # ========== 拦截重复清仓指令 ==========
        # 检查是否已有活跃的 CL 平仓单（orderRef 以 "CL_" 开头）
        if snapshot.live_trades:
            for t in snapshot.live_trades:
                order_ref = getattr(t.order, "orderRef", "")
                if order_ref.startswith("CL_"):
                    self.sys_log(
                        f"⏭️ [清仓拦截] 已有活跃平仓单 Ref:{order_ref}，跳过本次强制清仓",
                        level="INFO",
                    )
                    return
        # 紧急清仓动作
        # --- 0. 肃清残留：发送新平仓指令前，先撤销所有可能存在的离场挂单 ---
        if snapshot.live_trades:
            for t in snapshot.live_trades:
                self.ib.cancelOrder(t.order)
            await asyncio.sleep(0.1)  # 短暂等待撤单指令发出
        # 从snapshot快照里面拉取最新持仓事实
        abs_pos = snapshot.abs_pos
        if abs_pos == 0:
            return
        qty = abs_pos
        action = "SELL" if snapshot.direction == "LONG" else "BUY"

        # 获取盘口价作为精算基准
        self.ib.reqMktData(self.contract, "", False, False)
        # 等待指定时间，或直到有数据
        start_t = time_module.time()
        ticker = None
        while time_module.time() - start_t < 5:
            await asyncio.sleep(0.5)
            ticker = self.live_ticker
            if t and ((ticker.last or 0) > 0 or (ticker.bid or 0) > 0):
                break
        self.ib.cancelMktData(self.contract)

        if ticker is None or (ticker.last is None and ticker.close is None):
            self.sys_log("⚠️ [强平执行] 无法获取有效行情，直接使用市价单", level="WARN")
            trigger_p = 0.0
        else:
            trigger_p = ticker.last if ticker.last > 0 else ticker.close

        # 拟定工单：优先尝试激进限价单，失败则上市价单
        timestr = datetime.now(EASTERN_TZ).strftime("%H%M%S")
        abv_action = "BOT" if action == "BUY" else "SLD"
        if ticker and (ticker.bid if action == "SELL" else ticker.ask):
            lmt_price = round(
                ticker.bid - 0.05 if action == "SELL" else ticker.ask + 0.05, 2
            )
            close_order = LimitOrder(action, qty, lmt_price)
            order_ref = f"CL_{self.symbol}_Close_{abv_action[:3]}_lmt_{timestr[:6]}"[
                :32
            ]  # Cl=Close（与收盘平仓单统一前缀）
        else:
            close_order = MarketOrder(action, qty)
            order_ref = f"CL_{self.symbol}_Close_{abv_action[:3]}_mkt_{timestr[:6]}"[
                :32
            ]  # Cl=Close（与收盘平仓单统一前缀）
        # ✅ 核心增强：为紧急平仓单设置唯一orderRef（与check_and_exit统一格式）
        close_order.orderRef = order_ref  # ✅ 关键：设置orderRef
        trade = self.ib.placeOrder(self.contract, close_order)
        order_id = close_order.orderId  # ✅ 直接从订单对象获取
        self.filled_flag = True
        # 记录强平审计快照 (对齐标准结构)
        self._temp_order_audit = {
            "order_id": trade.order.orderId,
            "label": "Close_Force",
            "trigger_price": trigger_p,
            "last_p_lmt": 0.0,
            "last_s_aux": 0.0,
        }
        # ✅ 增强日志：记录orderRef便于审计（与check_and_exit风格统一）
        self.sys_log(
            f"📦 [提交强制平仓单] ID:{order_id} | Ref:{order_ref} | "
            f"{action} {qty}股 @ 参考价:{trigger_p}",
            level="CRITICAL",  # 紧急清仓使用CRITICAL级别（高于INFO）
        )
        # 阻塞式等待成交 (最多等5 秒)
        wait_timer = 0
        while not trade.isDone() and wait_timer < 5:
            await asyncio.sleep(1)
            wait_timer += 1
        if not trade.isDone():
            self.sys_log(
                f"⏳ [强制平仓超时] OID:{order_id}(Ref:{order_ref}) 未完全成交",
                level="WARN",
            )
        else:
            self.sys_log(
                f"✅ [强制平仓完成] OID:{order_id}(Ref:{order_ref}) 已成交",
                level="INFO",
            )

    def _cancel_orders(self, snapshot, reason="timeout", is_adding=False):
        """
        [治理层] 安全撤单公共函数
        参数:
            is_adding: True=加仓单撤单（保留持仓状态）, False=入场单撤单（完全放弃交易）
        """
        if not snapshot.entry_orders:
            self.sys_log("🛡️ [撤单跳过] 无入场订单，无需撤单", level="DEBUG")
            return []
        cancelled_orders = []
        for o in snapshot.entry_orders:
            # ⚠️ 核心保护：已部分成交的订单禁止撤单
            if (
                getattr(o, "is_partially_filled", False)
                or self.order_fill_map.get(o.orderId, 0) > 0
            ):
                self.sys_log(
                    f"🛡️ [撤单拦截] OID:{o.orderId} 已部分成交({self.order_fill_map.get(o.orderId,0)}股)，跳过撤单",
                    level="WARN",
                )
                continue
            trade = next(
                (t for t in snapshot.live_trades if t.order.orderId == o.orderId), None
            )
            if trade and getattr(trade.orderStatus, "status", "") in (
                "Submitted",
                "PreSubmitted",
                "PendingSubmit",
            ):
                try:
                    self.ib.cancelOrder(o)
                    cancelled_orders.append(o.orderId)
                    self.order_fill_map.pop(o.orderId, None)
                    self.sys_log(
                        f"✅ [撤单] orderId={o.orderId} | qty={o.totalQuantity} | 原因:{reason}",
                        level="DEBUG",
                    )
                except Exception as e:
                    self.sys_log(
                        f"⚠️ [撤单失败] orderId={o.orderId}: {str(e)[:80]}",
                        level="ERROR",
                    )
                    self.sys_log(
                        f".StackTrace:\n{traceback.format_exc()}", level="DEBUG"
                    )

        # === 核心修正：根据场景差异化重置状态 ===
        if is_adding:
            # ✅ 加仓撤单：仅放弃加仓意图，回归纯持仓状态
            self.state = "HOLDING_STAGE"  # 保持持仓意图
            self.sys_log(
                f"🔄 [加仓撤单] 放弃加仓意图，回归持仓状态 | 原因:{reason}",
                level="INFO",
            )
        else:
            # 入场撤单：完全放弃交易意图
            self.state = "OPEN_STAGE"
            self.sys_log(
                f"🔄 [入场撤单] 完全放弃交易意图 | 原因:{reason}", level="INFO"
            )

        self.order_place_time = 0
        self._temp_order_audit = {}
        self.is_processing_order = False
        self.filled_flag = True  # 触发下一次快照清理

        # 兜底日志
        remaining = len(snapshot.entry_orders) - len(cancelled_orders)
        if remaining > 0:
            self.sys_log(
                f"🛡️ [兜底] {remaining}笔订单可能未取消，5秒内通过cond_02清理",
                level="WARN",
            )

    def _chase_order(self, snapshot: ContextSnapshot):
        """
        [治理层] 动能单追单公共函数 (同步修复版)
        核心改进：
        1. 保持同步函数签名 (def 而非 async def)
        2. 使用 ib.waitOnUpdate() 泵送消息队列
        3. 增加订单修改后的服务器确认步骤
        """
        # === 阶段 1：方向判定 ===
        if not snapshot.entry_orders:
            self.sys_log(
                "❌ [_chase_order] 无入场订单，无法判定交易方向", level="ERROR"
            )
            return False

        p_order = snapshot.entry_orders[0]
        is_buy = p_order.action == "BUY"
        side_str = "LONG" if is_buy else "SHORT"

        # === 阶段 2：主订单定位与状态预检 ===
        p_target_id = snapshot.main_order_id
        if not p_target_id:
            self.sys_log("❌ [_chase_order] 审计指纹缺失 order_id", level="ERROR")
            return False

        # 获取实时订单对象
        live_trades = self.ib.trades()
        live_trade = next(
            (t for t in live_trades if t.order.orderId == p_target_id), None
        )

        if not live_trade:
            self.sys_log(
                f"⚠️ [_chase_order] 无法找到活跃订单 OID:{p_target_id}", level="WARN"
            )
            return False

        # 再次确认订单状态
        if live_trade.orderStatus.status in [
            "Filled",
            "Cancelled",
            "Inactive",
            "PendingCancel",
        ]:
            self.sys_log(
                f"⚠️ [_chase_order] 订单已不可修改：{live_trade.orderStatus.status}",
                level="WARN",
            )
            return False

        if live_trade.orderStatus.status == "PartiallyFilled":
            self.sys_log(
                f"⚠️ [_chase_order] 订单已部分成交，跳过追单 | "
                f"已成交:{live_trade.orderStatus.filled} / 总量:{live_trade.order.totalQuantity}",
                level="WARN",
            )
            return False

        if live_trade.orderStatus.remaining <= 0:
            self.sys_log(f"⚠️ [_chase_order] 订单剩余数量为 0", level="WARN")
            return False

        # === 阶段 3：获取实时对手价 (关键修复) ===
        opp_price = None
        self.ib.reqMktData(self.contract, "", False, False)

        # ✅ 关键修复：使用 waitOnUpdate 让事件循环处理网络消息
        start_t = time_module.time()
        while time_module.time() - start_t < 2.0:
            ticker = self.ib.ticker(self.contract)
            # self.ib.waitOnUpdate(timeout=0.1)  # 等待 0.1 秒让 ticker 更新
            if ticker and ticker.ask > 0 and ticker.bid > 0:
                opp_price = ticker.ask if is_buy else ticker.bid
                self.sys_log(
                    f"🔍 [追单 - 查询 ticker] ask={ticker.ask:.3f} | bid={ticker.bid:.3f} | 耗时={round(time_module.time() - start_t, 2)}s",
                    level="DEBUG",
                )
                break

        self.ib.cancelMktData(self.contract)

        if opp_price is None:
            self.sys_log("❌ [_chase_order] 无法获取有效行情，放弃追单", level="ERROR")
            return False

        # === 阶段 4：价格计算 ===
        orig_p_lmt = live_trade.order.lmtPrice
        # 激进穿透：买 +0.08, 卖 -0.08
        new_p_lmt = round(opp_price + 0.08, 2) if is_buy else round(opp_price - 0.08, 2)

        # 常识检查
        if (is_buy and new_p_lmt <= orig_p_lmt) or (
            not is_buy and new_p_lmt >= orig_p_lmt
        ):
            self.sys_log(f"⚡ [追单拦截] 新价格{new_p_lmt} 无优势，跳过", level="DEBUG")
            return False

        # === 阶段 5：执行修改 ===
        old_price = live_trade.order.lmtPrice
        live_trade.order.lmtPrice = new_p_lmt

        try:
            # 提交修改
            self.ib.placeOrder(live_trade.contract, live_trade.order)

            # ✅ 关键修复：等待服务器确认修改成功
            verify_start = time_module.time()
            modified_success = False

            while time_module.time() - verify_start < 3.0:
                self.ib.waitOnUpdate(timeout=0.2)
                # 刷新本地 trade 对象状态
                current_trades = self.ib.trades()
                current_trade = next(
                    (t for t in current_trades if t.order.orderId == p_target_id), None
                )

                if not current_trade:
                    break

                # 检查价格是否真的变了
                if current_trade.order.lmtPrice == new_p_lmt:
                    modified_success = True
                    break
                # 检查是否被拒
                if current_trade.orderStatus.status in [
                    "Inactive",
                    "Cancelled",
                    "PendingCancel",
                ]:
                    self.sys_log(
                        f"❌ [追单被拒] 订单状态变为 {current_trade.orderStatus.status}",
                        level="ERROR",
                    )
                    return False

            if not modified_success:
                self.sys_log(
                    f"❌ [追单失败] 3 秒内未确认价格变更，可能服务器拒绝", level="ERROR"
                )
                return False

            self.sys_log(
                f"✅ [追单确认成功] OID:{p_target_id} | {old_price} → {new_p_lmt} | 对手价:{opp_price:.2f}",
                level="INFO",
            )
            return True

        except Exception as e:
            self.sys_log(f"❌ [追单异常] {e}", level="ERROR")
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
            return False

    def _chase_order_old(self, snapshot: ContextSnapshot):
        """
        [治理层] 动能单追单公共函数
        职责：执行激进价格追单 + 止损单风险间隙守恒同步
        适用场景：cond_06_02 (入场追单) / cond_08_02 (开仓追单)

        核心原则：方向判定基于物理订单事实 (order.action)，而非字符串猜测 (label)
        """
        # === 阶段1：方向判定（基于物理订单事实）===
        if not snapshot.entry_orders:
            self.sys_log(
                "❌ [_chase_order] 无入场订单，无法判定交易方向", level="ERROR"
            )
            return False

        # ✅ 黄金标准：直接读取订单的 action 字段（BUY/SELL）
        p_order = snapshot.entry_orders[0]  # 第一个入场单即主单
        is_buy = p_order.action == "BUY"
        side_str = "LONG" if is_buy else "SHORT"

        # === 阶段2：主订单定位 ===
        p_target_id = snapshot.main_order_id
        if not p_target_id:
            self.sys_log("❌ [_chase_order] 审计指纹缺失 order_id", level="ERROR")
            return False

        # 🔥 从 IBKR 实时获取最新订单，而非使用快照
        live_trades = self.ib.trades()
        live_trade = next(
            (t for t in live_trades if t.order.orderId == p_target_id), None
        )
        if not live_trade:
            order_ids = [t.order.orderId for t in live_trades]
            self.sys_log(
                f"⚠️ [_chase_order] 无法从 IBKR 获取实时订单 OID:{p_target_id} | 活跃订单：{order_ids}",
                level="ERROR",
            )
            return False
        # 🔥 订单状态检查（尽早排除不可修改的订单）
        order_status = live_trade.orderStatus.status
        remaining = live_trade.orderStatus.remaining
        if (
            order_status in ["Filled", "Cancelled", "Inactive", "PendingCancel"]
            or remaining <= 0
        ):
            self.sys_log(
                f"⚠️ [_chase_order] 订单状态{order_status} 或剩余{remaining}，不可修改",
                level="WARN",
            )
            return False

        # === 阶段3：通过IBKR读取最新一个ticker，找对手价 ===
        opp_price = None
        self.live_ticker = self.ib.reqMktData(self.contract, "", False, False)
        # 等待指定时间，或直到有数据
        start_t = time_module.time()
        while time_module.time() - start_t < 2:

            ticker = self.live_ticker
            if ticker and ticker.ask > 0 and ticker.bid > 0:
                opp_price = ticker.ask if is_buy else ticker.bid
                self.sys_log(
                    f"🔍 [追单-查询ticker] ticker.ask={ticker.ask:.3f} | ticker.bid={ticker.bid:.3f}|"
                    f"ticker.last={ticker.last:.3f} | ticker.close={ticker.close:.3f}"
                    f"查询等待时间 = {round(time_module.time() - start_t,2)}",
                    level="DEBUG",
                )
                break
        if ticker is None:
            self.sys_log("❌ [_chase_order] live_ticker无法获取行情", level="ERROR")
            return False
        self.ib.cancelMktData(self.contract)
        self.live_ticker = None

        # === 阶段4：价格穿透调优 ===
        orig_p_lmt = live_trade.order.lmtPrice

        # 价格穿透：对手价 + 0.05 缓冲区（确保吃掉盘口厚度）
        new_p_lmt = round(opp_price + 0.08, 2) if is_buy else round(opp_price - 0.08, 2)

        # ==== 阶段5： 检查计算出的追单价格是否符合常识，不符合就跳过本次追单
        if is_buy and new_p_lmt <= orig_p_lmt:
            self.sys_log(
                f"⚡ [追单拦截] 做多的追单价格{new_p_lmt:.2f}小于等于当前挂单价格{orig_p_lmt:.2f}，跳过本次追单",
                level="DEBUG",
            )
            return False
        elif not is_buy and new_p_lmt >= orig_p_lmt:
            self.sys_log(
                f"⚡ [追单拦截] 做空的追单价格{new_p_lmt:.2f}大于等于当前挂单价格{orig_p_lmt:.2f}，跳过本次追单",
                level="DEBUG",
            )
            return False
        # === 阶段6：主订单追单 ===
        # 🔥 记录修改前价格（用于日志审计）
        old_price = live_trade.order.lmtPrice
        live_trade.order.lmtPrice = new_p_lmt

        try:
            # 🔥 提交修改（保持 orderId 不变，保持 Bracket 关系）
            chase_trade = self.ib.placeOrder(live_trade.contract, live_trade.order)
            self.sys_log(
                f"✅ [追单提交] OID:{p_target_id} | "
                f"原限价lmt:{old_price} → 新限价lmt:{new_p_lmt} | 对手价:{opp_price:.2f}",
                level="INFO",
            )

        except Exception as e:
            self.sys_log(
                f"❌ [追单修改失败] OID:{p_target_id} | 错误:{e}",
                level="ERROR",
            )
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
            return False

        # === 阶段7：审计指纹更新 ===
        self._temp_order_audit.update({"last_p_lmt": new_p_lmt})

        # === 阶段8：成功日志 ===
        self.sys_log(
            f"⚡ [追单提交TWS] {side_str} |对手价:{opp_price:.2f} → 订单新入场价:{new_p_lmt:.2f} | ",
            level="WARN",
        )

        return True

    def _sync_position(self, snapshot: ContextSnapshot):
        """
        [大脑中枢] 矩阵式对账引擎 (全息日志版)
        架构原则：特征匹配与处理逻辑 1:1 挂钩，全量输出物理态势日志。
        职责：通过 12 种互斥 Condition 象限以及象限之下的二级分类，识别物理现状，并下达精准治理指令。
        """
        # ======================================================================
        # --- 第 0 部分：解析基础的维度参数数据 ---
        # ======================================================================
        # --- 0.0 基础对账维度提取 (雷达参数) ---
        self.last_cond = getattr(self, "current_cond", "Cond_01_IDLE")
        self.current_cond = "Cond_Unknown"
        intent = self.state  # 内存意图：OPEN_STAGE / ORDER_SENT / HOLDING_STAGE
        has_pos = snapshot.has_position  # 物理存在性：True(有仓), False(空仓)
        abs_pos = snapshot.abs_pos  # 物理持仓量
        side = snapshot.direction
        # c_entry: 所有入场/加仓方向的挂单数量， 是订单的数量，不是股票数量
        c_entry = len(snapshot.entry_orders)
        # c_closing: 离场单订单的总数 (止损 + 止盈)
        c_closing = len(snapshot.closing_orders)
        count = c_entry + c_closing  # 柜台总活跃单据数
        has_orders = count > 0
        # --- 0.x 成交窗口二次确认（防 openTrades 空窗误判裸奔） ---
        last_exec_ts = getattr(self, "last_exec_ts", 0)
        recent_exec = (time_module.time() - last_exec_ts) <= getattr(
            self, "exec_window_sec", 6.0
        )  # 5秒一拍，给6秒缓冲
        # stop_qty: 所有止损单的总股数 (STP / STP LMT)
        stop_qty = sum(
            abs(o.totalQuantity)
            for o in snapshot.closing_orders
            if o.orderType in ["STP", "STP LMT"]
        )

        # tp_qty: 显式统计平仓类单据 (LMT 止盈 或 MKT 紧急平仓)
        # 逻辑：只要是 LMT 或 MKT 的离场单，我们就认为它是在“看守”获利目标的单子
        tp_qty = sum(
            abs(o.totalQuantity)
            for o in snapshot.closing_orders
            if o.orderType in ["LMT", "MKT"]
        )

        # --- 0.1 时间与标签审计 (执法刻度) ---
        # 计算指令发出后的生存时长：time_module.time() 是当前物理时间，order_place_time 是下单瞬间的时间锚点

        p_time = getattr(self, "order_place_time", 0) if intent == "ORDER_SENT" else 0
        if p_time > 0:
            elapsed = time_module.time() - p_time
        else:
            elapsed = 0  # 逻辑安全点：无下单则无耗时
        active_timing = p_time > 0 and intent == "ORDER_SENT"
        p_f_time = getattr(self, "partially_filled_time", 0)
        if p_f_time > 0:
            p_f_elapsed = time_module.time() - p_f_time
        else:
            p_f_elapsed = 0

        # --- 0.2 策略性格识别与同步宽限期 (执法分级) ---
        # 动能型信号特征：此类信号追求“破位即成交”，对排队容忍度极低 (10s)
        # 🔥 新逻辑：从 orderRef 解析的 entry_type 判断（唯一真理源）
        order_id = snapshot.main_order_id
        label = snapshot.main_order_label or "Unknown"
        entry_type = snapshot.main_order_entry_type or "Unknown"

        # 基于 entry_type 判断订单性格
        is_momentum = entry_type in ["Breakout"]  # 动能单
        is_pullback = entry_type in ["GiftZone", "Pullback"]  # 回调单
        is_reversal = entry_type in ["Reversal"]  # 🔥 新增：反转单
        # 🔥 降级兼容：如果 entry_type 解析失败，用 label 兜底
        # 降级兼容
        if entry_type == "Unknown":
            is_momentum = any(k in label for k in ["Elephant", "V180", "Law1", "Law8"])
            is_pullback = any(k in label for k in ["Law#3", "Law3", "Pullback"])
            is_reversal = any(
                k in label for k in ["Reversal", "Law2", "Law6"]
            )  # 🔥 新增

        # ========================================================================================
        # --- 第 1 部分：信号探测 (定义 意图/持仓/挂单 三个维度一共12个互斥象限，以及下面的二级分类) ---
        # =========================================================================================
        cond_01 = (
            intent == "OPEN_STAGE" and not has_pos and not has_orders
        )  # 无意图，无头寸，无在途订单  ---标准待机
        cond_02 = (
            intent == "OPEN_STAGE" and not has_pos and has_orders
        )  # 无意图，无头寸，有在途订单  ---可能是外部认为挂单
        cond_03 = (
            intent == "OPEN_STAGE" and has_pos and not has_orders
        )  # 无意图，有头寸，无在途订单  ---僵尸持仓
        cond_04 = (
            intent == "OPEN_STAGE" and has_pos and has_orders
        )  # 无意图，有头寸，有在途订单  ---意图与实际错位，需要再细分情况

        cond_05 = (
            intent == "ORDER_SENT" and not has_pos and not has_orders
        )  # 意图:已下单， 无头寸，无在途订单   --- 意图丢失

        cond_06 = (
            intent == "ORDER_SENT" and not has_pos and has_orders
        )  # 意图:已下单， 无头寸，有在途订单   --- 正常入场挂单
        cond_06_01 = (
            cond_06 and c_entry == 1 and elapsed <= 60 and not snapshot.has_partial_fill
        )  # 主订单发出后的10秒等候成交时间
        cond_06_02 = (
            cond_06
            and c_entry == 1
            and elapsed > 60
            and elapsed <= 90
            and not snapshot.has_partial_fill
        )  # 主订单发出后的25秒等候成交时间
        cond_06_03 = (
            cond_06
            and c_entry == 1
            and elapsed > 90
            and elapsed <= 120
            and not snapshot.has_partial_fill
        )  # 主订单发出后的 45秒等候成交时间
        cond_06_04 = (
            cond_06 and c_entry == 1 and elapsed > 120 and not snapshot.has_partial_fill
        )  # 主订单提交已经超过45秒未成交，撤单
        cond_06_partial = cond_06 and snapshot.has_partial_fill
        cond_06_05 = (
            cond_06 and c_entry > 1 and not snapshot.has_partial_fill
        )  # 后台出现 2个以上的主订单，异常情况，报错
        cond_06_06 = cond_06 and not any(
            [
                cond_06_01,
                cond_06_02,
                cond_06_03,
                cond_06_04,
                cond_06_05,
                cond_06_partial,
            ]
        )  # 意想不到的状况,报警

        cond_07 = (
            intent == "ORDER_SENT" and has_pos and not has_orders
        )  # 意图:已下单， 有头寸，无在途订单   --- 刚成交，意图还没更改
        # cond_07 这种情况，首先把self.state 改成"HOLDING_STAGE",然后 树立起 filled_flag, 然后需要追加止损单保护头寸"

        # cond_08 意图:已下单，有头寸，有在途订单（加仓/开仓象限）
        cond_08 = intent == "ORDER_SENT" and has_pos and has_orders

        # --- 8-A: ⚡ 极端风险：加仓裸奔（止损单消失）---
        cond_08_naked_push = cond_08 and c_entry > 0 and stop_qty == 0

        # --- 8-B: 🏗️ 标准加仓：加仓单在途且防线完备 ---
        cond_08_normal_push = cond_08 and c_entry > 0 and stop_qty > 0

        # 【关键优化】按时间阈值分层（与 cond_06 完全对齐）
        cond_08_01 = cond_08_normal_push and elapsed <= 60  # 60秒黄金撮合期
        cond_08_02 = (
            cond_08_normal_push and elapsed > 60 and elapsed <= 90
        )  # 10-25秒：动能单追单
        cond_08_03 = (
            cond_08_normal_push and elapsed > 90 and elapsed <= 120
        )  # 25-45秒：回调单撤单
        cond_08_04 = cond_08_normal_push and elapsed > 120  # >120秒：强制撤单
        cond_08_partial = cond_08 and snapshot.has_partial_fill

        # --- 8-C: ⚖️ 成交纠偏：加仓单刚成交，进入股数对账期 ---
        cond_08_fill_sync = cond_08 and c_entry == 0  # 主单已成交，仅剩止损单

        cond_08_05 = cond_08_fill_sync and stop_qty < abs_pos  # 止损缺口 → 纠偏
        cond_08_06 = cond_08_fill_sync and stop_qty > abs_pos  # 止损过量 → 纠偏
        cond_08_07 = cond_08_fill_sync and stop_qty == abs_pos  # 完美对齐 → 转正 ✅

        # --- 8-D: 🚨 未定义状态兜底 ---
        cond_08_08 = cond_08 and not any(
            [
                cond_08_naked_push,
                cond_08_01,
                cond_08_02,
                cond_08_03,
                cond_08_04,
                cond_08_05,
                cond_08_06,
                cond_08_07,
            ]
        )
        # --- cond_09：意图持仓，无头寸，无订单 ---
        # 逻辑：账户已清空，但内存 state 还没来得及 reset
        cond_09 = intent == "HOLDING_STAGE" and not has_pos and not has_orders
        # 这种状态下，通常直接执行 self.reset_context() 即可

        # --- cond_10：意图持仓，无头寸，有在途订单 ---
        # 逻辑：头寸可能被止损/手动平仓了，但柜台还残留着之前的保护单或开仓单
        cond_10 = (
            intent == "HOLDING_STAGE" and not has_pos and has_orders
        )  # 意图:持仓，无头寸，有在途订单  --- 已清仓，还有挂单，意图也未更改，需要cancel 残留的挂单

        # --- cond_11：意图持仓，有头寸，无在途订单 (🚨 绝对裸奔区) ---
        # 逻辑：这就是我们之前讨论的“绝对孤儿”，没有任何保护，没有任何进攻
        cond_11 = (
            intent == "HOLDING_STAGE" and has_pos and not has_orders
        )  # 意图:持仓，有头寸，无在途订单  --- 有持仓，无加仓单，无止盈单，也无止损单
        # 这种情况在 manage_position 中直接触发后补一个止损单 1.0*ATR，或者离场。

        # ✅ 真裸奔：无单 + 无partial_fill + 最近也没有成交回报
        cond_11_01 = cond_11 and (not snapshot.has_partial_fill) and (not recent_exec)
        # ✅ 成交/分拆窗口：无单但（partial_fill 或 最近成交过）
        cond_11_02 = cond_11 and (snapshot.has_partial_fill or recent_exec)

        # --- cond_12：意图持仓，有头寸，有在途订单 (核心治理区) ---
        # 逻辑：系统正常运行的主要区域，需要精细化对账
        cond_12 = intent == "HOLDING_STAGE" and has_pos and has_orders

        # 12-A：止损单状态 (基于 stop_qty)
        cond_12_01 = (
            cond_12 and stop_qty == 0
        )  # 有持仓有单，但止损单缺失（可能是只有止盈或只有加仓）
        cond_12_02 = (
            cond_12 and stop_qty > 0 and stop_qty < abs_pos
        )  # 止损单股数不足 (缺口)
        cond_12_03 = (
            cond_12 and stop_qty > 0 and stop_qty > abs_pos
        )  # 止损单股数过多 (过量)

        # 12-B：止盈单状态 (基于 tp_qty)
        cond_12_04 = cond_12 and tp_qty > 0  # 止盈单正在护航中

        # 12-C：稳态判定
        cond_12_05 = cond_12 and stop_qty == abs_pos  # 止损完全覆盖，标准稳态
        cond_12_06 = (
            cond_12 and stop_qty == abs_pos and tp_qty > 0
        )  # 止损止盈全方位覆盖

        # 12-D：加仓单干预 (如果在 HOLDING 阶段又触发了加仓逻辑)
        cond_12_07 = cond_12 and c_entry > 0  # 持仓期间有新的加仓单在排队

        # ======================================================================
        # --- 第 2 部分：外科手术式治理 (每种 Condition 独立代码块，全量日志输出) ---
        # ======================================================================

        # 2.A 部分  ---  OPEN_STAGE 治理 (待机与自愈) ---
        if cond_01:
            self.current_cond = "cond_01"
            self.filled_flag = False
            self.chase_flag = False
            # 稳态待机：不采取任何物理动作
        elif cond_02:
            self.current_cond = "cond_02"
            if self.current_cond != self.last_cond:
                self.sys_log(
                    f"⚠️ [Cond_02] 发现残留挂单(Count={count})，执行强制清理...",
                    level="WARN",
                )
            for t in snapshot.live_trades:
                self.ib.cancelOrder(t.order)
            self.filled_flag = False
            # 对后台的活跃的挂单进行撤单操作，发出指令，结果要等到下一次(大约5秒之后)take_snapshot的时候，再来查看
        elif cond_03:  # --- 状态 03：僵尸持仓 (无意图，有头寸，无订单) ---
            self.current_cond = "cond_03"
            if self.current_cond != self.last_cond:
                self.sys_log(
                    f"🚨 [Cond_03] 僵尸持仓报警：发现未知头寸({abs_pos}股)，立即启动紧急平仓并归位！",
                    level="CRITICAL",
                )
            # 树立起 filled_flag, 但是在本函数内不做操作，交给manage_position函数去调用 clear_pos()函数清仓
            self.filled_flag = True
            self.is_exiting = True

        elif cond_04:  # --- 状态 04：失控持仓 (无意图，有头寸，有挂单) --
            self.current_cond = "cond_04"
            if self.current_cond != self.last_cond:
                self.sys_log(
                    f"🚨 [Cond_04] 系统失控报警：无意图但有仓({abs_pos}股)且有单({count})！执行清场手术",
                    level="CRITICAL",
                )
            # 1. 第一步：肃清战场。撤销柜台所有非法挂单 (止损/止盈/加仓)
            # 只有清空了挂单，我们才能确保紧急平仓单的 Margin 和股数是安全的
            for t in snapshot.live_trades:
                self.ib.cancelOrder(t.order)
            # 2. 树立起 filled_flag, 但是在本函数内不做操作，交给manage_position函数去调用 clear_pos()函数清仓
            self.filled_flag = True
            self.is_exiting = True

        # --- 2.B 部分 ：ORDER_SENT 治理 (推进与转正) ---
        elif cond_05:
            self.current_cond = "cond_05"
            if self.current_cond != self.last_cond:
                self.sys_log(
                    f"♻️ [Cond_05] 意图丢失(无单无仓)，执行逻辑复位", level="WARN"
                )
            self.reset_context()
            self.filled_flag = False

        elif cond_06:  # 入场挂单象限
            if cond_06_01:
                self.current_cond = "cond_06_01"
                if self.current_cond != self.last_cond:
                    self.sys_log(
                        f"⏱️ [Cond_06_01] Entry_Type={entry_type}的订单提交{int(elapsed)}秒，耐心等待",
                        level="WARN",
                    )
                self.filled_flag = False

            elif cond_06_02:
                self.current_cond = "cond_06_02"  # 订单已经提交 >60秒 但是 <=90秒
                if (
                    is_momentum
                ):  # 如果是动能单，就修改订单价格，激进入场。 不是动能单就继续等待。
                    if not self.chase_flag:
                        self.sys_log(
                            f"⚡ [Cond_06_02] Entry_Type={entry_type}，动能订单提交已经{int(elapsed)}秒，改价格追单",
                            level="WARN",
                        )
                        self._chase_order(snapshot)
                        self.chase_flag = True
                    self.filled_flag = False
                else:  # 不是动能订单，就继续等候
                    if self.current_cond != self.last_cond:
                        self.sys_log(
                            f"⏱️ [Cond_06_02] Entry_Type={entry_type},非动能单,订单提交{int(elapsed)}秒，耐心等待",
                            level="WARN",
                        )
                    self.filled_flag = False

            elif (
                cond_06_03
            ):  # 主订单已经提交超过 25秒，但是还不到 45秒。 如果是pullback单或者reversal但，就撤单。其余类型订单继续等候
                self.current_cond = "cond_06_03"

                if is_pullback or is_reversal:
                    if self.current_cond != self.last_cond:
                        self.sys_log(
                            f"⏱️ [Cond_06_03] Entry_Type={entry_type}，订单提交已经{int(elapsed)}秒，撤单",
                            level="WARN",
                        )
                    self._cancel_orders(
                        snapshot,
                        reason="pullback or reversal订单超时",
                        is_adding=False,
                    )
                    self.filled_flag = False
                else:  # ✅ 动能单和其他订单继续等到 45 秒
                    if self.current_cond != self.last_cond:
                        self.sys_log(
                            f"⏱️ [Cond_06_03]Entry_Type={entry_type}，订单提交{int(elapsed)}秒，耐心等待",
                            level="WARN",
                        )
                    self.filled_flag = False
            elif cond_06_04:  # # 主订单已经提交超过 45秒，无论什么类型的订单一律撤单
                self.current_cond = "cond_06_04"
                elapsed_sec = int(elapsed)
                if self.current_cond != self.last_cond:
                    self.sys_log(
                        f"⏱️ [Cond_06_04]Entry_Type={entry_type}， 订单已经提交已经{int(elapsed)}秒，信号 {label} 超时，执行撤单)",
                        level="WARN",
                    )
                self._cancel_orders(snapshot, reason="超时强制撤单")
                self.filled_flag = False
            elif cond_06_partial:
                self.current_cond = "cond_06_partial"
                self.sys_log(
                    f"⏱️ [Cond_06_partial]开仓单在快照时刻，有部分成交 "
                    f"OID:{snapshot.partially_filled_orders[0].order.orderId} "
                    f"已成交{snapshot.partially_filled_orders[0].orderStatus.filled}股/"
                    f"订单总手数{snapshot.partially_filled_orders[0].order.totalQuantity}股 | "
                    f"部分成交发生在:{int(elapsed)}秒之前",
                    level="INFO",
                )
                # 不撤单，仅延长观察期
                if p_f_elapsed > 10:  # 上一笔部分成交过去已经超过10秒
                    self.sys_log(
                        "⚠️ [开仓单部分成交超时] 开仓单部分成交，剩余部分已经过了{elapsed}秒还未成交，尝试撤单剩余量",
                        level="WARN",
                    )
                    self._cancel_orders(
                        snapshot, reason="部分成交，剩余订单超时未成交，强制撤单"
                    )
                    self.filled_flag = False
                else:
                    # 部分成交，耐心等待
                    self.sys_log(
                        "⚠️ [部分成交] 上笔部分成交之后已经{p_f_elapsed}秒，继续等待剩余订单成交",
                        level="WARN",
                    )
                    self.filled_flag = False
            elif cond_06_05:  # 后台出现 2个以上的主订单，异常情况，报错
                self.current_cond = "cond_06_05"
                self.sys_log(
                    f"🚨  [Cond_06_05] 后台快照显示有({c_entry})个入场订单，请检查TWS order窗口",
                    level="ERROR",
                )
                try:
                    for t in snapshot.live_trades:
                        o = t.order
                        self.sys_log(
                            f"🔎 [Cond_06_05-Debug] "
                            f"orderId={o.orderId}, "
                            f"action={o.action}, "
                            f"type={o.orderType}, "
                            f"parentId={o.parentId}, "
                            f"qty={o.totalQuantity}",
                            level="DEBUG",
                        )
                    self.filled_flag = False
                except Exception as e:
                    self.sys_log(
                        f"⚠️ [Cond_06_05-Debug] 打印订单信息失败: {e}", level="ERROR"
                    )
            elif cond_06_06:
                self.current_cond = "cond_06_06"
                self.sys_log(
                    f"⚠️ [Cond_06_06] 探测到未定义的入场挂单状态组合,NOT 06_01/02/03/04/06",
                    level="ERROR",
                )
                try:
                    for t in snapshot.live_trades:
                        o = t.order
                        self.sys_log(
                            f"🔎 [Cond_06_06-Debug] "
                            f"orderId={o.orderId}, "
                            f"action={o.action}, "
                            f"type={o.orderType}, "
                            f"parentId={o.parentId}, "
                            f"qty={o.totalQuantity}",
                            level="DEBUG",
                        )
                    self.filled_flag = False
                except Exception as e:
                    self.sys_log(
                        f"⚠️ [Cond_06_06-Debug] 打印订单信息失败: {e}", level="ERROR"
                    )
        elif cond_07:
            self.current_cond = "cond_07"
            self.sys_log(
                f"🏗️ [Cond_07] 入场单成交瞬间！开始确权与身份转正", level="INFO"
            )
            # 核心转正动作
            self.state = "HOLDING_STAGE"
            self.actual_filled_qty = abs_pos
            self.avg_fill_price = snapshot.avg_cost
            self.position_side = snapshot.direction
            self.last_trade_qty = abs_pos
            self.order_place_time = 0
            self.filled_flag = True  # 🚨 摇铃：让 manage_position 立即补上止损单

        elif cond_08:  # 开仓象限治理
            # === 8-A: 裸奔加仓，没有止损单保护===
            if cond_08_naked_push:
                self.current_cond = "cond_08_naked"
                self.sys_log(
                    f"🚨 [Cond_08] 加仓裸奔风险！立刻摇铃补防", level="CRITICAL"
                )
                self.filled_flag = True

            # === 8-B: 标准加仓单在途处理（按时间分层）===
            elif cond_08_01:  # 加仓单提交 <60秒
                self.current_cond = "cond_08_01"
                self.filled_flag = False
                # 正常等待：10秒黄金撮合期内不干扰

            elif cond_08_02:  # 加仓单提交 60-90秒
                self.current_cond = "cond_08_02"
                self.filled_flag = False
                if is_momentum:  # 仅动能单追单
                    if not self.chase_flag:
                        self.sys_log(
                            f"⚡ [Cond_08_02] Entry_Type={entry_type}，动能加仓提交({int(elapsed)}秒)，执行追单",
                            level="WARN",
                        )
                        self._chase_order(snapshot)
                        self.chase_flag = True
                else:
                    if self.current_cond != self.last_cond:
                        self.sys_log(
                            f"⏱️ [Cond_08_02] Entry_Type={entry_type}，非动能加仓提交{int(elapsed)}秒，耐心等待",
                            level="WARN",
                        )

            elif cond_08_03:  # 加仓单提交 90-120秒
                self.current_cond = "cond_08_03"
                if is_pullback or is_reversal:  # Pullback 和 reversal订单撤单
                    self.sys_log(
                        f"⏱️ [Cond_08_03] Entry_Type={entry_type}，加仓提交已经{int(elapsed)}秒，执行撤单",
                        level="WARN",
                    )
                    self._cancel_orders(
                        snapshot, reason="加仓单提交超时未成交", is_adding=True
                    )  # ✅ 加仓场景
                    self.filled_flag = False
                else:
                    if self.current_cond != self.last_cond:
                        self.sys_log(
                            f"⏱️ [Cond_08_03] Entry_Type={entry_type}， 加仓单提交{int(elapsed)}秒，继续等待",
                            level="WARN",
                        )
                    self.filled_flag = False

            elif cond_08_04:  # 加仓单超过 120秒，如果没有部分成交，就撤单
                self.current_cond = "cond_08_04"
                elapsed_sec = int(elapsed)
                if snapshot.has_partial_fill:
                    self.sys_log(
                        f"⏳ [加仓单部分成交]  Entry_Type={entry_type}， 已成交{snapshot.partially_filled_orders[0].orderStatus.filled}股，"
                        f"剩余{snapshot.partially_filled_orders[0].orderStatus.remaining}股继续等待",
                        level="INFO",
                    )
                    self.filled_flag = False
                else:
                    self.sys_log(
                        f"⏱️ [Cond_08_04] Entry_Type={entry_type}， 开仓或加仓单超时{elapsed_sec}秒，强制撤单",
                        level="WARN",
                    )
                    self._cancel_orders(snapshot, reason="45s超时(加仓强制)")
                    self.filled_flag = False

            elif cond_08_partial:  # 加仓单部分成交
                self.current_cond = "cond_08_partial"
                self.sys_log(
                    f"⏱️ [Cond_08_partial] 加仓单在快照时刻，有部分成交 "
                    f"OID:{snapshot.partially_filled_orders[0].order.orderId} "
                    f"已成交{snapshot.partially_filled_orders[0].orderStatus.filled}股/"
                    f"订单总手数{snapshot.partially_filled_orders[0].order.totalQuantity}股 | "
                    f"部分成交已经过了:{p_f_elapsed}秒",
                    level="INFO",
                )
                # 不撤单，仅延长观察期
                if p_f_elapsed > 10:  # 上一笔部分成交过去已经超过10秒
                    self.sys_log(
                        "⚠️ [加仓单部分成交超时] 剩余部分已经{p_f_elapsed}秒还未成交，尝试撤单剩余量",
                        level="WARN",
                    )
                    self._cancel_orders(
                        snapshot,
                        reason="加仓单部分成交，剩余订单超{p_f_elapsed}秒未成交，强制撤单",
                    )
                    self.filled_flag = False
                else:
                    # 部分成交，耐心等待
                    self.sys_log(
                        "⚠️ [加仓单部分成交等候] 加仓单部分成交之后刚过{p_f_elapsed}秒，继续等待剩余订单成交",
                        level="WARN",
                    )
                    self.filled_flag = False
            # === 8-C: 成交纠偏处理（股数对账）===
            elif cond_08_05:
                self.current_cond = "cond_08_05"
                self.sys_log(
                    f"⚖️ [Cond_08_05] Entry_Type={entry_type}，开仓或加仓成交，止损不足({stop_qty} < {abs_pos})，补齐止损",
                    level="INFO",
                )
                self.filled_flag = True  # 摇铃触发纠偏

            elif cond_08_06:
                self.current_cond = "cond_08_06"
                self.sys_log(
                    f"⚖️ [Cond_08_06] Entry_Type={entry_type}， 开仓或加仓成交，止损过量({stop_qty} > {abs_pos})，削减止损",
                    level="INFO",
                )
                self.filled_flag = True  # 摇铃触发纠偏

            elif cond_08_07:
                self.current_cond = "cond_08_07"
                self.state = "HOLDING_STAGE"
                self.actual_filled_qty = abs_pos
                self.avg_fill_price = snapshot.avg_cost
                self.position_side = snapshot.direction
                self.last_trade_qty = abs_pos
                self.order_place_time = 0
                self.partially_filled_time = 0
                self.holding_start_time = time_module.time()
                self.filled_flag = False  # 摇铃复位
                self.chase_flag = False
                # === 加仓后止盈处理 ===
                if self.tp1_filled:  # 之前 TP1 已成交，现在是加仓
                    # 1. 撤销所有现存止盈单（通过遍历 snapshot.closing_orders）
                    for o in snapshot.closing_orders:
                        if getattr(o, "orderRef", "").startswith(("TP1_", "TP2_")):
                            self.ib.cancelOrder(o)
                            self.sys_log(f"🧹 [加仓] 撤销旧止盈单 {o.orderId}")
                    # 2. 重新计算止盈价位（沿用原步长）
                    original_step = abs(self.tp2 - self.tp1)  # 保存原步长
                    self.tp1 = self.tp2  # 原TP2 → 新TP1
                    self.tp2 = round(
                        self.tp1
                        + (
                            original_step
                            if snapshot.direction == "LONG"
                            else -original_step
                        ),
                        2,
                    )
                    self.sys_log(
                        f"⚖️ [Cond_08_07] 加仓订单成交，止损单股数与持仓完美对齐({stop_qty}={abs_pos})，转正至HOLDING_STAGE",
                        level="INFO",
                    )
                    self.sys_log(
                        f"🔄 [Cond_08_07]加仓后对止盈价格重置,原TP2({self.tp1:.2f})→新TP1 | 新TP2={self.tp2:.2f} (步长={original_step:.2f})",
                        level="DEBUG",
                    )
                    # 3. 重置 tp1_filled（新头寸的 TP1 尚未成交）
                    self.tp1_filled = False
                else:  # 之前的TP1没有成交，应该是新开仓，self.tp1_filled == False

                    self.sys_log(
                        f"⚖️ [Cond_08_07] 开仓订单成交，止损单股数与持仓完美对齐({stop_qty}={abs_pos})，转正至HOLDING_STAGE",
                        level="INFO",
                    )
                    self.sys_log(
                        f"🔄 [Cond_08_07] 开仓订单初始止盈价位 TP1:{self.tp1:.2f} TP2:{self.tp2:.2f} (无需重置)",
                        level="DEBUG",
                    )
            # === 8-D: 未定义状态兜底 ===
            elif cond_08_08:
                self.current_cond = "cond_08_08"
                self.sys_log(
                    f"⚠️ [Cond_08_08] 未定义加仓状态组合，执行强制撤单", level="ERROR"
                )
                self._cancel_orders(snapshot, reason="异常状态强制撤单")
                self.filled_flag = False
                try:
                    for t in snapshot.live_trades:
                        o = t.order
                        self.sys_log(
                            f"🔎 [Cond_08_08-Debug] orderId={o.orderId}, action={o.action}, "
                            f"type={o.orderType}, parentId={o.parentId}, qty={o.totalQuantity}",
                            level="DEBUG",
                        )
                except Exception as e:
                    self.sys_log(
                        f"⚠️ [Cond_08_08-Debug] 打印订单失败: {e}", level="ERROR"
                    )
        # --- 2.C 部分：HOLDING_STAGE 治理 (守护与对账) ---
        elif cond_09:
            self.current_cond = "cond_09"
            self.sys_log(f"🏁 [Cond_09] 持仓已结清，内存变量归位", level="INFO")
            self.state = "OPEN_STAGE"
            self.reset_context()
            self.filled_flag = False
            self.chase_flag = False

        elif cond_10:
            self.sys_log(
                f"🧹 [Cond_10] 发现无头寸但仍有挂单残留，开始逐单清场 | 订单数:{len(snapshot.live_orders)}",
                level="WARN",
            )
            for o in snapshot.live_orders:
                ref = getattr(o, "orderRef", "") or ""
                if ref.startswith("E_"):
                    order_type = "入场订单"
                elif ref.startswith("S_"):
                    order_type = "止损订单"
                elif ref.startswith("ES_"):
                    order_type = "紧急止损订单"
                elif ref.startswith("TP1_"):
                    order_type = "止盈订单TP1"
                elif ref.startswith("TP2_"):
                    order_type = "止盈订单TP2"
                elif ref.startswith("CL_"):
                    order_type = "清仓订单"
                else:
                    order_type = "未知类型订单"

                oid = getattr(o, "orderId", "N/A")
                self.sys_log(
                    f"🧹 [Cond_10] 发现残留的{order_type}，执行Cancel取消动作 | ID:{oid} | Ref:{ref[:32]}",
                    level="WARN",
                )
                self.ib.cancelOrder(o)

            self.filled_flag = False

        elif cond_11:
            if cond_11_01:  # HOLDING_STAGE, 有持仓,没有active order,也没有部分成交
                self.current_cond = "cond_11_01"
                self.sys_log(
                    f"🚨 [Cond_11_01] 绝对孤儿单探测！无任何防护单，立即补救",
                    level="CRITICAL",
                )
                # 状态同步
                self.actual_filled_qty = abs_pos
                self.avg_fill_price = snapshot.avg_cost
                # 摇铃：由接下来的逻辑根据 ATR 补齐 final_stop_price 并下单
                self.filled_flag = True
            elif (
                cond_11_02
            ):  # HOLDING_STAGE, 有持仓,没有active order,但是状态是有部分成交，我们等待，不操作
                self.current_cond = "cond_11_02"
                self.sys_log(
                    f"⏳ [Cond_11_02] 无在途订单但检测到成交窗口：partial_fill={snapshot.has_partial_fill} | recent_exec={recent_exec}，暂不补单",
                    level="WARN",
                )
                self.filled_flag = False

        # --- 状态 12：意图持仓，有头寸，有在途订单 (核心治理区) ---
        elif cond_12:
            # 1. 基础事实同步（无论处于哪个子状态，物理事实先更新）
            self.actual_filled_qty = abs_pos
            self.avg_fill_price = snapshot.avg_cost
            self.position_side = snapshot.direction

            # 2. 精细化子状态手术分流
            # [12_07] 加仓单监控（持仓期间的新进攻意图）
            if cond_12_07:
                self.current_cond = "cond_12_07"
                if self.current_cond != self.last_cond:
                    self.sys_log(
                        f"🏗️ [Cond_12_07] 进攻：持仓中且加仓单正在排队", level="DEBUG"
                    )
                self.filled_flag = False

            # [12_01] 有单但无止损：属于严重防御缺失（可能只有止盈或加仓挂单）
            elif cond_12_01:
                self.current_cond = "cond_12_01"
                self.sys_log(
                    f"🚨 [Cond_12_01] 持仓中防御单缺失！(只有止盈或加仓单)，立即摇铃补防",
                    level="CRITICAL",
                )
                self.filled_flag = True

            # [12_02] 止损不足：股数缺口
            elif cond_12_02:
                self.current_cond = "cond_12_02"
                self.sys_log(
                    f"⚖️ [Cond_12_02] 止损股数不足：持仓 {abs_pos} vs 止损 {stop_qty}，准备纠偏",
                    level="WARN",
                )
                self.filled_flag = True

            # [12_03] 止损过量：冗余风险
            elif cond_12_03:
                self.current_cond = "cond_12_03"
                self.sys_log(
                    f"⚖️ [Cond_12_03] 止损股数过量：持仓 {abs_pos} vs 止损 {stop_qty}，准备削减",
                    level="WARN",
                )
                self.filled_flag = True

            # [12_06] 特等稳态：止损对齐 + 止盈护航
            elif cond_12_06:
                self.current_cond = "cond_12_06"
                # 稳态不摇铃，仅做减仓事实探测
                if abs_pos <= (self.last_trade_qty * 0.7) and not self.tp1_filled:
                    self.tp1_filled = True
                    if self.current_cond != self.last_cond:
                        self.sys_log(
                            f"🔑 [Cond_12_06] 稳态：止损+止盈全方位护航中", level="INFO"
                        )
                self.filled_flag = False

            # [12_05] 标准稳态：止损完全对齐
            elif cond_12_05:
                self.current_cond = "cond_12_05"
                if self.current_cond != self.last_cond:
                    self.sys_log(
                        f"🔑 [Cond_12_05] 稳态---止损单vs持仓 1:1 覆盖中", level="INFO"
                    )
                self.filled_flag = False
                if abs_pos <= (self.last_trade_qty * 0.7) and not self.tp1_filled:
                    self.tp1_filled = True

            # [12_04] 仅止盈监控（作为 05/06 的补充审计）
            elif cond_12_04:
                self.current_cond = "cond_12_04"
                if self.current_cond != self.last_cond:
                    self.sys_log(f"🛡️ [Cond_12_04] 止盈单在位巡航", level="DEBUG")
                self.filled_flag = False
            # 异常边界哨兵
            else:
                self.current_cond = "cond_12_unhandled"
                self.sys_log(
                    f"❓ [Cond_12] 探测到未定义子状态组合，维持现状", level="ERROR"
                )
                self.filled_flag = False
        # ======================================================================
        # --- 第 3 部分：公共清理 (不受手术影响的底层清理) ---
        # ======================================================================
        # 3.1 入场锁释放：只有当物理柜台确认没有入场方向的挂单时，才允许解开下单保护锁
        if c_entry == 0:
            if self.is_processing_order:
                # self.sys_log("🔓 [清理] 柜台入场单已清空，释放 is_processing_order 锁", level="DEBUG")
                self.is_processing_order = False

        # 3.2 出场锁释放：只有当物理柜台确认没有平仓/止盈方向的挂单时，才允许解开平仓保护锁
        if c_closing == 0:
            if self.is_exiting:
                # self.sys_log("🔓 [清理] 柜台平仓单已清空，释放 is_exiting 锁", level="DEBUG")
                self.is_exiting = False

        # 3.3  审计补充动作：更新最新快照引用，供其他函数（如 _submit_tp）查询物理事实
        if self.current_cond != self.last_cond:
            self._save_cond_log(self.current_cond)

        self.latest_snapshot = snapshot
        return self.current_cond

    async def add_stop(self, snapshot: ContextSnapshot):
        """
        [手术刀-补建防线]
        职责：在没有任何保护的情况下，基于物理成本和 ATR 建立第一道确定性止损防线。
        """
        try:
            # --- 1. 物理股数与方向确权 ---
            qty = snapshot.abs_pos
            if qty <= 0:
                return

            # --- 2. 确定性价格计算 (SSOT: 物理成本 + 波动率) ---
            # 强制使用 1.0 * ATR 作为安全垫，不依赖任何内存旧价格
            atr_buffer = 1.0 * self.effective_atr

            # 以物理快照中的成交均价为计算基准
            if snapshot.direction == "LONG":
                target_price = snapshot.avg_cost - atr_buffer
            else:
                target_price = snapshot.avg_cost + atr_buffer

            # 格式化价格
            target_price = round(target_price, 2)

            # --- 3. 构造与提交订单 ---
            action = "SELL" if snapshot.direction == "LONG" else "BUY"
            abv_action = "BOT" if action == "BUY" else "SLD"
            # ✅ 核心增强：为紧急止损单设置唯一orderRef

            timestr = datetime.now(EASTERN_TZ).strftime("%H%M%S")

            order_ref = f"ES_{self.symbol}_EmStp_{abv_action[:3]}_sto_{timestr[:6]}"[
                :32
            ]  # ES=Emergency Stop
            # 独立止损单，不绑定 parentId
            new_stop = StopOrder(action, qty, target_price)
            new_stop.orderRef = order_ref  # ✅ 关键：设置orderRef
            # --- 4. 物理执行与意图转正 ---
            # 物理加锁：防止在订单确认前产生重复指令
            self.is_exiting = True
            self.ib.placeOrder(self.contract, new_stop)
            self.final_stop_price = target_price

            self._temp_order_audit.update(
                {
                    "label": "Emergency_AddStop",
                    "last_p_lmt": 0,  # 止损单无主单限价
                    "last_s_aux": target_price,
                }
            )
            # ✅ 增强日志：记录orderRef便于审计
            self.sys_log(
                f"🛡️ [补防执行] 提交独立止损单 | Ref:{order_ref} | "
                f"{action} {qty}股 @ {target_price} | 意图转正为 ORDER_SENT",
                level="WARN",
            )

        except Exception as e:
            self.sys_log(f"❌ [add_stop] 补单异常: {e}", level="ERROR")

    async def manage_position(self, snapshot: ContextSnapshot):
        """
        [治理层-中央调度 V7.5]
        职责：接收 _sync_position 诊断标签，执行异步物理动作。
        优先级：1. 紧急清场 > 2. 补防 > 3. 纠偏 > 4. 稳态维护
        """
        # --- 1. 环境准入与实时价格抓取 ---

        if not self.ib.isConnected():
            return
        if self.is_exiting and snapshot.abs_pos == 0:
            return

        # 统一获取当前价格（用于阶段 B 探测）
        if (
            hasattr(self, "bars_reference")
            and self.bars_reference
            and len(self.bars_reference) > 0
        ):
            bar = self.bars_reference[-1]
            # 🔥 根据持仓方向选择价格：做空用 Low，做多用 High
            if snapshot.direction == "SHORT":
                current_price = bar.high  # ✅ 做空：用最高价(high)衡量上冲风险
            else:
                current_price = bar.low  # ✅ 做多：用最低价(low)衡量下探风险
        else:  # 没有5秒bar
            self.sys_log(
                f"[管理仓位]当前没有5秒bar的数据，无法获得current_price,放弃这一轮manage_position",
                level="INFO",
            )
            return

        price_valid = current_price is not None and current_price > 0
        # ======================================================================
        # --- 大厅 A：【防御治理手术室】 (物理对账与生存保障) ---
        # 核心逻辑：凡是涉及“改变物理单据数量”的动作，执行后立即 return 等待下一拍对账。
        # ======================================================================
        # 1. 紧急清场 (最高优先级)
        # [判定] 针对 cond_11_01 (绝对裸奔) 的空间二次审计：若无保护且已亏损过大，视为致命伤
        is_broken_orphan = False

        if price_valid and self.current_cond == "cond_11_01":
            risk_threshold = 1.0 * self.effective_atr
            is_long_broken = snapshot.direction == "LONG" and current_price <= (
                self.avg_fill_price - risk_threshold
            )
            is_short_broken = snapshot.direction == "SHORT" and current_price >= (
                self.avg_fill_price + risk_threshold
            )
            if is_long_broken or is_short_broken:
                is_broken_orphan = True

        # 触发清场的 Cond 分布：
        # cond_03: [僵尸持仓] 无意图、有头寸、无订单
        # cond_04: [系统失控] 无意图、有头寸、有残留挂单
        # Broken_Orphan: [裸奔破位] 有头寸、无止损、现价已穿透 1.0*ATR
        if self.current_cond in ["cond_03", "cond_04"] or is_broken_orphan:
            reason = "Broken_Orphan" if is_broken_orphan else self.current_cond
            self.sys_log(
                f"🔥 [手术A-紧急平仓] 触发原因: {reason}，立即市价清场",
                level="CRITICAL",
            )
            await self.clear_pos(snapshot)
            self.filled_flag = False
            return  # 清仓结束之后，立刻从当前函数返回，不再操心下面的其他事宜

        # 2. 补建防线 (第二优先级)
        # 触发补单的 Cond 分布：
        # cond_07: [成交瞬间] 入场单刚 fill，state 未转正前发现的保护空白
        # cond_11: [绝对裸奔] 正常持仓期间，所有止损/止盈单据离奇消失
        # cond_12_01: [防御缺失] 柜台有止盈或加仓单，但唯独缺失止损单
        if (
            self.current_cond in ["cond_07", "cond_11_01", "cond_12_01"]
        ) and snapshot.abs_pos > 0:
            self.sys_log(
                f"🛡️ [手术B-补建止损单] 诊断标签: {self.current_cond}", level="WARN"
            )
            await self.add_stop(snapshot)
            self.filled_flag = False
            return  # 提交完补建的止损单之后，也从本函数返回，不再操心下面的其他事宜

        # ======================================================================
        # --- 阶段 B：【防线纠偏手术】 (第二优先级：对账) ---
        # 针对场景：手里有单，但“股数”对不上（通常发生在加仓成交后或减仓未同步时）。
        # ======================================================================

        # 触发纠偏的 Cond 分布：
        # cond_08_naked: [开仓裸奔] 正在开仓但原有止损单消失 (此时必须补上新股数的止损)
        # cond_08_05: [开仓成交-缺口] 开仓后止损股数 < 物理总持仓
        # cond_08_06: [开仓成交-过量] 开仓后止损股数 > 物理总持仓
        # cond_12_02: [持仓对账-缺口] 稳态持仓中发现止损股数不足
        # cond_12_03: [持仓对账-过量] 稳态持仓中发现止损股数多余
        need_sync_vol = any(
            c in self.current_cond
            for c in [
                "cond_08_naked",
                "cond_08_05",
                "cond_08_06",
                "cond_12_02",
                "cond_12_03",
            ]
        )
        last_exec_ts = getattr(self, "last_exec_ts", 0)
        recent_exec = (time_module.time() - last_exec_ts) <= getattr(
            self, "exec_window_sec", 6.0
        )
        is_excess_stop = self.current_cond in ("cond_08_06", "cond_12_03")
        # is_gap_stop = self.current_cond in ("cond_08_05", "cond_12_02")

        if (self.filled_flag or need_sync_vol) and snapshot.abs_pos > 0:

            # ✅ 仅在“需要纠偏股数”的cond下拦截（避免止损/止盈执行窗口改数量）
            if need_sync_vol and is_excess_stop and recent_exec:
                self.sys_log(
                    f"⏳ [手术C-延迟进行止损数量的纠偏] cond={self.current_cond} | recent_exec={recent_exec} | "
                    f"pos={snapshot.abs_pos}，成交窗口内暂不改止损单数量，等待下一拍",
                    level="WARN",
                )
                self.filled_flag = False
                return

            self.sys_log(
                f"⚖️ [手术C-纠偏止损单里的股数] 诊断标签: {self.current_cond}，同步股数至 {snapshot.abs_pos}",
                level="INFO",
            )
            await self._update_stop(
                price=self.final_stop_price, volume=snapshot.abs_pos, snapshot=snapshot
            )
            self.filled_flag = False
            return

        # ======================================================================
        # --- 大厅 C：【收益治理巡航厅】 (结算、追踪与止盈) ---
        # 只有在物理对账“稳态”或“终局”时才进入。
        # ======================================================================

        if price_valid and snapshot.abs_pos > 0:  # 只要有持仓事实，就无条件开启止盈扫描
            # (1) 追踪止损：微调价格 (非物理股数变动，不 return，允许继续探测止盈)
            suggested_sl = await self._trailing_stop(current_price, snapshot)
            if suggested_sl is not None and suggested_sl > 0:
                # 追踪属于价格维护，不设 force，内部有 0.05 步长保护
                # self.sys_log(
                #    f"准备调用_update_Stop函数调整止损价格，suggested_sl=={suggested_sl}",
                #    level="DEBUG",
                # )
                await self._update_stop(
                    price=suggested_sl,
                    volume=snapshot.abs_pos,
                    force=False,
                    snapshot=snapshot,
                )

            # (2) 止盈探测：触碰判定
            await self._detect_take_profit(current_price, snapshot)
        self.filled_flag = False
        return self.current_cond

    def take_snapshot(self):
        try:
            # --- 1. 获取持仓镜像 ---
            positions = self.ib.positions()
            tws_p = next(
                (p for p in positions if p.contract.symbol == self.symbol), None
            )
            fact_pos = tws_p.position if tws_p else 0.0
            fact_avg_cost = tws_p.avgCost if tws_p else 0.0

            # --- 2. 获取活跃 Trade 镜像 ---
            # 拿到的是 Trade 对象，它包裹着 Order
            live_trades = [
                t
                for t in self.ib.openTrades()
                if t.contract.symbol == self.symbol and t.isActive()
            ]

            # --- 3. 封装并返回 ---
            # snapshot 内部会自动生成 live_orders, entry_orders 和 closing_orders
            snapshot = ContextSnapshot(
                fact_pos=fact_pos, avg_cost=fact_avg_cost, live_trades=live_trades
            )

            return snapshot
        except Exception as e:
            self.sys_log(
                f"❌ [take_snapshot] IBKR订单和持仓拍照失败。报错代码: {e}",
                level="ERROR",
            )
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
            return None

    def reset_context(self):
        """
        [系统级复位 - V5.0 物理对账版]
        职责：彻底清理物理挂单，并回归 OPEN_STAGE。不再依赖内存影子变量。
        """
        try:
            self.sys_log(
                f"♻️ [全量复位] 启动。正在清理物理残存并重置状态机...", level="INFO"
            )

            # 1. 物理单据全量清理 (SSOT：直接对柜台开刀)
            open_trades = self.ib.openTrades()
            for t in open_trades:
                if t.contract.symbol == self.symbol and t.isActive():
                    self.sys_log(
                        f"🧹 [清理] 撤销柜台残留单(ID:{t.order.orderId} 类型:{t.order.orderType})",
                        level="WARN",
                    )
                    self.ib.cancelOrder(t.order)

            # 2. 核心状态机复位
            self.state = "OPEN_STAGE"
            self.entry_law = None
            self.position_side = ""

            # 3. 核心标志位复位 (必须保留，用于控制节拍)
            self.tp1_filled = False
            self.is_processing_order = False
            self.is_exiting = False

            # 4. 影子账本销账 (仅保留算法必须的价位缓存)
            self.tp1 = 0.0
            self.tp2 = 0.0
            self.current_tp_cond = "None"
            self.last_tp_cond = "None"

            self.initial_stop_price = 0.0
            self.final_stop_price = 0.0
            self.avg_fill_price = 0.0
            self.actual_filled_qty = 0
            self.last_trade_qty = 0
            self._loss_recorded_orders.clear()
            self.currrent_stop_cond = ""
            self.currrent_tp_cond = ""
            self.last_stop_cond = "IDLE"
            self.last_tp_cond = "IDLE"

            # 5. 清理审计属性 (回归初始模板)
            self._temp_order_audit = {
                "order_id": 0,
                "label": "IDLE",
                "trigger_price": 0.0,
                "last_p_lmt": 0.0,
                "last_s_aux": 0.0,
            }

            self.latest_snapshot = None
            self.order_place_time = 0  # ✨ 补充：时间锚点必须归零
            self.partially_filled_time = 0
            self.holding_start_time = 0
            self.order_fill_map.clear()
            # 🔥🔥🔥【新增】Law1 大象柱关键价位复位
            self.law1_elephant_high = 0.0
            self.law1_elephant_low = 0.0
            self.law1_elephant_open = 0.0
            self.law1_elephant_close = 0.0

            # 🔥🔥🔥【新增】orderRef 解析结果复位
            self.order_features.clear()

            # 6. 法则旗语清理
            self._reset_all_ready_flags()

            self.sys_log(
                "✅ [交易完成] 状态机回归 OPEN_STAGE,self.is_exiting = False,self.is_processing_order = False。",
                level="INFO",
            )

        except Exception as e:
            self.state = "OPEN_STAGE"
            self.is_processing_order = False
            self.is_exiting = False
            self.sys_log(f"❌ [reset_context] 严重异常: {e}", level="ERROR")

    async def check_and_exit(self):
        """
        [系统层-收盘执行器 V5.0 物理清场版]
        职责：终盘强制清场，完全基于柜台事实执行撤单与平仓，不再依赖任何内存影子变量。
        """
        self.sys_log(
            f"🚨 [Market Close] 收到收盘指令，开始物理清场 {self.symbol}...",
            level="Schedule",
        )

        # 1. 物理撤单：强制排空柜台所有挂单 (SSOT原则)
        try:
            open_trades = await self.ib.reqOpenOrdersAsync()
            my_active_trades = [
                t
                for t in open_trades
                if t.contract.symbol == self.symbol and t.isActive()
            ]

            if my_active_trades:
                self.sys_log(
                    f"🧹 [清理挂单] 发现IBKR有 {len(my_active_trades)} 笔在途挂单，正在强制撤销...",
                    level="INFO",
                )
                for t in my_active_trades:
                    self.ib.cancelOrder(t.order)
                await asyncio.sleep(1)  # 给柜台物理撤单留出通讯时间
            else:
                self.sys_log(f"✅ [清理挂单] IBKR后台已经无活跃挂单。", level="DEBUG")
        except Exception as e:
            self.sys_log(f"⚠️ 撤单过程异常: {e}", level="ERROR")
            self.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")

        # 2. 物理平仓：基于 ib.positions() 事实执行 3 次强平尝试
        for attempt in range(3):
            # 实时拉取最新持仓事实
            positions = await self.ib.reqPositionsAsync()
            pos = next((p for p in positions if p.contract.symbol == self.symbol), None)

            if not pos or pos.position == 0:
                self.sys_log(
                    f"🎉 [清场对账] 确认 {self.symbol} 账户已空。", level="INFO"
                )
                break

            # 确定物理动作 (基于正负号，不看 position_side)
            action = "SELL" if pos.position > 0 else "BUY"
            qty = abs(pos.position)

            # 获取盘口价作为精算基准
            self.live_ticker = self.ib.reqMktData(self.contract, "", False, False)
            await asyncio.sleep(1)
            ticker = self.live_ticker
            if ticker is None:
                self.sys_log(
                    "⚠️ [强平执行] live_ticker 未初始化，无法获取盘口价，直接使用市价单",
                    level="WARN",
                )
                # 此时 ticker 保持 None，后续限价单分支将跳过
            trigger_p = (
                ticker.last
                if (ticker and ticker.last > 0)
                else (ticker.close if ticker else 0)
            )
            # ✅ 核心增强：生成收盘平仓单orderRef（32字符内）

            timestr = datetime.now(EASTERN_TZ).strftime("%H%M%S")
            abv_action = "BOT" if action == "BUY" else "SLD"
            # 拟定工单：优先尝试激进限价单，失败则上市价单
            if ticker and (ticker.bid if action == "SELL" else ticker.ask):
                lmt_price = round(
                    ticker.bid - 0.05 if action == "SELL" else ticker.ask + 0.05, 2
                )
                order_ref = (
                    f"CL_{self.symbol}_Close_{abv_action[:3]}_lmt_{timestr[:6]}"[:32]
                )  # CL=Close
                close_order = LimitOrder(action, qty, lmt_price)
            else:
                order_ref = (
                    f"CL_{self.symbol}_Close_{abv_action[:3]}_mkt_{timestr[:6]}"[:32]
                )  # CL=Close
                close_order = MarketOrder(action, qty)
            self.sys_log(
                f"📡 [强平执行] 准备尝试第 {attempt+1} 次平仓 |生成订单Ref:{order_ref} | 实仓:{pos.position} | 参考价:{trigger_p}",
                level="INFO",
            )
            close_order.orderRef = order_ref  # ✅ 关键：设置orderRef
            # 物理发射 (不再绑定回调，靠 filled_flag 触发下一节拍)
            trade = self.ib.placeOrder(self.contract, close_order)
            self.filled_flag = True

            # 记录强平审计快照 (对齐标准结构)
            self._temp_order_audit = {
                "order_id": trade.order.orderId,
                "label": "MKT_Close",
                "trigger_price": trigger_p,
                "last_p_lmt": 0.0,
                "last_s_aux": 0.0,
            }

            self.sys_log(
                f"📦 [强平单提交] ID:{trade.order.orderId} | Ref:{order_ref} | {action} {qty}股",
                level="INFO",
            )
            # 阻塞式等待成交 (最多等 10 秒)
            wait_timer = 0
            while not trade.isDone() and wait_timer < 5:
                await asyncio.sleep(2)
                wait_timer += 1

            if not trade.isDone():
                self.sys_log(
                    f"⏳ [强平超时] OID:{trade.order.orderId} 未完全成交，准备重试...",
                    level="WARN",
                )
        # self.ib.cancelMktData(self.contract)
        # 2.5 日终财务对账：确认空仓后，阻塞等待并批量落盘
        positions = await self.ib.reqPositionsAsync()
        pos = next((p for p in positions if p.contract.symbol == self.symbol), None)
        if (not pos) or (pos.position == 0):
            await self._end_of_day_reconcile(wait_seconds=15.0)
        else:
            self.sys_log(
                f"⚠️ [日终对账跳过] {self.symbol} 仍有持仓 {pos.position}，不执行对账",
                level="WARN",
            )

        # 3. 终点审计
        # self.save_trade_logs()
        self.state = "OPEN_STAGE"
        self.sys_log(
            f"🏁 [财务闭环] {self.symbol} 状态机锁定为OPEN_STAGE, 退出程序。",
            level="INFO",
        )

    def on_commission_report(self, trade, fill, commission_report):
        exec_id = commission_report.execId
        commission = commission_report.commission
        realized_pnl = commission_report.realizedPNL

        if not hasattr(self, "settlement_ledger"):
            self.settlement_ledger = {}
        if not hasattr(self, "exec_id_map"):
            self.exec_id_map = {}
        if not hasattr(self, "pending_commission"):
            self.pending_commission = {}

        self.sys_log(
            f"📊 [佣金报告回调] ExecId:{exec_id[:30]}... | "
            f"佣金:{commission:.2f} | PnL:{realized_pnl:.2f}",
            level="INFO",
        )

        oid = self.exec_id_map.get(exec_id)
        if not oid or oid not in self.settlement_ledger:
            self.pending_commission[exec_id] = {
                "commission": commission,
                "realized_pnl": realized_pnl,
                "report": commission_report,  # 或只存需要的字段
            }
            self.sys_log(
                f"⏳ [佣金报告暂存] ExecId:{exec_id[:30]}... 订单尚未创建，暂存待处理",
                level="DEBUG",
            )
            return

        state = self.settlement_ledger[oid]
        self._apply_commission_to_state(state, exec_id, commission, realized_pnl)
        asyncio.create_task(self._try_settle_order(oid))

    def _apply_commission_to_state(self, state, exec_id, commission, realized_pnl):
        """将佣金数据应用到订单结算状态，处理去重和状态标记"""
        if exec_id in state.commission_exec_ids:
            self.sys_log(
                f"⏭️ [佣金重复应用忽略] ExecId:{exec_id[:30]}... | OID:{state.order_id}",
                level="DEBUG",
            )
            return

        state.commission_exec_ids.add(exec_id)
        state.total_commission += commission
        if state.is_closing_order:
            state.total_realized_pnl += realized_pnl

        # 检查佣金是否已全覆盖
        if (
            state.is_order_done
            and len(state.exec_ids) > 0
            and len(state.commission_exec_ids) >= len(state.exec_ids)
        ):
            state.is_commission_ready = True
            self.sys_log(
                f"✅ [佣金报告全覆盖] OID:{state.order_id} | 佣金 exec:{len(state.commission_exec_ids)}/{len(state.exec_ids)} | 总佣金:{state.total_commission:.2f}",
                level="INFO",
            )
        else:
            self.sys_log(
                f"⏳ [佣金报告收集中...] OID:{state.order_id} | 佣金 exec:{len(state.commission_exec_ids)}/{len(state.exec_ids)} | 订单完成:{state.is_order_done}",
                level="DEBUG",
            )

    async def _try_settle_order(self, oid):
        """
        检查是否满足"订单完成"且"佣金到位"，满足则执行最终记账
        """
        # 稍微等待一下，确保数据同步
        await asyncio.sleep(0.1)
        if not hasattr(self, "order_fill_map"):
            self.order_fill_map = {}
        if not hasattr(self, "seen_exec_ids_by_oid"):
            self.seen_exec_ids_by_oid = {}

        if oid not in self.settlement_ledger:
            return

        state = self.settlement_ledger[oid]

        # 条件 1: 订单必须全部成交
        if not state.is_order_done:
            return

        # 🔥 新增：动态刷新一次佣金覆盖状态（防 race condition）
        if (
            state.is_order_done
            and len(state.exec_ids) > 0
            and len(state.commission_exec_ids) >= len(state.exec_ids)
        ):
            state.is_commission_ready = True

        # 条件 2: 佣金报告已收到 OR 超时 fallback
        if not state.is_commission_ready:
            # 如果还没收到佣金，启动超时检查
            t = getattr(state, "_timeout_task", None)
            if t is None or t.done():
                state._timeout_task = asyncio.create_task(
                    self._settlement_timeout_checker(oid)
                )
            return

        # ✅ 满足所有条件，执行最终财务记账
        if not state.settled:
            await self._finalize_financials(oid)
            state.settled = True
            t = getattr(state, "_timeout_task", None)
            if t and not t.done():
                t.cancel()
            # 清理内存
            # 🔥 新增：结算完成后，才清理去重集合和成交映射 (防止翻倍)
            self.order_fill_map.pop(oid, None)
            self.seen_exec_ids_by_oid.pop(oid, None)
            self.settlement_ledger.pop(oid, None)
            # 清理 exec_id_map
            for eid in state.exec_ids:
                self.exec_id_map.pop(eid, None)

    async def _settlement_timeout_checker(self, oid):
        """
        超时保护：如果 15 秒后佣金报告还没到，强制结算
        """
        await asyncio.sleep(15.0)
        if oid not in self.settlement_ledger:
            return
        if not hasattr(self, "order_fill_map"):
            self.order_fill_map = {}
        if not hasattr(self, "seen_exec_ids_by_oid"):
            self.seen_exec_ids_by_oid = {}

        state = self.settlement_ledger.get(oid)
        if state and not state.settled and state.is_order_done:
            self.sys_log(
                f"⚠️ [结算超时] OID:{oid} 未收到佣金报告，启用本地估算", level="WARN"
            )
            state.is_commission_ready = True  # 强制标记为就绪
            state.settled_by_timeout = True  # 🔥 新增：标记为超时结算
            await self._finalize_financials(oid)
            state.settled = True

            # 🔥 新增：超时结算完成后，也要清理
            self.order_fill_map.pop(oid, None)
            self.seen_exec_ids_by_oid.pop(oid, None)
            self.settlement_ledger.pop(oid, None)

            for eid in state.exec_ids:
                self.exec_id_map.pop(eid, None)

    async def _finalize_financials(self, oid):
        """
        唯一出口：在此处打印财务日志，确保与 TWS 订单窗口一致
        """
        if oid not in self.settlement_ledger:
            return

        state = self.settlement_ledger[oid]
        # 🔥 修复：计算真实的加权平均成交价
        if state.filled_qty > 0:
            final_avg_price = state.total_value / state.filled_qty
        else:
            final_avg_price = state.avg_price  # 兜底

        # 1. 数据校准
        if not state.is_closing_order:
            final_pnl = 0.0
            logic_source = "IBKR_Comm_Only"
        else:
            # 🔥 修复：只有超时未收到 IBKR 数据时，才使用本地估算
            # 这样就不会把“真实 PnL=0"的交易误判为缺失数据
            if state.settled_by_timeout:
                # 本地估算逻辑 (fallback)
                if state.side == "BOT":  # 平仓买入
                    final_pnl = (state.avg_cost - final_avg_price) * state.filled_qty
                else:  # 平仓卖出
                    final_pnl = (final_avg_price - state.avg_cost) * state.filled_qty
                logic_source = "Local_Estimated"
            else:
                final_pnl = state.total_realized_pnl
                logic_source = "IBKR_Realized"
        # 2. 打印最终财务日志 (只打印一次)
        action_symbol = "🟢" if final_pnl > 0 else ("🔴" if final_pnl < 0 else "⚪")
        cov = f"{len(state.commission_exec_ids)}/{len(state.exec_ids)}"
        self.sys_log(
            f"💰 [财务最终对账] OID:{oid} | {state.side} {state.filled_qty} @ {final_avg_price:.2f} | "
            f"PnL:{final_pnl:.2f} | Comm:{state.total_commission:.2f} | 来源:{logic_source} | "
            f"status={getattr(state,'last_status','UNKNOWN')} | timeout={state.settled_by_timeout} | cov={cov}",
            level="INFO",
        )

        # 3. 调用原有的 log_trade 记录详细流水
        self.log_trade(
            time_str=datetime.now()
            .astimezone(EASTERN_TZ)
            .strftime("%Y-%m-%d %H:%M:%S"),
            action=state.side,
            qty=state.filled_qty,
            price=final_avg_price,  # 🔥 使用计算后的加权均价
            realized_pnl=final_pnl,
            commission=state.total_commission,
            exec_id=state.exec_ids[0] if state.exec_ids else "N/A",
            label=getattr(state, "label", "UNKNOWN"),
            order_id=state.order_id,
            order_ref=state.order_ref,
        )

    async def _end_of_day_reconcile(self, wait_seconds: float = 15.0):
        """
        日终清仓后对账：等待 IBKR 推送佣金/PNL，然后把 settlement_ledger 里未结算的订单统一落盘。
        设计目标：不依赖 isDone()/status 实时信号；只在日终做一次阻塞式收口。
        """
        if not hasattr(self, "settlement_ledger"):
            self.settlement_ledger = {}
        if not hasattr(self, "exec_id_map"):
            self.exec_id_map = {}
        if not hasattr(self, "order_fill_map"):
            self.order_fill_map = {}
        if not hasattr(self, "seen_exec_ids_by_oid"):
            self.seen_exec_ids_by_oid = {}
        if not hasattr(self, "pending_commission"):
            self.pending_commission = {}
        if not self.settlement_ledger:
            self.sys_log(
                "🧾 [日终对账] settlement_ledger 为空，无需处理", level="DEBUG"
            )
            return

        # 1) 先等一等，让 commissionReport / realizedPNL 尽量到齐
        self.sys_log(
            f"🧾 [日终对账] 等待 {wait_seconds:.1f}s 收集佣金/PNL...", level="INFO"
        )
        await asyncio.sleep(wait_seconds)

        # 2) 第一轮：对已经满足条件的，走正常结算通道
        oids = list(self.settlement_ledger.keys())
        for oid in oids:
            state = self.settlement_ledger.get(oid)
            if not state:
                continue

            # 日终兜底：把“订单已结束”视为 True（清仓后不再纠结状态机）
            state.is_order_done = True

            # 尽量走你现有的 _try_settle_order（它会自行判断 commission_ready + timeout_task）
            await self._try_settle_order(oid)

        # 3) 第二轮：仍未结算的，强制落盘（不再启动 timeout_task，不再 sleep）
        remaining = list(self.settlement_ledger.keys())
        if not remaining:
            self.sys_log("✅ [日终对账] 已全部完成结算", level="INFO")
            return

        self.sys_log(
            f"⚠️ [日终对账] 仍有 {len(remaining)} 笔未结算，启用强制落盘（无佣金则按本地估算）",
            level="WARN",
        )

        for oid in remaining:
            state = self.settlement_ledger.get(oid)
            if not state or state.settled:
                continue

            # 强制结算口径：视作 timeout 结算
            state.is_order_done = True
            state.is_commission_ready = True
            state.settled_by_timeout = True

            await self._finalize_financials(oid)
            state.settled = True

            # ——清理（与 timeout checker 同口径）——
            self.order_fill_map.pop(oid, None)
            self.seen_exec_ids_by_oid.pop(oid, None)
            self.settlement_ledger.pop(oid, None)

            for eid in state.exec_ids:
                self.exec_id_map.pop(eid, None)
        if self.pending_commission:
            self.sys_log(
                f"⚠️ [日终对账] 清理未匹配佣金报告 {len(self.pending_commission)} 条",
                level="DEBUG",
            )
        self.pending_commission.clear()
        self.sys_log("✅ [日终对账] 强制落盘完成", level="INFO")
