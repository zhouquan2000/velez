# ======================================================================
# events.py - 精确导入清单 (修正版)
# ======================================================================

# --- 1. 标准库 ---
import asyncio
import time as time_module
import traceback
from datetime import (
    datetime,
    time,
    timedelta,
)  # 🔥 添加 time 和 timedelta，移除 timezone
from dataclasses import dataclass, field

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
from .shared import (
    sys_log,
    EASTERN_TZ,
    contexts_placeholder,
    standardize_df,
)
from . import shared


# ======================================================================
# 新增：订单结算状态容器 (放在文件顶部，所有函数之前)
# ======================================================================
@dataclass
class OrderSettlementState:
    """
    用来暂存订单的成交和佣金信息，直到全部成交才记账
    """

    order_id: int
    order_ref: str
    total_qty: float
    filled_qty: float = 0.0
    total_value: float = 0.0  # 🔥 新增：用于计算加权均价 (价格 * 数量)
    is_order_done: bool = False  # 订单是否全部成交 (包含取消)
    is_commission_ready: bool = False  # 佣金报告是否收到
    exec_ids: list = field(default_factory=list)  # 已发生的 exec（来自 execDetails）
    last_status: str = "UNKNOWN"  # 🔥 记录订单最终状态: Filled/Cancelled/Inactive
    # 🔥 新增：追踪已收到佣金报告的 exec_id（去重 + 计数）
    commission_exec_ids: set = field(default_factory=set)
    # 财务数据
    total_commission: float = 0.0
    total_realized_pnl: float = 0.0

    # 基础信息
    side: str = ""
    avg_price: float = 0.0
    avg_cost: float = 0.0  # 需要外部传入成本价
    # 🔥 新增：修复缺失字段，防止崩溃
    label: str = ""
    position_side: str = ""
    # 标记
    is_closing_order: bool = False
    settled: bool = False  # 是否已完成最终记账
    settled_by_timeout: bool = False


# ======================================================================
# 下方代码请替换 events.py 中的 on_exec_details 及其辅助函数
# ======================================================================


def on_exec_details(trade, fill):
    """
    [V8.0 安全合并版 - 继承所有旧逻辑 + 修复财务对账]
    """
    # 第一层：基础分拣
    symbol = trade.contract.symbol
    ctx = contexts_placeholder.get(symbol)

    if not ctx:
        return

    # 🔥 动态初始化新账本 (防止报错，无需修改 shared.py)
    if not hasattr(ctx, "settlement_ledger"):
        ctx.settlement_ledger = {}
    if not hasattr(ctx, "exec_id_map"):
        ctx.exec_id_map = {}

    # 🔥 新增：按订单隔离的去重集合 (修复内存泄漏)
    if not hasattr(ctx, "seen_exec_ids_by_oid"):
        ctx.seen_exec_ids_by_oid = {}

    # 🔥 保留旧版 order_fill_map 逻辑 (确保兼容性)
    oid = trade.order.orderId
    shares_filled = fill.execution.shares
    exec_id = fill.execution.execId  # 🔥 确保这里能拿到 exec_id

    # 🔥 修复 2：按订单隔离去重 (避免全局污染)
    seen_set = ctx.seen_exec_ids_by_oid.setdefault(oid, set())
    is_new_exec = exec_id not in seen_set

    if not hasattr(ctx, "order_fill_map"):
        ctx.order_fill_map = {}

    if is_new_exec:
        seen_set.add(exec_id)
        ctx.order_fill_map[oid] = ctx.order_fill_map.get(oid, 0) + shares_filled

    # 🔥 保留旧版部分成交日志
    filled_so_far = ctx.order_fill_map.get(oid, 0)
    if filled_so_far > 0 and filled_so_far < trade.order.totalQuantity:
        setattr(trade.order, "is_partially_filled", True)
        ctx.partially_filled_time = int(time_module.time())
        ctx.sys_log(
            f"🛡️ [订单部分成交] OID:{oid} 已成交{filled_so_far}股",
            level="DEBUG",
        )

    # 🔥 保留旧版完成清理日志
    if trade.isDone():
        status = getattr(trade.orderStatus, "status", "UNKNOWN")
        ctx.sys_log(f"🧹 [订单结束] OID:{oid} status={status}", level="DEBUG")
        ctx.partially_filled_time = 0
        # ctx.sys_log(
        #    f"🧹 [订单全部成交] OID:{oid} 完全成交，清理 order_fill_map", level="DEBUG"
        # )

    order_ref = getattr(trade.order, "orderRef", "")

    # 🔥 保留旧版 TP1/TP2 逻辑 (关键业务逻辑)
    if trade.isDone():
        if order_ref.startswith("TP1_"):
            ctx.tp1_filled = True
            ctx.sys_log(f"💰 [TP1 完全成交] 允许加仓", level="SUCCESS")
        elif order_ref.startswith("TP2_"):
            ctx.tp1_filled = False
            ctx.sys_log(f"🎉 [TP2 完全成交] 全部平仓", level="SUCCESS")

    # 第二层：全量逻辑装甲 (Try-Except 保护)
    try:
        # 1. 提取物理事实
        execution = fill.execution
        report = fill.commissionReport
        exec_id = fill.execution.execId
        order = trade.order
        order_id = trade.order.orderId

        # 🔥 保留旧版日志 (成交明细)
        ctx.sys_log(
            f"🧾 [成交明细] OID:{order_id} | ParentID:{order.parentId} | "
            f"Qty:{execution.shares} | AvgPrice:{execution.avgPrice}",
            level="DEBUG",
        )

        # 2. ⚡ 逻辑摇铃 (保留旧版状态标志)
        ctx.filled_flag = True
        ctx.last_exec_ts = time_module.time()  # 🔥 最近成交时间戳

        # 3. 🛡️ 过程锁释放 (保留旧版逻辑)
        if trade.isDone():
            ctx.is_processing_order = False
            ctx.is_exiting = False

        # 4. 🆕 新财务结算逻辑 (替换旧的 _process_fill)
        # 初始化新结算状态 (如果还没初始化)
        if oid not in ctx.settlement_ledger:
            is_closing = order_ref.startswith(("S_", "TP", "CL", "EXIT"))
            # 获取标签信息 (兼容旧版)
            label = "UNKNOWN"
            if hasattr(ctx, "_temp_order_audit") and isinstance(
                ctx._temp_order_audit, dict
            ):
                label = ctx._temp_order_audit.get("label", "UNKNOWN")

            ctx.settlement_ledger[oid] = OrderSettlementState(
                order_id=oid,
                order_ref=order_ref,
                total_qty=order.totalQuantity,
                side=execution.side,
                is_closing_order=is_closing,
                avg_cost=getattr(ctx, "avg_fill_price", 0.0),
                label=label,  # 保存标签供最终记账使用
                position_side=getattr(ctx, "position_side", "UNKNOWN"),
            )

        state = ctx.settlement_ledger[oid]
        # 🔥 修复：先判断 exec_id 是否重复，再累加数量和金额
        # 注意：这里也需要用 seen_set 判断，确保财务账本也不重复累加
        if is_new_exec:
            state.exec_ids.append(exec_id)  # ✅ 关键：别忘了记录 exec_ids
            state.filled_qty += execution.shares
            state.total_value += execution.shares * execution.avgPrice
            # 🔥 新增：检查是否有暂存的佣金报告
            if hasattr(ctx, "pending_commission") and exec_id in ctx.pending_commission:
                pending = ctx.pending_commission.pop(exec_id)
                ctx._apply_commission_to_state(
                    state, exec_id, pending["commission"], pending["realized_pnl"]
                )
        # 映射 ExecId 到 OrderId
        ctx.exec_id_map[exec_id] = oid

        filled_so_far = float(getattr(trade.orderStatus, "filled", 0.0))

        total_qty = float(getattr(trade.order, "totalQuantity", 0.0))
        status = getattr(trade.orderStatus, "status", "UNKNOWN")

        # 1) 数量达成：直接认为订单完成（Filled 场景）
        # 1) Filled 判定：用 state.filled_qty（最可靠）
        # if filled_so_far >= total_qty - 1e-9:
        if state.filled_qty >= total_qty - 1e-9:
            state.is_order_done = True
            state.last_status = "Filled"

        # 2) 或者状态达成：Cancelled/Inactive 也视为订单结束（部分成交也要结算）
        elif status in ("Cancelled", "Inactive"):
            if not state.is_closing_order:
                state.is_order_done = True
                state.last_status = status
            else:
                # 离场单：不允许部分成交就结算
                state.is_order_done = False
                state.last_status = status

        # 尝试触发结算 (异步)
        # 注意：这里不再检查 filled_qty >= total_qty，因为取消的订单也要结算已成交部分
        if state.is_order_done:
            asyncio.create_task(ctx._try_settle_order(oid))

        # 5) fill.commissionReport 仅作“观测日志”
        #    ⚠️ 不作为财务权威来源，也不在这里触发结算
        #    （真实佣金/RealizedPnL 以 commissionReportEvent 为准）
        # if report:
        #    ctx.sys_log(
        #        f"🛈 [Fill内CommissionReport-仅观测] ExecId:{exec_id[:8]}... "
        #        f"comm={getattr(report, 'commission', None)} pnl={getattr(report, 'realizedPNL', None)}",
        #        level="DEBUG",
        #    )

    except Exception as e:
        # 确保崩溃不蔓延
        ctx.sys_log(f"❌ [on_exec_details] 逻辑崩溃：{e}", level="ERROR")
        ctx.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")


def on_bar_update(*args):
    """
    [V5.0 响应式总调度中心]
    职责：
    1. 接收 5s 原始数据并合成 2min K线（保持原逻辑）。
    2. [核心重塑]：检查摇铃信号，原子化获取物理快照。
    3. [核心重塑]：驱动所有基于事实快照的决策子系统。
    """
    if not args:
        return
    bars = args[0]
    if not bars:
        return

    target_symbol = bars.contract.symbol
    ctx = contexts_placeholder.get(target_symbol)
    if not ctx:
        return

    raw_df = util.df(bars)
    if raw_df is None or raw_df.empty:
        return

    processed_df = standardize_df(raw_df)
    if processed_df is not None and not processed_df.empty:
        processed_df.columns = [
            c.lower().replace("_", "") for c in processed_df.columns
        ]
        if "time" in processed_df.columns:
            processed_df.rename(columns={"time": "datetime"}, inplace=True)
        df_bars = processed_df
    else:
        return

    # 更新 5s 缓冲区
    ctx.raw_5s_buffer = pd.concat([ctx.raw_5s_buffer, df_bars]).drop_duplicates(
        subset=["datetime"]
    )
    last_dt = df_bars["datetime"].iloc[-1]
    current_price = float(df_bars["close"].iloc[-1])

    # ======================================================================
    # 🔥 [新增] 计算最近 24 根 5 秒 bar 的高低点 (2min 窗口)
    # ======================================================================
    if len(ctx.raw_5s_buffer) >= 24:
        recent_24_bars = ctx.raw_5s_buffer.tail(24)
        # 🔍 防御性列名识别
        cols = {c.lower().replace("_", ""): c for c in recent_24_bars.columns}

        # 计算 120 秒内的最低点和最高点
        ctx.dip_of_2min = float(recent_24_bars[cols["low"]].min())
        ctx.bounce_of_2min = float(recent_24_bars[cols["high"]].max())

    else:
        # 数据不足 24 根时，保持原有值或设为 None
        ctx.dip_of_2min = getattr(ctx, "dip_of_2min", None)
        ctx.bounce_of_2min = getattr(ctx, "bounce_of_2min", None)
    # ======================================================================

    while last_dt >= ctx.last_hist_kline_time + timedelta(minutes=2):
        if last_dt > ctx.last_hist_kline_time + timedelta(minutes=15):
            mask_check = (ctx.raw_5s_buffer["datetime"] >= ctx.last_hist_kline_time) & (
                ctx.raw_5s_buffer["datetime"] < last_dt
            )
            if ctx.raw_5s_buffer.loc[mask_check].empty:
                old_ptr = ctx.last_hist_kline_time
                ctx.last_hist_kline_time = last_dt.replace(second=0, microsecond=0)
                ctx.sys_log(
                    f"⏰ [时间对齐] 发现超过15分钟的真空期({old_ptr.strftime('%H:%M')} -> {ctx.last_hist_kline_time.strftime('%H:%M')})，执行跳跃式对齐。",
                    level="INFO",
                )
                break  # 退出 while 循环，等待后续数据积累

        start_t = ctx.last_hist_kline_time
        end_t = start_t + timedelta(minutes=2)

        # ✨ 严格从 5s 缓冲区切片，物理隔离 2min 历史
        mask = (ctx.raw_5s_buffer["datetime"] >= start_t) & (
            ctx.raw_5s_buffer["datetime"] < end_t
        )
        recent_5s = ctx.raw_5s_buffer.loc[mask]
        new_2min_bar = None

        # ✨ [核心修正]：补票判定逻辑异步化
        current_time_ts = time_module.time()
        # 如果收到的5秒bar数量少于24根，认为存在数据传输丢失现象，进行直接向TWS服务器申请这根2分钟K线数据，不用拼接方式
        now_et = datetime.now(EASTERN_TZ).time()

        if (
            (recent_5s.empty or len(recent_5s) < 24)
            and (current_time_ts - getattr(ctx, "last_patch_time", 0) > 30)
            and now_et > time(9, 32, 2)
        ):
            ctx.last_patch_time = current_time_ts  # 记录时间，30秒内不准重复补票
            ctx.sys_log(
                f"⚠️ [5s数据缺失] 发现 {start_t.strftime('%H:%M')} 样本不足({len(recent_5s)}/24)，启动补票...",
                level="WARN",
            )
            # --- 将异步补票任务丢进循环执行，不阻塞当前的 on_bar_update ---
            try:
                asyncio.run_coroutine_threadsafe(
                    ctx.async_patch_ticket(start_t, end_t), ctx.loop
                )
            except Exception as e:
                ctx.sys_log(f"⚠️ [补票执行失败] {str(e)}", level="ERROR")
            # 由于补票已经交给异步处理，这里我们直接跳过本次循环的后续合成逻辑
            # 防止异步补票和下面的“保底合成”冲突
            ctx.last_hist_kline_time = end_t
            continue

        # 保底合成逻辑
        if not new_2min_bar and not recent_5s.empty:
            # ✨ [防御性编程]：自动识别列名，兼容 open/open_ 等情况
            cols = {c.lower().replace("_", ""): c for c in recent_5s.columns}

            new_2min_bar = {
                "datetime": start_t,
                "open": recent_5s[cols["open"]].iloc[0],
                "high": recent_5s[cols["high"]].max(),
                "low": recent_5s[cols["low"]].min(),
                "close": recent_5s[cols["close"]].iloc[-1],
                "volume": recent_5s[cols["volume"]].sum(),
            }
        # --- 4. 归档与感知 (变量名严格对齐 init) ---
        if new_2min_bar:

            formatted_bar = (
                f"Time:{start_t.strftime('%H:%M')},"
                f"O:{new_2min_bar['open']:.3f}, "
                f"H:{new_2min_bar['high']:.3f}, "
                f"L:{new_2min_bar['low']:.3f}, "
                f"C:{new_2min_bar['close']:.3f}, "
                f"V:{int(new_2min_bar['volume'])}"
            )
            ctx.sys_log(
                f"🟢 [2分钟K线] 合成完毕 | {formatted_bar}",
                level="INFO",
            )
            ctx.process_new_2min_bar(new_2min_bar)
        else:
            # ✨ 兜底：如果彻底没数据，记录一个警告，防止静默丢失
            ctx.sys_log(
                f"⚠️ [合成跳过] {start_t.strftime('%H:%M')} 完全无5s样本且不满足补票条件",
                level="WARN",
            )

        # --- 5.0 在 drop 过期 5s 数据前，先把增量 5s 数据落盘 ---
        try:
            if not ctx.raw_5s_buffer.empty and "datetime" in ctx.raw_5s_buffer.columns:
                to_save = ctx.raw_5s_buffer.copy()

                # 只保存未写入过的增量
                if getattr(ctx, "last_5s_saved_dt", None) is not None:
                    to_save = to_save[to_save["datetime"] > ctx.last_5s_saved_dt]

                if not to_save.empty:
                    to_save = to_save.sort_values("datetime").copy()
                    to_save["datetime"] = pd.to_datetime(
                        to_save["datetime"], errors="coerce"
                    )
                    to_save = to_save[to_save["datetime"].notna()]

                    if not to_save.empty:
                        last_saved_dt = to_save["datetime"].max()

                        # 时间格式按你要求保留到 hh:mm:ss
                        to_save["datetime"] = to_save["datetime"].dt.strftime(
                            "%H:%M:%S"
                        )

                        export_cols = [
                            c
                            for c in [
                                "datetime",
                                "open",
                                "high",
                                "low",
                                "close",
                                "volume",
                            ]
                            if c in to_save.columns
                        ]
                        to_save = to_save[export_cols]

                        to_save.to_csv(
                            ctx.raw_5s_csv_filename,
                            mode="a",
                            index=False,
                            header=not ctx.raw_5s_csv_header_written,
                            encoding="utf-8-sig",
                        )

                        ctx.raw_5s_csv_header_written = True
                        ctx.last_5s_saved_dt = last_saved_dt

        except Exception as e:
            ctx.sys_log(f"⚠️ [5s CSV写入失败] {e}", level="WARN")

        # --- 6. 清理与推进 ---
        # ✨ 清理过期的 5s 缓冲区，防止内存溢出
        ctx.raw_5s_buffer = ctx.raw_5s_buffer[
            ctx.raw_5s_buffer["datetime"] > last_dt - timedelta(minutes=10)
        ]
        ctx.last_hist_kline_time = end_t  # ✨ 确保这行代码在 while 的最后，且能被执行到
        # while 循环体到这一行结束

    # 每5秒钟我们调用 take_snapshot 拿回一份不可变的 ContextSnapshot 对象
    snapshot = None
    snapshot = ctx.take_snapshot()
    if snapshot:
        ctx.latest_snapshot = snapshot
    else:
        snapshot = getattr(ctx, "latest_snapshot", None)
        if not snapshot:
            ctx.sys_log(
                "❌ [take_snapshot] 本拍快照失败且无历史快照可回退，跳过治理",
                level="ERROR",
            )
            return
        ctx.sys_log(
            "⚠️ [take_snapshot] 本拍快照失败，回退使用上一拍快照继续治理", level="WARN"
        )

    ctx._sync_position(snapshot)
    # 增加防御性检查（可选但推荐）
    if ctx.loop is None or not ctx.loop.is_running():  # ✅ 统一使用ctx.loop
        ctx.sys_log("⚠️ [on_bar_update] 事件循环未就绪，跳过治理", level="WARN")
        return
    try:
        asyncio.run_coroutine_threadsafe(ctx.manage_position(snapshot), ctx.loop)
    except Exception as e:
        ctx.sys_log(f"⚠️ [manage_position执行失败] {str(e)}", level="ERROR")
        ctx.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
    try:
        asyncio.run_coroutine_threadsafe(
            ctx.run_decision_pipeline(
                current_price, shared.global_last_vix_close, snapshot
            ),
            ctx.loop,
        )
    except Exception as e:
        ctx.sys_log(f"⚠️ [run_decision_pipeline执行失败] {str(e)}", level="ERROR")
        ctx.sys_log(f".StackTrace:\n{traceback.format_exc()}", level="DEBUG")
