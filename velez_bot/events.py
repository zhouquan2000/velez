from .shared import *
from . import shared

def on_commission_report(commission_report):
    exec_id = commission_report.execId
    for ctx in contexts_placeholder.values():
        if exec_id in ctx._pending_fills:
            ctx.commission_report(commission_report)
            break

def on_exec_details(trade, fill):
    """
    [V7.9 一体化财务与逻辑调度中心 - 工业级加固版]
    """
    # 第一层：基础分拣（不进 try，因为 ctx 找不到就没法 sys_log）
    symbol = trade.contract.symbol
    ctx = contexts_placeholder.get(symbol)

    if not ctx: return
    
    oid = trade.order.orderId
    shares_filled = fill.execution.shares
    ctx.order_fill_map[oid] = ctx.order_fill_map.get(oid, 0) + shares_filled
    
    if ctx.order_fill_map[oid] > 0 and ctx.order_fill_map[oid] < trade.order.totalQuantity:
        setattr(trade.order, 'is_partially_filled', True)
        ctx.sys_log(f"🛡️ [订单部分成交] OID:{oid} 已成交{ctx.order_fill_map[oid]}股", level="DEBUG")

    if trade.isDone() and trade.orderStatus.filled >= trade.order.totalQuantity:
        ctx.order_fill_map.pop(oid, None)  # 安全删除
        ctx.sys_log(f"🧹 [订单全部成交] OID:{oid} 完全成交，清理order_fill_map", level="DEBUG") 
    order_ref = getattr(trade.order, 'orderRef', '')   
    # 检查是否为止盈单成交
    if trade.isDone():  # ✅ 关键修复：仅订单完全成交时更新状态
        if order_ref.startswith('TP1_'):
            ctx.tp1_filled = True
            ctx.sys_log(f"💰 [TP1完全成交] 允许加仓", level="SUCCESS")
        elif order_ref.startswith('TP2_'):
            ctx.tp1_filled = False
            ctx.sys_log(f"🎉 [TP2完全成交] 全部平仓", level="SUCCESS")
    # 第二层：全量逻辑装甲
    try:
        # 1. 提取物理事实
        execution = fill.execution
        report = fill.commissionReport
        exec_id = fill.execution.execId
        order = trade.order
        order_id = trade.order.orderId
        # 收集成交信息（用于暂存或立即记账）
        fill_info = {
            'time': fill.execution.time,
            'side': fill.execution.side,
            'qty': fill.execution.shares,
            'price': fill.execution.avgPrice,
            'order_id': order_id,
            'order_ref': order_ref,     
            'label': ctx._temp_order_audit.get('label', 'UNKNOWN'),
            'position_side': ctx.position_side,
            'avg_cost': ctx.avg_fill_price
        }
        ctx.sys_log(
            f"🧾 [成交明细] OID:{order_id} | ParentID:{order.parentId} | "
            f"Qty:{execution.shares} | AvgPrice:{execution.avgPrice}",
            level="DEBUG"
        )

        # 2. ⚡ 逻辑摇铃 (原子化核心：逻辑先行)
        # 只要成交就摇铃，让司令部下一秒立刻审计持仓
        ctx.filled_flag = True

        # 3. 🛡️ 过程锁释放
        # 物理单据彻底结束时，释放下单保护锁
        if trade.isDone():
            ctx.is_processing_order = False
            ctx.is_exiting = False

        # 4. 🧾 财务审计 (佣金报告到达后执行)
        # 判断佣金报告是否已完整
        if report and (report.realizedPNL != 0 or report.commission != 0):
            # 报告已就绪，立即记账
            ctx._process_fill(exec_id, fill_info, report)
            # ✅ 取消超时任务（如果已启动）
            if exec_id in ctx._pending_fills:
                _, task = ctx._pending_fills.pop(exec_id)
                if not task.done():
                    task.cancel()
            return
        # 仅当report未就绪时启动超时任务
        if exec_id not in ctx._pending_fills:  # 避免重复启动
            timeout_task = asyncio.create_task(ctx._check_pending_fill(exec_id))
            ctx._pending_fills[exec_id] = (fill_info, timeout_task)
        
        
    except Exception as e:
        # 确保崩溃不蔓延，记录现场错误
        ctx.sys_log(f"❌ [on_exec_details] 逻辑崩溃: {e}", level="ERROR")


def on_bar_update(*args):
    """
    [V5.0 响应式总调度中心] 
    职责：
    1. 接收 5s 原始数据并合成 2min K线（保持原逻辑）。
    2. [核心重塑]：检查摇铃信号，原子化获取物理快照。
    3. [核心重塑]：驱动所有基于事实快照的决策子系统。
    """
    if not args: return
    bars = args[0]
    if not bars: return
    
    target_symbol = bars.contract.symbol 
    ctx = contexts_placeholder.get(target_symbol)
    if not ctx: return

    raw_df = util.df(bars)
    if raw_df is None or raw_df.empty: return
    
    processed_df = standardize_df(raw_df)
    if processed_df is not None and not processed_df.empty:
        processed_df.columns = [c.lower().replace('_', '') for c in processed_df.columns]
        if 'time' in processed_df.columns:
            processed_df.rename(columns={'time': 'datetime'}, inplace=True)
        df_bars = processed_df
    else: return

    # 更新 5s 缓冲区
    ctx.raw_5s_buffer = pd.concat([ctx.raw_5s_buffer, df_bars]).drop_duplicates(subset=['datetime'])
    last_dt = df_bars['datetime'].iloc[-1]
    current_price = float(df_bars['close'].iloc[-1])


    
    while last_dt >= ctx.last_hist_kline_time + timedelta(minutes=2):
        if last_dt > ctx.last_hist_kline_time + timedelta(minutes=15):
            mask_check = (ctx.raw_5s_buffer['datetime'] >= ctx.last_hist_kline_time) & (ctx.raw_5s_buffer['datetime'] < last_dt)
            if ctx.raw_5s_buffer.loc[mask_check].empty:
                old_ptr = ctx.last_hist_kline_time
                ctx.last_hist_kline_time = last_dt.replace(second=0, microsecond=0)
                ctx.sys_log(f"⏰ [时间对齐] 发现超过15分钟的真空期({old_ptr.strftime('%H:%M')} -> {ctx.last_hist_kline_time.strftime('%H:%M')})，执行跳跃式对齐。", level="INFO")
                break # 退出 while 循环，等待后续数据积累

        start_t = ctx.last_hist_kline_time
        end_t = start_t + timedelta(minutes=2)
        
        # ✨ 严格从 5s 缓冲区切片，物理隔离 2min 历史
        mask = (ctx.raw_5s_buffer['datetime'] >= start_t) & (ctx.raw_5s_buffer['datetime'] < end_t)
        recent_5s = ctx.raw_5s_buffer.loc[mask]
        new_2min_bar = None

        # ✨ [核心修正]：补票判定逻辑异步化
        current_time_ts = time_module.time()
        #如果收到的5秒bar数量少于24根，认为存在数据传输丢失现象，进行直接向TWS服务器申请这根2分钟K线数据，不用拼接方式
        now_et = datetime.now(EASTERN_TZ).time()
 
        if (recent_5s.empty or len(recent_5s) < 24) and (current_time_ts - getattr(ctx, 'last_patch_time', 0) > 30) and now_et > time(9,32,2):
            ctx.last_patch_time = current_time_ts # 记录时间，30秒内不准重复补票
            ctx.sys_log(f"⚠️ [5s数据缺失] 发现 {start_t.strftime('%H:%M')} 样本不足({len(recent_5s)}/24)，启动补票...", level="WARN")
            # --- 将异步补票任务丢进循环执行，不阻塞当前的 on_bar_update ---     
            try: 
                asyncio.run_coroutine_threadsafe(ctx.async_patch_ticket(start_t, end_t), ctx.loop)
            except Exception as e:
                ctx.sys_log(f"⚠️ [补票执行失败] {str(e)}", level="ERROR")
            # 由于补票已经交给异步处理，这里我们直接跳过本次循环的后续合成逻辑
            # 防止异步补票和下面的“保底合成”冲突
            ctx.last_hist_kline_time = end_t
            continue

        # 保底合成逻辑
        if not new_2min_bar and not recent_5s.empty:
            # ✨ [防御性编程]：自动识别列名，兼容 open/open_ 等情况
            cols = {c.lower().replace('_', ''): c for c in recent_5s.columns}
            
            new_2min_bar = {
                'datetime': start_t,
                'open': recent_5s[cols['open']].iloc[0],
                'high': recent_5s[cols['high']].max(),
                'low': recent_5s[cols['low']].min(),
                'close': recent_5s[cols['close']].iloc[-1],
                'volume': recent_5s[cols['volume']].sum()
            }
        # --- 4. 归档与感知 (变量名严格对齐 init) ---
        if new_2min_bar:
                     
            formatted_bar = (
                f"O:{new_2min_bar['open']:.3f}, "
                f"H:{new_2min_bar['high']:.3f}, "
                f"L:{new_2min_bar['low']:.3f}, "
                f"C:{new_2min_bar['close']:.3f}, "
                f"V:{int(new_2min_bar['volume'])}"
            )
            ctx.sys_log(f"🟢 [2分钟K线] {start_t.strftime('%H:%M')} 合成完毕 | {formatted_bar}", level="INFO")
            ctx.process_new_2min_bar(new_2min_bar)
        else:
            # ✨ 兜底：如果彻底没数据，记录一个警告，防止静默丢失
            ctx.sys_log(f"⚠️ [合成跳过] {start_t.strftime('%H:%M')} 完全无5s样本且不满足补票条件", level="WARN")
        # --- 5. 清理与推进 ---            
        # ✨ 清理过期的 5s 缓冲区，防止内存溢出
        ctx.raw_5s_buffer = ctx.raw_5s_buffer[ctx.raw_5s_buffer['datetime'] > last_dt - timedelta(minutes=10)]
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
            ctx.sys_log("❌ [take_snapshot] 本拍快照失败且无历史快照可回退，跳过治理", level="ERROR")
            return
        ctx.sys_log("⚠️ [take_snapshot] 本拍快照失败，回退使用上一拍快照继续治理", level="WARN")

    ctx._sync_position(snapshot)
    # 增加防御性检查（可选但推荐）
    if ctx.loop is None or not ctx.loop.is_running():  # ✅ 统一使用ctx.loop
        ctx.sys_log("⚠️ [on_bar_update] 事件循环未就绪，跳过治理", level="WARN")
        return
    try: 
        asyncio.run_coroutine_threadsafe(ctx.manage_position(snapshot), ctx.loop)
    except Exception as e:
                ctx.sys_log(f"⚠️ [manage_position执行失败] {str(e)}", level="ERROR")    
    try: 
        asyncio.run_coroutine_threadsafe(
            ctx.run_decision_pipeline(current_price, shared.global_last_vix_close, snapshot),
            ctx.loop
        )
    except Exception as e:
                ctx.sys_log(f"⚠️ [run_decision_pipeline执行失败] {str(e)}", level="ERROR")     

# ======================================================================
# --- 🚀 [07-1 核心：主节拍器与多品种调度] ---
# ======================================================================



