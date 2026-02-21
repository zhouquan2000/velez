from .shared import *
from .trading_context import TradingContext
from .events import on_exec_details, on_commission_report, on_bar_update

async def main():
    global contexts_placeholder,loop 
    if loop is None:
        try:
            loop = asyncio.get_running_loop()  # Python 3.7+ 推荐
        except RuntimeError:
            loop = asyncio.get_event_loop()    # 回退方案（Python 3.6-3.9）
    
    sys_log("🚀 量化交易程序启动，加载各个股票的配置参数", level="System")
    await asyncio.sleep(1.0) # 确保 IB API 连接稳定
    start_funds = await show_account_detail()
    sys_log(f"💰 [启动第2步] TWS账户资金查询。当前可用资金总额: ${start_funds:,.3f} USD", level="System")

    await asyncio.sleep(1.0) # 确保账户资金读取工作已经完成
    await load_vix_data_async(is_init=True)
    contexts_placeholder.update({
            symbol: TradingContext(symbol, ib, loop=loop, **STOCK_CONFIGS.get(symbol, {}))
            for symbol in SYMBOLS
        })
    ib.execDetailsEvent += on_exec_details
    ib.commissionReportEvent += on_commission_report
    for symbol, ctx in contexts_placeholder.items():
        # sys_log(f"📋 个股配置参数| {symbol} | EB门槛: {ctx.eb_range_mult}x | EB量能: {ctx.eb_vol_mult}x | 风险单元: ${ctx.risk_unit}", level="System")
        sys_log(
            f"📋 个股配置参数| {symbol} | "
            f"EB门槛:{ctx.eb_range_mult}x | EB量能:{ctx.eb_vol_mult}x | "
            f"纯度:{ctx.eb_body_ratio} | MaxQty:{ctx.max_qty} | 风险单元:${ctx.risk_unit}",
            level="System"
        )

    ib.errorEvent += on_tws_error # 全局错误监听器
    sys_log("📥 TWS系统报错监听器函数已加载...")
    
    # --- 3. [重构版]：执行全量初始化 (ATR、滑点、历史补票) ---
    #sys_log("📦 正在为所有品种灌装初始化数据...", level="INFO")
    
    # 获取所有 context 实例
    contexts = list(contexts_placeholder.values())
    sys_log("🔍 正在启动合约身份(conId)验证程序...", level="System")
    for ctx in contexts:
        try:
            # 物理操作：联网TWS补全合约的所有隐式参数（特别是conId）
            qualified = await ib.qualifyContractsAsync(ctx.contract)
            if qualified:
                ctx.contract = qualified[0]
                # 此时ctx.contract已经从一个只有名字的“毛坯”，变成了带身份证号的“成品”
                ctx.log(f"✅ 合约认证成功 (conId: {ctx.contract.conId})")
            else:
                ctx.log(f"❌ 合约认证失败：请确认 {ctx.symbol} 是否有行情权限或拼写正确", level="ERROR")
                ctx.suspend_today = True
        except Exception as e:
            ctx.log(f"⚠️ 合约认证发生异常: {e}", level="ERROR")
            ctx.suspend_today = True
    # 第一次初始化尝试 (并发执行)
    
    init_tasks = [ctx.load_history_data() for ctx in contexts if not ctx.suspend_today]
    
    if init_tasks:
        results = await asyncio.gather(*init_tasks, return_exceptions=True)
        
        # 将结果映射回对应的 context (注意：这里需要过滤掉已挂起的)
        active_contexts = [ctx for ctx in contexts if not ctx.suspend_today]
        for i, res in enumerate(results):
            ctx = active_contexts[i]
            if res is not True:
                ctx.suspend_today = True 
                sys_log(f"⚠️ [{ctx.symbol}] 历史数据初始化失败，已进入停摆模式。原因: {res}", level="WARN")
            else:
                ctx.log(f"✅ [{ctx.symbol}]参数变量初始化完成，进行下一步。")

    # --- 4. [移植 06-9]：等待市场开盘 (Sleep) ---
    now = datetime.now(EASTERN_TZ)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    if now >= market_close:
        sys_log("⛔ 交易时段(9:30-16:00)已经结束,执行退出流程。", level="System")
        return
    if now < market_open:
        wait_sec = (market_open - now).total_seconds() - 5
        sys_log(f"⏳ [盘前等待] 正在等待开盘，剩余 {int(wait_sec)} 秒...")
        await asyncio.sleep(max(0, wait_sec))

    # --- 5. [实盘同步与权限审计]：开盘前最后一次态势核对 ---
    
    sys_log("🔄 正在从 TWS 同步接收最新实盘数据...")
    # 5.1 异步同步持仓与在途订单 (自愈逻辑)
    sync_tasks = [ctx.sync_initial_state() for ctx in contexts_placeholder.values() if not ctx.suspend_today]
    await asyncio.gather(*sync_tasks)

    # 5.2 ✨ [核心增强] 战前行情权限审计 (对齐 06-9)
    sys_log("🛡️ 开始检查每个品种是否订阅了实时数据...")
    
    check_tasks = [ctx.check_subscription_async() for ctx in contexts_placeholder.values() if not ctx.suspend_today]
    check_results = await asyncio.gather(*check_tasks)
    
    if not all(check_results):
        sys_log("🚨 [SECURITY ALERT] 没有能接收到实时行情，请立即处理！", level="ALERT")


    # 6. 启动 5s 实时链路
    active_streams = []
    for symbol, ctx in contexts_placeholder.items():
        # ✨ 关键补丁：如果初始化失败，跳过实时链路挂载
        if ctx.suspend_today:
            ctx.log(f"🚫 {symbol} 今日暂停交易，不再与TWS服务器进行数据同步", level="ERROR")
            continue
            
        try:
            bars = ib.reqRealTimeBars(ctx.contract, 5, 'TRADES', useRTH=False)
            bars.updateEvent += on_bar_update 
            ctx.bars_reference = bars 
            active_streams.append(bars)
            ctx.log(f"📡 {symbol}与TWS服务器的实时链路已经建立，接收5秒bar数据 (RTH: Off)")
        except Exception as e:
            ctx.log(f"❌ {symbol}与TWS服务器的数据链路异常，错误代码: {e}", level="ERROR")
    sys_log("🚀 一切准备就绪，等待TWS传送来的各个股票的5秒bar实时行情", level="INFO")
    
    # 启动每日安全守护
    asyncio.create_task(daily_closing_safety_guard(contexts_placeholder))
    
    # ✨ 保持异步心跳，防止函数直接退出
    while True:
        await asyncio.sleep(10)


def run():
    # 1. variable initialization for safe KeyboardInterrupt cleanup
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main())

    except KeyboardInterrupt:
        try:
            sys_log('接收到用户中断指令 Ctrl+C，启动紧急清仓..', level='System')
            loop.run_until_complete(contrl_c_exit(contexts_placeholder))
        except Exception as e:
            sys_log(f'紧急清仓期间发生二次异常: {e}', level='CRITICAL')
        finally:
            if loop.is_running():
                loop.stop()
            sys_log('系统安全退出。', level='System')
            os._exit(0)
