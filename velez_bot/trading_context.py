from .shared import *
from . import shared
from .context_snapshot import ContextSnapshot

class TradingContext:

    def __init__(self, symbol, ib, loop, **kwargs):
        self.symbol = symbol
        self.ib = ib
        self.contract = Stock(symbol, 'SMART', 'USD')
        self.loop = loop
        # --- 1. [风险与执行参数] ---
        self.risk_unit = float(kwargs.get('risk_unit', 120)) # 默认$120
        self.max_qty = int(kwargs.pop('max_qty', 200))  # 默认200股
        self.eb_range_mult = float(kwargs.pop('eb_range_mult', 2.0))
        self.eb_vol_mult   = float(kwargs.pop('eb_vol_mult', 1.2))
        self.eb_body_ratio = float(kwargs.pop('eb_body_ratio', 0.85))

        self.static_atr = float(kwargs.pop('static_atr', 0.5))     # 初始静态ATR
        self.atr = self.static_atr                           # 动态ATR缓存
        self.effective_atr = 0.0                             # 用于 plan_trade 的动态 ATR 锚点
        self.min_gap_factor = 0.8                            # 强制止损呼吸空间因子 (0.8 * ATR)
        self.tp1_ratio = 0.5                                 # TP1 默认减仓比例 (50%)
        self.risk_pyramid_factor = 0.5                       # 加仓风险折减系数
        self.slippage_allowance = float(kwargs.pop('slippage_allowance', 0.05))
        self.custom_params = kwargs         # 用于扩展参数
        self.pnl_total = 0.0
        self.order_fill_map = {}  # {order_id: filled_qty}
        self.filled_flag = False
        self._pending_fills = {}          # dict: execId -> (fill_info, timestamp)
        self._pending_timeout = 5        # 超时秒数
        # --- [止损快照变量] 用于固化探测瞬间的价格锚点 ---
        self.law1_sl = 0.0
        self.law2_sl = 0.0
        self.law3_sl = 0.0
        self.law4_sl = 0.0
        self.law5_sl = 0.0
        self.law6_sl = 0.0
        self.law8_sl = 0.0
        self.v180_sl = 0.0

        # --- 2. [态势感知与法则旗语] ---
        # Law #1-8 信号位 (由 _detect_market_patterns 驱动)
        self.ready_to_long_law1 = self.ready_to_short_law1 = False # Elephant Bar
        self.ready_to_long_law2 = self.ready_to_short_law2 = False # Color Change
        self.ready_to_long_law3 = self.ready_to_short_law3 = False # 3-5 Bars
        self.ready_to_long_law4 = self.ready_to_short_law4 = False # RBI/GBI
        self.ready_to_long_law5 = self.ready_to_short_law5 = False # 20MA Cross
        self.ready_to_long_law6 = self.ready_to_short_law6 = False # Home Run
        self.ready_to_long_law8 = self.ready_to_short_law8 = False # Fab 42
        self.ready_to_long_180  = self.ready_to_short_180  = False # 180反转
        
        # 辅助形态记录
        self.current_tail_type = None      # 存放当前正在探测的K线形态 (BT/TT)
        self.last_confirmed_tail = None    # 存放上一根已经收盘确认的K线形态
        self.bars_reference = None
        
        self.elephant_bar_log = []      # 大象柱分析日志
        self.red_bar_count = 0          # 实时红柱计数
        self.green_bar_count = 0        # 实时绿柱计数
        self.last_red_count = 0         # ✨ 新增：变绿瞬间，备份之前的红柱数
        self.last_green_count = 0       # ✨ 新增：变红瞬间，备份之前的绿柱数
        self.low_of_dip = 0.0
        self.high_of_bounce = 0.0       
        
        # --- 3.1 [状态机与影子账本] ---
        self.last_processed_time = None  # 初始化为 None，确保第一次计算能顺利通过哨兵校验
        self.state = "OPEN_STAGE"       # 核心状态机
        self.order_place_time = 0
        self.actual_filled_qty = 0      # 当前物理持仓数量 (绝对值)
        self.avg_fill_price = 0.0       # 当前持仓的平均成本
        self.is_exiting = False           # ✨ 新增：离场/减仓专用内存锁
        self.pending_label = None
        self.entry_law = None           # 入场信号标签
        self.position_side = ""      # LONG / SHORT
        self.last_entry_price = 0.0     # 信号触发参考价
        self.latest_snapshot = None
        self.current_cond = "Cond_01_IDLE"
        self.last_cond = "Cond_01_IDLE"
        self._temp_order_audit = {
            'order_id': 0,          # 捕获 p_order.orderId
            'label': 'IDLE',        # 捕获指令中的 label
            'trigger_price': 0.0,   # 触发时的参考价 (财务对账基准)
            'last_p_lmt': 0.0,      # 下单瞬间的主订单的 LMT 价
            'last_s_aux': 0.0       # 下单瞬间的止损单的 Stop 价格（用于固定 Gap）
        }
        self.initial_stop_price = 0.00        #主订单下单时候的止损价格，当主订单成交那一刻记录下来
        self.final_stop_price = 0.0     # 动态调整之后的止损单上的止损价格
        self.trade_records = []         # 交易审计记录
        self._loss_recorded_orders = set() 
        #self.in_flight_qty = 0          # 正在 TWS 柜台挂着的平仓股数 (绝对值)
        self.last_bar_minute = None    # 用于 15min 跨线判定
        self.last_patch_time = 0       # 用于补票频率限制

        # 3.2  影子哨兵参数 (对齐 execute_trade / manage_position)
        self.tp1 = 0.0                  # 减仓目标价
        self.tp2 = 0.0                  # 终极目标价
        self.tp1_filled = False         # TP1止盈单是否已经成交了
        self.last_trade_qty = 0         # 初始成交总股数
        self.is_pyramid_processed = False
        self.is_processing_order = False # 入场/加仓锁：防止主订单重复提交
        self.tp1_qty = 0                # 计划止盈单TP1的手数
        
        # --- 3.3 [财务审计与对账开关] ---
        
        self.entry_total_count = 0      # 累计成交笔数
        self.processed_exec_ids = set() # ✨ 核心加固：存储已处理的成交 ID，防止重复计账

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
        self.ma8 = 0.0                  # 快速追踪均线
        self.ma20 = 0.0                 # 主趋势均线
        self.ma200 = None               # 长期基准线
        self.ma20_prev = 0.0            # T-1 均线值
        self.ma20_prev2 = 0.0           # T-2 均线值
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
        self.strade = None              # 止损单引用
        self.parent_trade = None        # 主单引用
        self.tp_trade = None            #止盈单引用
        self.suspend_today = False      # 当日熔断
        self._last_vix_warn = False     # VIX 警告位
        self.consecutive_losses = 0     # 连续亏损计数
        self.margin_requirement = 0.3   # 保证金要求
        self.capital_buffer = 0.05

        # 日志配置
        today_str = datetime.now(EASTERN_TZ).strftime('%Y-%m-%d')
        self.log_filename = f"log_{self.symbol}_{today_str}.txt"
        self.sys_log(f"✅{symbol}的变量初始化工作完成", level="INFO")

    def sys_log(self, message, level="INFO"):
        """
        [07-12 品种级桥接]
        职责：将品种信息封装后，递交给全局输出引擎
        """
        # 1. 业务图标定义
        biz_icons = {
            "DECISION": "🎯", 
            "INFO":     "💡", 
            "WARN":     "⚠️", 
            "ERROR":    "🚫", 
            "FILTER":   "🛡️",
            "DEBUG": "🛠️"
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
                ts = datetime.now(EASTERN_TZ).strftime('%H:%M:%S')
                f.write(f"[{ts}] {context_msg}\n")
        except:
            pass

    log = sys_log  # 别名，方便调用


    async def check_subscription_async(self, timeout=5):
        """[类方法] 行情权限审计：异步验证实时是否具有实时行情权限"""
        self.log(f"📡 正在检查验证 {self.symbol} 是否能接收到来自TWS的实时行情 (探测时长: {timeout}s)...")
        try:
            # 订阅行情流（不使用快照，直接看是否有数据泵推送）
            self.ib.reqMktData(self.contract, '', False, False)
            
            start_t = time_module.time()
            while time_module.time() - start_t < timeout:
                await asyncio.sleep(0.5)
                # 获取该合约的最新行情快照
                ticker = self.ib.ticker(self.contract)
                # 检查是否有有效报价或最后成交价
                if ticker and (ticker.last > 0 or ticker.bid > 0):
                    self.ib.cancelMktData(self.contract) # 验证完立刻关闭，节省资源
                    self.log(f"✅ {self.symbol} 可以实时获取到TWS的实时行情数据，验证通过。")
                    return True
            
            self.ib.cancelMktData(self.contract)
            self.log(f"❌ {self.symbol} 实时获取行情验证超时！请确认今天是交易日并且确认TWS已订阅该市场实时行情。")
            self.play_sound("ERROR")
            return False
        except Exception as e:
            self.log(f"⚠️ 实时获取行情数据异常，错误代码: {e}")
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
                self.log(f"⚠️ {self.symbol} 初始快照采集失败，跳过对账", level="WARN")
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
                
                self.log(f"🕵️ [启动对账] 发现{self.symbol}实盘持仓: {snapshot.fact_pos}股 | 均价: {snapshot.avg_cost:.2f}")
                
                if snapshot.active_stop_order:
                    self.final_stop_price = snapshot.active_stop_order.auxPrice
                    self.log(f"🩹 [启动对账] 已找回柜台止损单，止损价: {self.final_stop_price}")
                self.filled_flag = True
            else:
                # --- 场景 B：无持仓，检查是否有在途入场单 ---
                # 寻找 parentId == 0 的非止损挂单 (即入场单)
                entry_trade = next((o for o in snapshot.live_orders 
                                  if o.parentId == 0 and o.orderType not in ['STP', 'STP LMT']), None)
                
                if entry_trade:
                    self.state = "ORDER_SENT"
                    self.log(f"🛰️ [启动对账] 发现入场单在途，重置状态为 ORDER_SENT | ID: {entry_trade.orderId}")
                    self.filled_flag = True
                else:
                    self.state = "OPEN_STAGE"
                    self.log(f"📡 [启动对账] {self.symbol} 账户空闲，处于待机状态。")

            # 3. 🛡️ 重建精算快照 (用于 log_trade 财务对账兜底)
            
            self._temp_order_audit = {
                'order_id': 0, 
                'label': "REBOOT-SYNC",
                'trigger_price': snapshot.avg_cost if snapshot.has_position else 0.0,
                'last_p_lmt': 0.0, 
                'last_s_aux': self.final_stop_price
            }
            
            
            self.log(f"✅ {self.symbol} V5.0 逻辑链路初始化完成，当前持仓: {snapshot.fact_pos}", level="INFO")

        except Exception as e:
            self.log(f"❌ {self.symbol} 启动自愈对账异常: {e}", level="ERROR")


    def estimate_ibkr_commission(self, qty: float, price: float, direction: str) -> float:
        """[类方法] 估算 IBKR 阶梯佣金与监管费 (对齐 06-9)"""
        qty = abs(qty)
        # 1. 基础阶梯佣金 (Tiered)
        base_comm = max(0.35, qty * 0.0035)
        
        # 2. 监管费 (Regulator Fees)
        sec_fee = 0.0
        finra_fee = 0.0
        
        if direction.upper() == 'SELL':
            # SEC 费率 (估算值)
            sec_fee = (qty * price) * 0.0000229
            # FINRA 费率 (0.000119/股，最高 5.95)
            finra_fee = min(5.95, qty * 0.000119)
            
        total_comm = base_comm + sec_fee + finra_fee
        return round(total_comm, 2)

    
    def log_trade(self, time_str: str, action: str, qty: float, price: float, 
              pnl: float, commission: float, exec_id: str = "", 
              label=None, order_id: int = None, order_ref: str = None): 
    
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
            fact_pnl = 0.0  
            is_closing = False
            if order_ref:
                is_closing = (
                    order_ref.startswith('S_') or 
                    order_ref.startswith('TP') or 
                    order_ref.startswith('CL')
                ) 
            else:
                is_closing = (
                    (self.position_side == "LONG" and action == "SLD") or 
                    (self.position_side == "SHORT" and action == "BOT")
                )
                
            if is_closing:
                net_pnl = round(pnl - commission, 2)
                self.pnl_total += net_pnl
                self.entry_total_count += 1
            else:
                # 开仓：净盈亏为 手续费，不累加
                net_pnl = round(-commission, 2) 
                self.pnl_total += net_pnl  
                self.entry_total_count += 1
            log_msg = (f"💰 [对账-{label or 'N/A'}]"
                        f"💰  {action} {qty} @ {price:.3f} | "
                       f"佣金: {commission:.2f} | 净盈亏: {net_pnl:.2f} | "
                       f"累计PnL: {round(self.pnl_total, 2)} | 连损: {self.consecutive_losses}")
            self.log(log_msg, level="INFO")
            # --- 3. 连损判定与熔断逻辑 ---
            if is_closing :
                is_judgment_error = False
                # 只有在产生实际亏损时才检查是否触及底线
                if net_pnl < 0:
                    init_stop = getattr(self, 'initial_stop_price', 0)
                    if init_stop == 0:
                        init_stop = getattr(self, 'final_stop_price', 0)

                    if self.position_side == "LONG":
                        if init_stop != 0 and price <= init_stop:
                            is_judgment_error = True
                    else: # SHORT
                        if init_stop != 0 and price >= init_stop:
                            is_judgment_error = True

                if is_judgment_error:
                    # 去重检查：同一订单的多次成交只计一次
                    if order_id is not None and order_id in self._loss_recorded_orders:
                        self.log(f"⏭️ 同一止损单后续成交，跳过连续亏损计数", level="DEBUG")
                    else:
                        self.consecutive_losses += 1
                        if order_id is not None:
                            self._loss_recorded_orders.add(order_id)
                        self.log(f"📉 [战术失败]初始止损线出发离场，连续亏损计数器更新，已经连续亏损: {self.consecutive_losses}次", level="WARN")
                else:
                    # 风险管理成功（盈利离场或亏损但保护了底线）
                    self.consecutive_losses = 0 
                    if net_pnl < 0:
                        self.log(f"🛡️ [风险管理] 亏损，但离场价优于初始计划，重置连损。")
                    else:
                        self.log(f"🎉 [战术成功] 盈利离场，重置连损。")

            # --- 4. 判定是否触发今日熔断 ---
            # 逻辑 4.1：战术熔断（原有逻辑：连续 3 次触及初始止损底线失败）
            is_tactical_suspend = (self.consecutive_losses >= 3)
            # 逻辑 4.2：财务熔断（新增建议：个股亏损超过 3 倍 Risk Unit）
            # 假设 risk_unit 是 150，亏损 450 就停
            max_pnl_loss = -(3 * getattr(self, 'risk_unit', 150))
            is_financial_suspend = (self.pnl_total <= max_pnl_loss)
            # 逻辑 4.3：财务熔断（无论是连续3次止损还是亏损金额到了每日允许的上限，都高悬免战牌）
            if is_tactical_suspend or is_financial_suspend:
                self.suspend_today = True
                self.play_sound("SUSPEND")                
                reason = "连续止损3次" if is_tactical_suspend else f"今日本股票{self.symbol}已经亏损{self.pnl_total}超过当日上限({max_pnl_loss})"
                self.log(f"🚨 [个股熔断] {self.symbol} 触发停止交易。原因: {reason}", level="ERROR")

            # --- 5. 构造标准审计记录 ---
            record = {
                'time': time_str,
                'symbol': self.symbol,
                'action': action,
                'qty': qty,
                'price': round(price, 3),
                'commission': round(commission, 2),
                'net_pnl': net_pnl,
                'cum_pnl': round(self.pnl_total, 2),
                'loss_streak': self.consecutive_losses,
                'exec_id': exec_id,
                'label': label or 'UNKNOWN'  # ✨ 建议加上：让 CSV 有灵魂
            }
            self.trade_records.append(record)

            # --- 6. 物理增量落盘 (CSV) ---
            today_str = datetime.now(EASTERN_TZ).strftime('%Y%m%d')
            file_name = f"{self.symbol}_Trade_Activity_{today_str}.csv"
            
            df_item = pd.DataFrame([record])
            file_exists = os.path.isfile(file_name)
            df_item.to_csv(file_name, mode='a', index=False, header=not file_exists, encoding='utf-8')
            
            
        except Exception as e:
            self.log(f"❌ log_trade 财务对账或物理落盘异常: {e}", level="ERROR")

    def _save_audit_log(self, cond_code):
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
            
            # 文件路径：例如 logs/AAPL_2026-02-08.audit
            log_dir = "logs"
            if not os.path.exists(log_dir): os.makedirs(log_dir)
            file_path = os.path.join(log_dir, f"{self.symbol}_{datetime.now().strftime('%Y-%m-%d')}.audit")
            
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(log_line)
                
        except Exception as e:
            print(f"❌ 审计日志写入失败: {e}")

    def play_sound(self, sound_type: str):
        """[类方法] 感官报警系统：为不同交易事件分配独立音色 (移植自 06-9)"""
        try:
            if sound_type == "ENTRY":
                winsound.Beep(1200, 300)   # 入场：高音 (1200Hz)
            elif sound_type == "EXIT":
                winsound.Beep(800, 500)    # 出场：中音 (800Hz)
            elif sound_type == "ELEPHANT":
                # 大象柱发现：急促三连音
                for _ in range(3):
                    winsound.Beep(1500, 100)
            elif sound_type == "SUSPEND":
                # 熔断警报：长低音 (400Hz)
                winsound.Beep(400, 1000)
            elif sound_type == "ALERT":
                winsound.Beep(1000, 500)   # 一般预警
        except Exception:
            pass # 容错处理

    async def load_history_data(self): 
        """
        [数据中心-物理隔离重塑版 07-18]
        职责：
        1. 抓取历史 2min K线并存入物理隔离仓库 history_2min_bars（面包）。
        2. 严禁污染 raw_5s_buffer（面粉）。
        3. 锚定最后时刻为 last_hist_kline_time，作为实时补票的基准。
        """
        self.log(f"🚀 正在加载{self.symbol}的2分钟K线历史数据...", level="INFO")
        
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
                durationStr='3 D',
                barSizeSetting='2 mins', 
                whatToShow='TRADES', 
                useRTH=True,
                formatDate=1
            )
            
            # --- 3. 原子级审计区 ---
            raw_df = util.df(bars)
            if raw_df is None or raw_df.empty:
                self.log("❌ TWS 返回历史数据为空，尝试兜底...", level="ERROR")
                self.static_atr = 0.20 
                return False

            raw_df = standardize_df(raw_df) 
            if raw_df is None or raw_df.empty:
                return False

            # --- 4. 数据清洗与时区转换 ---
            target_col = next((c for c in ['datetime', 'date', 'time'] if c in raw_df.columns), None)
            if not target_col:
                self.log("🚨 无法找到datetime 时间列名", level="ERROR")
                return False

            # 强制转换与物理级 RTH 过滤 (09:30-15:58)
            raw_df[target_col] = pd.to_datetime(raw_df[target_col], utc=True).dt.tz_convert(EASTERN_TZ)
            raw_df = raw_df.set_index(target_col).between_time('09:30', '15:58').reset_index()
            raw_df.rename(columns={target_col: 'datetime'}, inplace=True)

            if raw_df.empty:
                self.log("⚠️ RTH 过滤后无有效历史数据", level="WARN")
                return False

            # --- 5. 静态 ATR 计算 (逻辑保持不变) ---
            raw_df['date_str'] = pd.to_datetime(raw_df['datetime']).dt.strftime('%Y-%m-%d')
            all_dates = sorted(raw_df['date_str'].unique())
            curr_date_str = now_et.strftime('%Y-%m-%d')
            
            if curr_date_str in all_dates and now_et.time() >= market_open_time:
                prev_date = all_dates[-2] if len(all_dates) >= 2 else all_dates[-1]
            else:
                prev_date = all_dates[-1]
            
            prev_day_data = raw_df[raw_df['date_str'] == prev_date]
            self.static_atr = round((prev_day_data['high'] - prev_day_data['low']).mean(), 4) if len(prev_day_data) >= 30 else 0.20
            
            # --- 6. 物理落地 CSV ---
            safe_time_str = end_time_str.replace(":", "-")
            K_LINE_FILE = f'{self.symbol}-2min-Kline-{safe_time_str}.csv'
            try:
                raw_df.to_csv(K_LINE_FILE, index=False)
            except Exception as e:
                self.log(f"⚠️ CSV 写入失败，错误代码: {e}", level="WARN")

            # --- 7. ✨ 核心物理隔离操作 ✨ ---
            
            # A. 填充 2min 成品仓库 (面包库)
            self.history_2min_bars = raw_df.copy()
            
            # B. 绝对排空 5s 缓冲区 (确保启动瞬间补票逻辑触发)
            self.raw_5s_buffer = pd.DataFrame() 
            
            # C. 锚定最后时刻 (去掉秒数，确保与 2min 节点严格对齐)
            last_bar_time = raw_df['datetime'].iloc[-1].replace(second=0, microsecond=0)
            # ✨ [开盘对齐补丁]：如果历史数据停留在昨日，且当前已接近或处于今日交易时段
            # 则强制将锚点对齐到今日 09:30，物理跳过隔夜空档，防止合成器刷屏
            now_et = datetime.now(EASTERN_TZ)
            today_market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            
            if last_bar_time < today_market_open:
                self.last_hist_kline_time = today_market_open
                self.log(f"⏰ 历史数据加载到({last_bar_time.strftime('%m-%d %H:%M')})，系统自动对齐至今日开盘锚点: {self.last_hist_kline_time.strftime('%H:%M')}", level="INFO")
            else:
                self.last_hist_kline_time = last_bar_time
            
            # D. 指标初始化对齐
            self.effective_atr = self.static_atr 

            # ==========================================================
            # ✨ [热机逻辑 - 插入此处] ✨
            # 职责：在正式开启实时探测前，将今日已发生的连涨/连跌趋势补齐到计数器中
            # ==========================================================
            today_str = datetime.now(EASTERN_TZ).strftime('%Y-%m-%d')
            dt_series = pd.to_datetime(raw_df['datetime'])
            # 仅提取今日 RTH 时段的数据进行模拟回放
            today_bars = raw_df[dt_series.dt.strftime('%Y-%m-%d') == today_str]
            
            # 初始化状态
            self.red_bar_count = 0
            self.green_bar_count = 0
            self.last_red_count = 0
            self.last_green_count = 0
            self.high_of_bounce = 0.0
            self.low_of_dip = 0.0

            if not today_bars.empty:
                for _, bar in today_bars.iterrows():
                    if bar['close'] > bar['open']:
                        if self.red_bar_count > 0:
                            self.last_red_count = self.red_bar_count
                            self.green_bar_count = 1
                            self.high_of_bounce = bar['high']
                            self.low_of_dip = 0.0
                        else:
                            self.green_bar_count += 1
                            self.high_of_bounce = max(self.high_of_bounce, bar['high'])
                        self.red_bar_count = 0
                    elif bar['close'] < bar['open']:
                        if self.green_bar_count > 0:
                            self.last_green_count = self.green_bar_count
                            self.red_bar_count = 1
                            self.low_of_dip = bar['low']
                            self.high_of_bounce = 0.0
                        else:
                            self.red_bar_count += 1
                            self.low_of_dip = min(self.low_of_dip, bar['low']) if self.low_of_dip > 0 else bar['low']
                        self.green_bar_count = 0
                self.last_processed_time = today_bars.iloc[-1]['datetime']
                self.log(f"🔥 热机完成：🔴红:{self.red_bar_count} | 🟢绿:{self.green_bar_count}", level="INFO")

            await self.update_15m_cache(is_init=True)
            self.calculate_indicators()

            # ✨ [热机终点线：在此处插入] ✨
            if not today_bars.empty:
                # 热机结束后，将指针推移到今日历史 Bar 的终点，防止合成器二次处理
                # 这行代码是解决“开盘刷屏”和“中途重启重复合成”的核武级加固
                self.last_hist_kline_time = today_bars.iloc[-1]['datetime'].replace(second=0, microsecond=0)
                self.log(f"🔥 态势感知热机完成：当前指针对齐至 {self.last_hist_kline_time}", level="INFO")

            # --- 8. 隔离效果审计日志 ---
            self.log(f"✅ {self.symbol}历史数据加载成功，加载{len(self.history_2min_bars)}根K线", level="INFO")
            
            
            return True

        except Exception as e:
            self.log(f"🚨 [Fatal Error] 2分钟K线历史数据加载失败，错误代码: {e}", level="CRITICAL")
            return False



    def save_trade_logs(self):
        """[系统层-归档中心] 导出非实时审计类数据 (大象柱 & K线)"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        
        # 1. 导出大象柱复盘报告
        if self.elephant_bar_log:
            pd.DataFrame(self.elephant_bar_log).to_csv(f"{self.symbol}-Elephant-{timestamp}.csv", index=False)
            
        # 2. 导出全天原始 K 线 (用于后期回测校验)
        if not self.history_2min_bars.empty:
            self.history_2min_bars.to_csv(f"{self.symbol}-FullDay-Kline-{timestamp}.csv", index=False)
            
        self.sys_log("💾 复盘数据包（大象柱/K线）归档成功。")


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
        duration = '3 D' if is_init else '1 D'
        
        try:
            # 错峰与请求 (保持 UTC 锚点对齐)
            end_time_str = datetime.now(timezone.utc).strftime("%Y%m%d %H:%M:%S UTC")
            bars = await self.ib.reqHistoricalDataAsync(
                self.contract, endDateTime=end_time_str, durationStr=duration,
                barSizeSetting='15 mins', whatToShow='TRADES', useRTH=True, formatDate=1
            )
            
            raw_df = util.df(bars)
            if raw_df is None or raw_df.empty: return False
            
            df = standardize_df(raw_df)
            if df is None or df.empty:
                self.log("⚠️ [15m同步] 标准化后的数据为空，跳过本次更新", level="WARN")
                return False # 安全退出，不执行后续报错代码
            
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert(EASTERN_TZ)

            # --- 🛠️ 3. 核心改进：无损增量缝合 (防止数据失血) ---
            if not hasattr(self, 'kline_cache_15m') or is_init:
                # 初始态或强制初始化：全量覆盖
                self.kline_cache_15m = df.copy()
            else:
                # 运行态：将 1D 新数据缝合进旧的 3D 背景
                self.kline_cache_15m = pd.concat([self.kline_cache_15m, df], ignore_index=True)
                # 物理去重并排序
                self.kline_cache_15m.drop_duplicates(subset=['datetime'], keep='last', inplace=True)
                self.kline_cache_15m.sort_values('datetime', inplace=True)
                # 保持 100 根左右的长度，足以应付 MA20/50 计算
                self.kline_cache_15m = self.kline_cache_15m.tail(100)

            # --- 🛡️ 4. 指标驱动：确保 15min 趋势实时更新 ---
            v_count = len(self.kline_cache_15m)
            if v_count >= 20:
                # 刷新 15min MA20 斜率、大象柱状态等
                self.check_15m_elephant_status()
                self.calculate_15m_trend(now_et)
                self.log(f"✅ 15min同步成功 | 模式:{'Init' if is_init else 'Live'} | 总样本:{v_count}", level="INFO")
            
            return True

        except Exception as e:
            self.log(f"🚨 15min同步异常: {e}", level="ERROR")
            return False


    def check_15m_elephant_status(self):
        """[Law #8 核心组件-加固版] 增加了数值审计日志，防止灵异判定"""
        # 1. 物理防御：严防 NoneType 崩溃
        if self.kline_cache_15m is None:
            return
        if self.kline_cache_15m.empty or len(self.kline_cache_15m) < 10:
            return

        last_15m = self.kline_cache_15m.iloc[-1]
        
        # 计算 15min 波动审计
        range_15m = round(last_15m['high'] - last_15m['low'], 2)
        avg_range_15m = round((self.kline_cache_15m['high'] - self.kline_cache_15m['low']).tail(20).mean(), 2)
        body_15m = round(abs(last_15m['close'] - last_15m['open']), 2)
        
        # 严格门槛：实体占比 > 70%，波动 > 1.8倍平均
        threshold_range = round(1.8 * avg_range_15m, 2)
        ratio = round(body_15m / range_15m if range_15m != 0 else 0, 2)
        
        is_eb_15m = (range_15m > threshold_range) and (ratio > 0.7)
        
        # 清除旧标志位（确保每一周期重新审计）
        self.ready_to_long_law8 = False
        self.ready_to_short_law8 = False

        if is_eb_15m:
            side = "BULL" if last_15m['close'] > last_15m['open'] else "BEAR"
            if side == "BULL":
                self.ready_to_long_law8 = True
            else:
                self.ready_to_short_law8 = True
            
            self.last_15m_eb_time = last_15m['datetime']
            self.log(f"🔥 [Law #8 审计通过] 15min {side} 大象! | 实体:{body_15m} | 门槛:{threshold_range} | 占比:{ratio}")
        else:
            # 可选：Debug 时输出为什么没触发
            # self.log(f"🔍 [Law #8 审计未触发] 15min 实体:{body_15m} (门槛:{threshold_range})")
            pass


    def calculate_15m_trend(self,ts: datetime):
        """[类方法] 计算 15min 大周期的趋势方向"""
        # 提示：
        # 1. 检查 self.kline_cache_15m 是否满足 20 根
        # 2. 将结果存入 self.is_15m_trending_up 和 self.is_15m_trending_down
        if isinstance(self.kline_cache_15m, pd.DataFrame) and len(self.kline_cache_15m) >= 20:
            last_sync_time = self.kline_cache_15m['datetime'].iloc[-1]
            # 计算当前 5s Bar 时间与最后一根 15min 柱时间的差值（秒）
            time_diff = (ts - last_sync_time).total_seconds()

            # 🛡️ 风险控制：如果 15min 数据延迟超过 22 分钟（1320秒），判定数据失效
            # 正常情况下，在 XX:15:05 同步后，time_diff 应该是 5-600 秒左右
            if time_diff > 1320:
                if ts.minute % 5 == 0: # 减少日志频率，每 5 分钟提醒一次
                    self.log(f"🚨 [Data Stale] 15min 官方数据过期! 延迟: {time_diff/60:.1f}min。共振判定将失效。")
                self.is_15m_trending_up = False
                self.is_15m_trending_down = False
            else:
                # 1. 计算均线序列
                self.ma20_15m_series = self.kline_cache_15m['close'].rolling(window=20).mean()
                
                # 2. 提取归一化数值 (统一使用 ma20_15m_ 前缀)
                self.ma20_15m_val = self.ma20_15m_series.iloc[-1]   # 当前 15min MA20
                self.ma20_15m_prev = self.ma20_15m_series.iloc[-2]  # 前一根 15min MA20
                
                # 3. 状态判定 (使用归一化后的变量进行比对)
                self.is_15m_trending_up = self.ma20_15m_val > self.ma20_15m_prev
                self.is_15m_trending_down = self.ma20_15m_val < self.ma20_15m_prev
                # ✨ 增加 Debug 日志，便于实时监控大周期态势
                trend_str = "UP 📈" if self.is_15m_trending_up else ("DOWN 📉" if self.is_15m_trending_down else "SIDE")
                self.log(f"ℹ️ 15分钟K线MA20趋势判断完成: {trend_str} (MA20: {self.ma20_15m_val:.3f})", level="DEBUG")

    def process_new_2min_bar(self, bar):
        """[中央处理器] 无论合成还是补票，每当得到一根新的2min K线之后，都调用这个函数"""
        # 🛡️ 物理防重放机制：如果传入的 K 线时间并不比现有的新，则直接无视
        if not self.history_2min_bars.empty:
            if bar['datetime'] <= self.history_2min_bars['datetime'].max():
                # 这种情况通常是重复合成或补票冲突，必须拦截，否则计数器会炸
                return
        # 1. 归档与排序
        new_row = pd.DataFrame([bar])
        self.history_2min_bars = pd.concat([self.history_2min_bars, new_row], ignore_index=True)
        self.history_2min_bars.drop_duplicates(subset=['datetime'], keep='last', inplace=True)
        self.history_2min_bars.sort_values('datetime', inplace=True)

        # 2. 跨线探测 (这是您最关心的 15min 对齐)
        new_minute = bar['datetime'].astimezone(EASTERN_TZ).minute
        is_cross_15 = (self.last_bar_minute is not None) and (self.last_bar_minute // 15 != new_minute // 15)
        self.last_bar_minute = new_minute # 更新锚点

        if is_cross_15:
            # 💡 跨线了！启动异步链条：同步大周期 -> 重新算指标 -> 重新审信号
            async def sync_and_recalc(snapshot_bar=bar):
                await load_vix_data_async(is_init=False)
                success_k = await self.update_15m_cache(is_init=False)
                if success_k:
                    self.calculate_indicators() # 数据拿到了再算，没拿到不瞎算
                    self._detect_market_patterns(snapshot_bar)
                    self.active_signal = self.analyze_signals(snapshot_bar['close'], shared.global_last_vix_close)
                    self.log(f"🔄 [跨15分钟线同步] 15min 背景已注入决策流")
            asyncio.create_task(sync_and_recalc(bar))
        else:
            # 💡 常规时刻：直接计算即可
            self.calculate_indicators()
            self._detect_market_patterns(bar)
            self.active_signal = self.analyze_signals(bar['close'], shared.global_last_vix_close)
            
    async def async_patch_ticket(self, st_time, ed_time):
        """[类方法] 异步补票并触发审计与对账"""
        try:
            # 1. 物理买票 (注意 ctx 变 self)
            patch_bars = await self.ib.reqHistoricalDataAsync(
                self.contract,
                endDateTime=ed_time.strftime('%Y%m%d %H:%M:%S'),
                durationStr='600 S', 
                barSizeSetting='2 mins',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            
            if patch_bars:
                target = next((b for b in patch_bars if b.date == st_time), None)
                if target:
                    new_bar = {
                        'datetime': st_time,
                        'open': target.open, 'high': target.high,
                        'low': target.low, 'close': target.close, 'volume': target.volume
                    }
                    
                    self.sys_log(f"✅ [补票成功] 读取 {st_time.strftime('%H:%M')} 2分钟K线操作完成", level="INFO")
                    
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
        current_bar_time = last_bar['datetime']

        # 🛡️ 时间戳哨兵：防止同一根 K 线被热机逻辑和实时逻辑重复计算
        if hasattr(self, 'last_processed_time') and self.last_processed_time == current_bar_time:
            return
        # 必须至少有 20 根 K 线才能开始计算 MA20
        data_len = len(self.history_2min_bars)
        if data_len < 20: 
            return

        df = self.history_2min_bars

        try:
            # --- 2. 均线分级计算 (对齐 07-18 逻辑) ---
            ma20_series = df['close'].rolling(window=20).mean()
            ma8_series = df['close'].rolling(window=8).mean()
            
            self.ma20 = ma20_series.iloc[-1]
            self.ma8 = ma8_series.iloc[-1]
            self.ma20_prev = ma20_series.iloc[-2]
            self.ma20_prev2 = ma20_series.iloc[-3]

            # --- 3. MA200 与超级趋势判定 (SuperTrend) ---
            if data_len >= 200:
                self.ma200 = df['close'].rolling(window=200).mean().iloc[-1]
                curr_price = df['close'].iloc[-1]
                # A. 基础微观判定 (2min 三线顺排)
                micro_uptrend = (curr_price > self.ma8) and (self.ma8 > self.ma20) and (self.ma20 > self.ma200)
                micro_downtrend = (curr_price < self.ma8) and (self.ma8 < self.ma20) and (self.ma20 < self.ma200)
                
                # B. 引入战略过滤器 (15min 趋势同步)
                # 获取 15min 标志位，如果没有数据（None/False）则默认为不通过（保守策略）
                macro_up_confirm = getattr(self, 'is_15m_trending_up', False)
                macro_down_confirm = getattr(self, 'is_15m_trending_down', False)

                # C. 最终共振判定
                self.is_super_uptrend = micro_uptrend and macro_up_confirm
                self.is_super_downtrend = micro_downtrend and macro_down_confirm
                
                # ✨ 可选：增加审计日志，便于观察为何 SuperTrend 没激活
                if micro_uptrend and not macro_up_confirm:
                    self.sys_log(f"⚠️ [趋势背离] 2min已排布，但15min未转向向上，SuperUptrend拦截", level="DEBUG")
            else:
                self.ma200 = None
                self.is_super_uptrend = False
                self.is_super_downtrend = False

            # --- 4. MA20 方向转折实证日志 ---
            self.is_ma20_turning_up = (self.ma20 > self.ma20_prev) and (self.ma20_prev >= self.ma20_prev2)
            if self.is_ma20_turning_up:
                self.sys_log(f"🔼 MA20 正在向上 | 当前: {self.ma20:.3f} | 前一: {self.ma20_prev:.3f} | 前二: {self.ma20_prev2:.3f}", level="INFO")
          
            self.is_ma20_turning_down = (self.ma20 < self.ma20_prev) and (self.ma20_prev <= self.ma20_prev2)
            if self.is_ma20_turning_down:
                self.sys_log(f"🔽 MA20 正在向下 | 当前: {self.ma20:.3f} | 前一: {self.ma20_prev:.3f} | 前二: {self.ma20_prev2:.3f}", level="INFO")

            # --- 5. [重构版] 三级 ATR 融合算法 (防止大象柱污染) ---
            base_atr = getattr(self, 'static_atr', 0.5)  # 昨日基准
            dynamic_atr = (df['high'] - df['low']).rolling(window=14).mean().iloc[-1] # 瞬时体温

            now_time = datetime.now(EASTERN_TZ).time()

            if now_time < time(10, 0):
                # 🛡️ 最佳实践：在 10:00 之前，限制动态 ATR 的“破坏力”
                # 强制动态值不能超过基准值的 1.5 倍，防止大象柱瞬间拉大止损空间
                capped_dynamic = min(dynamic_atr, base_atr * 1.5)
                # 使用 7:3 比例混合，基准占大头
                self.effective_atr = round((base_atr * 0.7) + (capped_dynamic * 0.3), 4)
                self.sys_log(f"🧬 [ATR平滑] 早盘防御模式: 物理上限拦截生效", level="DEBUG")
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
            self.avg_range_20 = (prev_20['high'] - prev_20['low']).mean()
            self.avg_volume = prev_20['volume'].mean()

            # --- 7. 实证日志 ---
            self.sys_log(f"📊 前14根K线的动态ATR=:{dynamic_atr:.3f} |前一交易日195根K线的静态ATR=:{base_atr:.3f} | 最终采用值:{self.effective_atr}", level="DEBUG")

            # --- 8. 计算连续红绿柱计数 ---
            if not self.history_2min_bars.empty:
                last_bar = self.history_2min_bars.iloc[-1]
                if last_bar['close'] > last_bar['open']: # 🟢 变绿
                    if self.red_bar_count > 0: # 瞬时转折：由红转绿第一根
                        self.last_red_count = self.red_bar_count # 备份压抑红柱数
                        self.green_bar_count = 1                 # ✨ 恢复：确认为第一根反转
                        self.high_of_bounce = last_bar['high']    # 初始化上涨极值
                        self.low_of_dip = 0.0                     # 结束下跌，重置锚点
                        self.log(f"📊 红绿柱记数器|🔴连续下跌:{self.last_red_count}根红柱之后，出现第1根上涨🟢绿柱")
                    else: # 持续上涨
                        self.last_red_count = 0 
                        self.green_bar_count += 1
                        # ✨ 动态维护：确保记录波段最高点
                        self.high_of_bounce = max(self.high_of_bounce, last_bar['high']) if self.high_of_bounce > 0 else last_bar['high']
                    
                    self.red_bar_count = 0

                elif last_bar['close'] < last_bar['open']: # 🔴 变红
                    if self.green_bar_count > 0: # 瞬时转折：由绿转红第一根
                        self.last_green_count = self.green_bar_count # 备份上涨绿柱数
                        self.red_bar_count = 1                       # ✨ 对称恢复：确认为第一根反转
                        self.low_of_dip = last_bar['low']             # 初始化下跌极值
                        self.high_of_bounce = 0.0                     # 结束上涨，重置锚点
                        self.log(f"📊 红绿柱记数器|🟢连续上涨:{self.last_green_count}根绿柱之后，出现第1根下跌🔴红柱")
                    else: # 持续下跌
                        self.last_green_count = 0
                        self.red_bar_count += 1
                        # ✨ 动态维护：确保记录波段最低点
                        self.low_of_dip = min(self.low_of_dip, last_bar['low']) if self.low_of_dip > 0 else last_bar['low']

                    self.green_bar_count = 0
                    
                else:
                    # 平盘（十字星）：通常 Velez 建议维持现状或计数停滞
                    self.log(f"📊 红绿柱记数器|这是一根十字星K线，之前有:🔴连续下跌红柱:{self.red_bar_count}根 | 🟢连续上涨绿柱:{self.green_bar_count}根")
                    pass
                self.last_processed_time = current_bar_time    
                
        except Exception as e:
            self.sys_log(f"🚨 [指标计算异常] {self.symbol}: {e}", level="ERROR")
    

    def _detect_law1(self, new_bar):
        """
        [Law #1] Elephant Bar (扳机探测器)
        职责：识别统计学意义上的巨型实体，驱动下单偏见。
        标准：
        1. 波动 (Range) > 2.0 * effective_atr (或 avg_range_20 * 2.0)
        2. 实体 (Body) > 80% 占比 (无长影线)
        3. 成交量 (Volume) > 1.2 * avg_volume (机构实证)
        """
        # 初始化标志位
        self.ready_to_long_law1 = False
        self.ready_to_short_law1 = False

        # 1. 物理审计：确保计算基础已就绪
        if not hasattr(self, 'effective_atr') or not hasattr(self, 'avg_range_20'):
            return

        # 2. 基础数值计算
        bar_range = new_bar['high'] - new_bar['low']
        body_size = abs(new_bar['close'] - new_bar['open'])
        
        # 3. 核心门槛对账
        # 使用我们融合后的 effective_atr 作为核心防御门槛
        volatility_threshold = self.eb_range_mult * self.effective_atr  # 替换 2.0
        body_ratio = body_size / bar_range if bar_range != 0 else 0
        vol_ratio = new_bar['volume'] / self.avg_volume if self.avg_volume > 0 else 0
        # 4. 判定逻辑：必须满足 波动、实体、成交量 三项审计
        is_big_enough = body_size > volatility_threshold
        is_solid = body_ratio >= self.eb_body_ratio
        is_high_volume = vol_ratio >= self.eb_vol_mult

        # 修正后的逻辑结构
        if is_solid and is_big_enough:
            if new_bar['close'] > new_bar['open']:
                # 牛市逻辑...
                self.ready_to_long_law1 = True
                self.law1_sl = new_bar['low']  # 固化大象柱底端 (Option A)
                self.log(f"🐘 [Law#1 匹配] 🟢 Bull Elephant Bar! | 实体:{body_size:.3f} (阈值:{volatility_threshold:.3f})", level="INFO")
                #if not is_high_volume:
                #    self.log(f"   ⚠️ 警告: 成交量未达{self.eb_vol_mult}倍门槛", level="WARN")
            elif new_bar['close'] < new_bar['open']: # ✨ 增加明确的方向判断
                # 熊市逻辑...
                self.ready_to_short_law1 = True
                self.law1_sl = new_bar['high'] # 固化大象柱顶端 (Option A)
                self.log(f"🐘 [Law#1 匹配] 🔴 Bear Elephant Bar! | 实体:{body_size:.3f} (阈值:{volatility_threshold:.3f})", level="INFO")
                #if not is_high_volume:
                #    self.log(f"   ⚠️ 警告: 成交量未达{self.eb_vol_mult}倍门槛", level="WARN")


    def _detect_law2(self, new_bar):
        """
        [Law #2] Color Change (颜色反转探测器)
        职责：识别在连续下跌/上涨压抑后的第一根反向 K 线。
        标准：
        1. 压抑根数 >= 3 (Velez 经典定义：3-5 根同色柱为 Extended)
        2. 反转柱质量：实体 > 0.5 * effective_atr (防止噪音变色)
        """
        # 1. 初始化标志位
        self.ready_to_long_law2 = False
        self.ready_to_short_law2 = False

        # 2. 物理审计：确保计算基础已就绪
        if not hasattr(self, 'effective_atr'):
            return
            
        # 3. 基础数值提取
        is_green = new_bar['close'] > new_bar['open']
        is_red = new_bar['close'] < new_bar['open']
        body_size = abs(new_bar['close'] - new_bar['open'])

        # 定义变色柱的“质量门槛”（至少要有动态波动 50% 的实体，避免微弱闪烁）
        quality_threshold = self.effective_atr * 0.5

        # --- 4.1. 多头判定：3红变绿 ---
        if is_green:
            if 3 <= self.last_red_count <= 5 and self.green_bar_count == 1:
                if body_size >= quality_threshold:
                    self.ready_to_long_law2 = True
                    self.law2_sl = new_bar['low']
                    self.log(f"🌈 [Law#2信号] 🟢 Bull Color Change! | 压抑红柱:{self.last_red_count}根 | 反转实体:{body_size:.3f}", level="INFO")
                else:
                    pass
                    #self.log(f"🔍 [Law#2忽略] 3根压抑后变色但反转无力 ({body_size:.3f} < {quality_threshold:.3f})", level="DEBUG")
            elif self.last_red_count == 2:
                pass
                # 仅做审计记录，不触发 ready_to_long
                #self.log(f"👀 [Law#2观察] 仅2根红柱后的变色，不符合 Velez 3根压抑原则，跳过。", level="DEBUG")

        # --- 4.2. 空头判定：3绿变红 ---
        elif is_red:
            if 3 <= self.last_green_count <= 5 and self.red_bar_count == 1:
                if body_size >= quality_threshold:
                    self.ready_to_short_law2 = True
                    self.law2_sl = new_bar['high']
                    self.log(f"🌈 [Law#2信号] 🔴 Bear Color Change! | 压抑绿柱:{self.last_green_count}根 | 反转实体:{body_size:.3f}", level="INFO")
                else:
                    pass
                    #self.log(f"🔍 [Law#2忽略] 3根压抑后变色但反转无力 ({body_size:.3f} < {quality_threshold:.3f})", level="DEBUG")
            elif self.last_green_count == 2:
                pass
                #self.log(f"👀 [Law#2观察] 仅2根绿柱后的变色，不符合 Velez 3根压抑原则，跳过。", level="DEBUG")


    def _detect_law3(self, new_bar):
        """
        [Law #3] 3-5 Bars (深度回调探测器 - 工业加固版)
        职责：识别 3-5 根红柱回调至 20MA 支撑位后的反弹触发点。
        """
        self.ready_to_long_law3 = False
        self.ready_to_short_law3 = False

        if not hasattr(self, 'effective_atr'): return
        if self.ma20 is None: return
        if len(self.history_2min_bars) < 2: return

        prev_bar = self.history_2min_bars.iloc[-2]
        curr_price = new_bar['close']
        dist_to_ma20 = abs(curr_price - self.ma20)
        near_threshold = self.effective_atr * 1.0 
        
        is_green = new_bar['close'] > new_bar['open']
        is_red = new_bar['close'] < new_bar['open']

        # --- 修改 _detect_law3 中的判断条件 ---
        # 3.1 多头判定 (Bull Pullback)
        # 逻辑：当前是反转绿柱(is_green)，且“刚刚结束”的红柱丛(last_red_count)在3-5之间
        if is_green and self.green_bar_count == 1 and 3 <= self.last_red_count <= 5 and curr_price > self.ma20:
            if new_bar['high'] > prev_bar['high']:
                if dist_to_ma20 <= near_threshold:
                    self.ready_to_long_law3 = True
                    self.law3_sl = self.low_of_dip # 引用 Law 3 审计中记录的回调波段最低点
                    self.log(f"🎯 [Law#3] 🟢Bull Pullback! | 确认回调:{self.last_red_count}根红柱|当前K线越过前高:{new_bar['high']:.3f} > {prev_bar['high']:.3f}", level="INFO")
                else:
                    pass
                    #self.log(f"🔍 [Law#3审计] 满足3-5根确认，但当前K线距离MA20还是太远 [dist_to_ma20]=({dist_to_ma20:.3f} > [1倍ATR]{near_threshold:.3f})", level="DEBUG")

        # 3.2 空头判定 (Bear Rally)
        # 逻辑：当前是反转红柱(is_red)，且“刚刚结束”的绿柱丛(last_green_count)在3-5之间
        elif is_red and self.red_bar_count == 1 and 3 <= self.last_green_count <= 5 and curr_price < self.ma20:
            if new_bar['low'] < prev_bar['low']:
                if dist_to_ma20 <= near_threshold:
                    self.ready_to_short_law3 = True
                    self.law3_sl = self.high_of_bounce # 引用 Law 3 审计中记录的反弹波段最高点
                    self.log(f"🎯 [Law#3] 🔴Bear Rally! | 确认反弹:{self.last_green_count}根绿柱|当前K线越过前低:{new_bar['low']:.3f} < {prev_bar['low']:.3f}", level="INFO")
                else:
                    pass
                    #self.log(f"🔍 [Law#3审计] 满足3-5根确认，但当前K线距离MA20还是太远 [dist_to_ma20]=({dist_to_ma20:.3f} > [1倍ATR]{near_threshold:.3f})", level="DEBUG")



    def _detect_law4(self, new_bar):
        """
        [Law #4] RBI/GBI (忽略柱探测器 - 趋势宽松版)
        职责：在普通趋势中捕捉1-2根反向柱的短暂洗盘，无需超级趋势约束。
                
        标准（Velez原始教学）：
        1. 趋势：20MA方向向上（多头）或向下（空头）即可，不要求Price>MA8>MA20>MA200
        2. 计数：连续反向柱仅限1-2根（RBI: 1-2根红柱后变绿；GBI: 1-2根绿柱后变红）
        3. 触发：当前K线High/Low突破前一根反向柱的极端值
        """        
        self.ready_to_long_law4 = False
        self.ready_to_short_law4 = False

        # 物理审计
        if not hasattr(self, 'effective_atr'): return  
        if self.ma20 is None: return
        if len(self.history_2min_bars) < 2: return

        prev_bar = self.history_2min_bars.iloc[-2]
        if not (pd.notna(new_bar['high']) and pd.notna(prev_bar['high'])):
            return
        # --- 4.1 多头判定 (RBI: Red Bar Ignored) ---
        # ✅ 修正：仅需MA20向上趋势（普通趋势），不再强制Super Trend
        #is_uptrend = self.is_ma20_turning_up  # 或可扩展：or (self.ma20 is not None and new_bar['close'] > self.ma20)
        # 多头判定增强（二选一或组合）
        is_uptrend = (
            self.is_ma20_turning_up or 
            (self.ma20 is not None and new_bar['close'] > self.ma20)
        )
        if is_uptrend and 1 <= self.last_red_count <= 2 and self.green_bar_count == 1:
            # 核心触发：当前价格(或最高点)冲破了刚才那根红柱的高点
            if new_bar['high'] > prev_bar['high']:
                self.ready_to_long_law4 = True
                self.law4_sl = prev_bar['low']  # 固化前一根被忽略红柱的低点
                trend_type = "SuperTrend" if self.is_super_uptrend else "NormalTrend"
                self.log(
                    f"⚡ [Law#4-RBI] 🟢 忽略红柱触发! | 趋势:{trend_type} | "
                    f"压抑:{self.last_red_count}根红柱 | 突破:{new_bar['high']:.3f} > {prev_bar['high']:.3f}",
                    level="INFO"
                )
            else:
                pass
                #self.log(
                #    f"🔍 [Law#4-RBI] 普通上升趋势中发现忽略柱候选，等待突破前高 {prev_bar['high']:.3f}",
                #    level="DEBUG"
                #)

        # --- 4.2 空头判定 (GBI: Green Bar Ignored) ---
        # ✅ 修正：仅需MA20向下趋势（普通趋势），不再强制Super Trend
        #is_downtrend = self.is_ma20_turning_down  # 或可扩展：or (self.ma20 is not None and new_bar['close'] < self.ma20)
        is_downtrend = (
            self.is_ma20_turning_down or 
            (self.ma20 is not None and new_bar['close'] < self.ma20)
        )
        if is_downtrend and 1 <= self.last_green_count <= 2 and self.red_bar_count == 1:
            if new_bar['low'] < prev_bar['low']:
                self.ready_to_short_law4 = True
                self.law4_sl = prev_bar['high']  # 固化前一根被忽略绿柱的高点
                trend_type = "SuperTrend" if self.is_super_downtrend else "NormalTrend"
                self.log(
                    f"⚡ [Law#4-GBI] 🔴 忽略绿柱触发! | 趋势:{trend_type} | "
                    f"压抑:{self.last_green_count}根绿柱 | 突破:{new_bar['low']:.3f} < {prev_bar['low']:.3f}",
                    level="INFO"
                )
            else:
                pass
                #self.log(
                #    f"🔍 [Law#4-GBI] 普通下降趋势中发现忽略柱候选，等待跌破前低 {prev_bar['low']:.3f}",
                #    level="DEBUG"
                #)

    def _detect_law5(self, new_bar):
        """
        [Law #5] 20MA Cross (均线穿越探测器 - 工业加固版)
        职责：识别具有统计学力量的 20MA 物理穿越信号。        
        标准：
        1. 物理穿越：收盘价从均线一侧运行至另一侧。
        2. 强度审计：实体大小 (Body) 必须 > 0.5 * effective_atr (防止噪音)。
        3. 大象特权：若实体 > 2.0 * effective_atr，视为强力机构信号。
        """
        self.ready_to_long_law5 = False
        self.ready_to_short_law5 = False

        # 1. 物理审计
        if not hasattr(self, 'effective_atr'): return
        if self.ma20 is None:          return
        if self.ma20_prev is None:     return 
        if len(self.history_2min_bars) < 2:  return

        # 2. 基础数据提取
        prev_close = self.history_2min_bars['close'].iloc[-2]
        curr_close = new_bar['close']
        body_size = abs(new_bar['close'] - new_bar['open'])
        
        # 定义门槛
        noise_threshold = self.effective_atr * 0.5  # 您的硬性过滤要求
        elephant_threshold = self.effective_atr * 2.0 # 大象柱门槛

        # 3. 核心判定逻辑
        # --- 3.1 多头穿越 (Bull Cross) ---
        if prev_close <= self.ma20_prev and curr_close > self.ma20:
            if body_size > noise_threshold:
                self.ready_to_long_law5 = True
                self.law5_sl = new_bar['low']  # 固化穿越柱的低点
                # 区分日志等级
                if body_size >= elephant_threshold:
                    tag = "🔥 [Law#5信号] 🟢 Elephant Power 20MA Cross!"
                else:
                    tag = "🎯 [Law#5信号] 🟢 Standard 20MA Cross"
                    
                self.log(f"{tag} | 实体:{body_size:.3f} | MA20:{self.ma20:.3f} | 穿越点对账:{prev_close:.3f}->{curr_close:.3f}", level="INFO")
            else:
                pass
                #self.log(f"🔍 [Law#5忽略] 发现多头交叉但力度过弱 ({body_size:.3f} < {noise_threshold:.3f})", level="DEBUG")

        # --- 3.2 空头穿越 (Bear Cross) ---
        elif prev_close >= self.ma20_prev and curr_close < self.ma20:
            if body_size > noise_threshold:
                self.ready_to_short_law5 = True
                self.law5_sl = new_bar['high']  # 固化穿越柱的高点  
                if body_size >= elephant_threshold:
                    tag = "🔥 [Law#5信号] 🔴 Elephant Power 20MA Cross!"
                else:
                    tag = "🎯 [Law#5信号] 🔴 Standard 20MA Cross"
                    
                self.log(f"{tag} | 实体:{body_size:.3f} | MA20:{self.ma20:.3f} | 穿越点对账:{prev_close:.3f}->{curr_close:.3f}", level="INFO")
            else:
                pass
                #self.log(f"🔍 [Law#5忽略] 发现空头交叉但力度过弱 ({body_size:.3f} < {noise_threshold:.3f})", level="DEBUG")


    def _detect_law6(self, new_bar):
        """
        [Law #6] Home Run (均线影线反转探测器 - 工业加固版)
        职责：识别回踩并穿透 20MA 后，形成的剧烈拒绝影线信号。
        
        标准：
        1. 形态：影线长度 >= 2 * 实体长度 (长影线审计)。
        2. 穿透：影线尖端(Low/High) 必须穿过 20MA (物理触碰确认)。
        3. 收盘：BT 必须收在 20MA 之上，TT 必须收在 20MA 之下。
        """
        self.ready_to_long_law6 = False
        self.ready_to_short_law6 = False

        # 1. 物理审计
        if not hasattr(self, 'effective_atr'): return 
        if self.ma20 is None: return
        
        # 2. 基础形态数据计算
        high, low, open_p, close = new_bar['high'], new_bar['low'], new_bar['open'], new_bar['close']
        body_size = abs(close - open_p)
        upper_tail = high - max(open_p, close)
        lower_tail = min(open_p, close) - low
        
        
        if body_size < self.effective_atr * 0.5:
            return  # 实体过小，直接过滤（避免伪信号）
        # 3. 核心判定逻辑
        # --- 3.1 多头判定 (Bottoming Tail - BT) ---
        # A. 形态审计：下影线必须是实体的 2 倍以上，且具备一定的绝对长度
        is_bt_shape = (lower_tail >= 2.0 * body_size) and (lower_tail > 0.3 * self.effective_atr)
        
        if is_bt_shape:
            # B. 穿透审计：Low 必须低于 20MA (穿过均线)
            has_penetrated = low < self.ma20
            # C. 收盘审计：收盘必须拉回到 20MA 之上 (拒绝成功)
            has_recovered = close > self.ma20
            
            if has_penetrated and has_recovered:
                self.ready_to_long_law6 = True
                self.law6_sl = new_bar['low']  # 固化长影线的最尖端
                ratio = lower_tail / body_size
                
                self.log(
                    f"⚾ [Law#6]🟢 Home Run (BT)|影线比:{ratio:.1f}x | "
                    f"实体:{body_size:.3f} (ATR:{self.effective_atr:.3f}) | "  # ✅ 增加ATR参考
                    f"Low:{low:.3f} < MA20:{self.ma20:.3f} < Close:{close:.3f}",
                    level="INFO"
                )
            else:
                pass
                #self.log(f"🔍 [Law#6审计] 发现BT形态但未满足穿透收回条件 | Low:{low:.3f} | Close:{close:.3f}", level="DEBUG")

        # --- 3.2 空头判定 (Topping Tail - TT) ---
        # A. 形态审计：上影线必须是实体的 2 倍以上
        is_tt_shape = (upper_tail >= 2.0 * body_size) and (upper_tail > 0.3 * self.effective_atr)
        
        if is_tt_shape:
            # B. 穿透审计：High 必须高于 20MA
            has_penetrated = high > self.ma20
            # C. 收盘审计：收盘必须压回到 20MA 之下
            has_rejected = close < self.ma20
            
            if has_penetrated and has_rejected:
                self.ready_to_short_law6 = True
                self.law6_sl = new_bar['high'] # 固化长影线的最尖端
                ratio = upper_tail / body_size
                self.log(f"⚾ [Law#6信号] 🔴 Home Run (TT)! | 影线比:{ratio:.1f}x | 实体:{body_size:.3f} | High:{high:.3f} > MA20:{self.ma20:.3f} > Close:{close:.3f}", level="INFO")
            else:
                pass
                #self.log(f"🔍 [Law#6审计] 发现TT形态但未满足穿透压回条件", level="DEBUG")



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
        [Law #8] Fab 42 (趋势共振探测器 - 共振宽松版)
        职责：识别15m大周期趋势方向与2m突破动作的协同，不再强制要求2m MA20斜率同步。
        
        标准（Velez原始教学）：
        1. 大周期趋势：15min MA20方向向上（多头）或向下（空头）即可
        2. 小周期突破：当前K线为第一根变色柱（Color Change）且实体力度 > 0.5×ATR
        3. 空间约束：价格偏离8MA ≤ 1.5×ATR（防止追涨杀跌）
        4. 【关键修正】不再要求2min MA20斜率与15min同步，仅需15min趋势方向确认
        """
        self.ready_to_long_law8 = False
        self.ready_to_short_law8 = False

        # 1. 物理审计
        if not hasattr(self, 'effective_atr'): return 
        if self.ma20 is None: return
        if self.ma8 is None: return
        
        # 2. 基础数据计算
        curr_price = new_bar['close']
        body_size = abs(new_bar['close'] - new_bar['open'])
        dist_to_ma8 = abs(curr_price - self.ma8)
        
        # 门槛设定
        extension_limit = self.effective_atr * 1.5  # 偏离极限
        power_threshold = self.effective_atr * 0.5   # 变色力度门槛
        
        is_green = new_bar['close'] > new_bar['open']
        is_red = new_bar['close'] < new_bar['open']
        prev_bar = self.history_2min_bars.iloc[-2]
        if not (pd.notna(new_bar['high']) and pd.notna(prev_bar['high'])):
            return
        # 3. 核心判定逻辑
        
        # --- 3.1 多头共振判定 (Bull Resonance) ---
        if self.is_15m_trending_up :
            # A. 空间审计：检查是否过度拉升
            if dist_to_ma8 <= extension_limit:
                # B. 触发审计：变色确认且有力度
                if is_green and self.green_bar_count == 1 and body_size > power_threshold:
                    self.ready_to_long_law8 = True
                    self.law8_sl = new_bar['low']
                    # ✨ 增强日志：标注2min MA20实际状态（便于回测分析）
                    ma20_status = "2m-MA20↑" if self.is_ma20_turning_up else "2m-MA20→/↓"
                    self.log(
                        f"🚀 [Law#8-Fab42] 🟢 共振触发! | 15m:UP | {ma20_status} | "
                        f"实体:{body_size:.3f} | 偏离8MA:{dist_to_ma8:.3f}",
                        level="INFO"
                    )
                    #self.log(f"🚀 [Law#8信号] 🟢Fab42 首次共振触发!!|15m/2m同步向上|实体:{body_size:.3f}|偏离8MA:{dist_to_ma8:.3f}", level="INFO")
                else:
                    # 未触发原因诊断（调试友好）
                    if not is_green:
                        reason = "非绿柱"
                    elif self.green_bar_count != 1:
                        reason = f"非首根({self.green_bar_count}连涨)"
                    else:
                        reason = f"力度不足({body_size:.3f} < {power_threshold:.3f})"
                    #self.log(f"🔍 [Law#8] 15m趋势向上但未触发: {reason}", level="DEBUG")
            else:
                pass
                #self.log(
                #    f"🔍 [Law#8] 15m趋势向上但偏离8MA过远({dist_to_ma8:.3f} > {extension_limit:.3f})，放弃追高",
                #    level="DEBUG"
                #)

        # --- 3.2 空头共振判定 (Bear Resonance) ---
        elif self.is_15m_trending_down:
            if dist_to_ma8 <= extension_limit:
                if is_red and self.red_bar_count == 1 and body_size > power_threshold:
                    self.ready_to_short_law8 = True
                    self.law8_sl = new_bar['high']
                    ma20_status = "2m-MA20↓" if self.is_ma20_turning_down else "2m-MA20→/↑"
                    self.log(
                        f"🚀 [Law#8-Fab42] 🔴 共振触发! | 15m:DOWN | {ma20_status} | "
                        f"实体:{body_size:.3f} | 偏离8MA:{dist_to_ma8:.3f}",
                        level="INFO"
                    )
                else:
                    if not is_red:
                        reason = "非红柱"
                    elif self.red_bar_count != 1:
                        reason = f"非首根({self.red_bar_count}连跌)"
                    else:
                        reason = f"力度不足({body_size:.3f} < {power_threshold:.3f})"
                    #self.log(f"🔍 [Law#8] 15m趋势向下但未触发: {reason}", level="DEBUG")
            else:
                pass
                #self.log(
                #    f"🔍 [Law#8] 15m趋势向下但偏离8MA过远({dist_to_ma8:.3f} > {extension_limit:.3f})，放弃杀跌",
                #    level="DEBUG"
                #)


    def _detect_180_reversal(self, new_bar):
        """
        [Law V180] 吞没性反转探测器 (工业加固版)
        职责：识别实体 100% 覆盖且带有“洗盘刺破”动作的 180 度强力反转。
        
        标准：
        1. 颜色：前红后绿 (Long) 或 前绿后红 (Short)。
        2. 实体：当前实体 >= 前一实体 (100% 吞没)。
        3. 刺破：当前 Low/High 必须触及或超越前一根的极端值。
        4. 位置：收盘价距离 20MA 必须在 1.5 * effective_atr 以内。
        """
        self.ready_to_long_180 = False
        self.ready_to_short_180 = False

        # 1. 物理审计
        if not hasattr(self, 'effective_atr'): return
        if self.ma20 is None: return
        if len(self.history_2min_bars) < 2: return

        prev_bar = self.history_2min_bars.iloc[-2]
        
        # 2. 基础数据计算
        body_prev = abs(prev_bar['close'] - prev_bar['open'])
        body_curr = abs(new_bar['close'] - new_bar['open'])
        if body_curr <= 0.04: return
        dist_to_ma20 = abs(new_bar['close'] - self.ma20)
        
        # 门槛设定
        near_ma_limit = self.effective_atr * 1.5
        min_body_threshold = self.effective_atr * 0.4 # 防止极小碎步的微观吞没

        # 3. 核心判定逻辑
        
        is_bull_180_shape = (prev_bar['close'] < prev_bar['open']) and \
                            (new_bar['close'] > new_bar['open']) and \
                            (body_curr >= body_prev)
        is_bear_180_shape = (prev_bar['close'] > prev_bar['open']) and \
                            (new_bar['close'] < new_bar['open']) and \
                            (body_curr >= body_prev)
        # --- 3.1 Bull 180 (多头吞没) ---
        if is_bull_180_shape and body_curr > min_body_threshold:
            # A.物理约束：当前 Low 触及或低于前低 (洗盘确认)
            has_washout = new_bar['low'] <= prev_bar['low']
            # B. ✨ 核心位置审计补丁,位置约束：靠近均线
            # 如果是 Super Uptrend，忽略均线距离；否则，必须在 1.5 ATR 内
            is_location_valid = self.is_super_uptrend or (dist_to_ma20 <= near_ma_limit)

            if has_washout and is_location_valid:
                self.ready_to_long_180 = True
                # 固化这两根K线中的最低点（刺破点）
                self.v180_sl = min(new_bar['low'], prev_bar['low'])
                status = "SuperTrend-Air" if self.is_super_uptrend else "MA20-Support"
                self.log(f"🔄 [V180信号] 🟢 Bull 180! | 吞没率:{(body_curr/body_prev*100):.0f}% | 距MA20:{dist_to_ma20:.3f} | 已完成洗盘刺破", level="INFO")
            else:
                pass
                #self.log(f"🔍 [V180审计] 满足吞没但位置不符 (距离:{dist_to_ma20:.3f} 且非超级趋势)", level="DEBUG")

        # --- 3.2 Bear 180 (空头吞没) ---
        if is_bear_180_shape and body_curr > min_body_threshold:
            # A. 物理约束：当前 High 触及或高于前高 (洗盘确认)
            has_washout = new_bar['high'] >= prev_bar['high']
            # B. ✨ 核心位置审计补丁,位置约束：靠近均线
            is_location_valid = self.is_super_downtrend or (dist_to_ma20 <= near_ma_limit)

            if has_washout and is_location_valid:
                self.ready_to_short_180 = True
                # 固化这两根K线中的最高点
                self.v180_sl = max(new_bar['high'], prev_bar['high'])
                status = "SuperTrend-Air" if self.is_super_downtrend else "MA20-Resistance"
                self.log(f"🔄 [V180信号] 🔴 Bear 180! | 吞没率:{(body_curr/body_prev*100):.0f}% | 距MA20:{dist_to_ma20:.3f} | 已完成洗盘刺破", level="INFO")
            else:
                pass
                #self.log(f"🔍 [V180审计] 满足吞没但位置不符 (距离:{dist_to_ma20:.3f} 且非超级趋势)", level="DEBUG")
        

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
        if new_bar is None: return 
        if not hasattr(self, 'effective_atr'): return
            
        high, low = new_bar['high'], new_bar['low']
        open_p, close_p = new_bar['open'], new_bar['close']
        
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
        if lower_shadow >= (body_size * SHADOW_TO_BODY_RATIO) and \
           lower_shadow >= (total_range * SHADOW_TO_TOTAL_RATIO) and \
           lower_shadow > ABS_STRENGTH_LIMIT:
            
            # 纯净度审计：下影线必须是上影线的 3 倍以上
            if lower_shadow >= (max(upper_shadow, 0.001) * PURITY_RATIO):
                self.current_tail_type = "BT"
                shadow_pct = (lower_shadow / total_range) * 100
                self.log(f"🕵️ [Tail-BT] 发现下影反转 | 占比:{shadow_pct:.1f}% | 纯度确认", level="DEBUG")
                return

        # --- B. 顶部影线 TT (Topping Tail) 判定 ---
        if upper_shadow >= (body_size * SHADOW_TO_BODY_RATIO) and \
           upper_shadow >= (total_range * SHADOW_TO_TOTAL_RATIO) and \
           upper_shadow > ABS_STRENGTH_LIMIT:
            
            # 纯净度审计：上影线必须是下影线的 3 倍以上
            if upper_shadow >= (max(lower_shadow, 0.001) * PURITY_RATIO):
                self.current_tail_type = "TT"
                shadow_pct = (upper_shadow / total_range) * 100
                self.log(f"🕵️ [Tail-TT] 发现上影压力 | 占比:{shadow_pct:.1f}% | 纯度确认", level="DEBUG")
                return


    def _reset_all_ready_flags(self):
        """[07-12 信号自愈] 在每一根 2min 柱开始探测前清空旧信号"""
        # 法则信号清空
        self.ready_to_long_law1 = self.ready_to_short_law1 = False
        self.ready_to_long_law2 = self.ready_to_short_law2 = False
        self.ready_to_long_law3 = self.ready_to_short_law3 = False
        self.ready_to_long_law4 = self.ready_to_short_law4 = False
        self.ready_to_long_law5 = self.ready_to_short_law5 = False
        self.ready_to_long_law6 = self.ready_to_short_law6 = False
        self.ready_to_long_law7 = self.ready_to_short_law7 = False # 预留
        self.ready_to_long_law8 = self.ready_to_short_law8 = False
        self.ready_to_long_180  = self.ready_to_short_180  = False
        # 辅助信号清空
        self.current_tail_type = None
        self.law1_sl = 0.0
        self.law2_sl = 0.0
        self.law3_sl = 0.0
        self.law4_sl = 0.0
        self.law5_sl = 0.0
        self.law6_sl = 0.0
        self.law8_sl = 0.0
        self.v180_sl = 0.0
        
        sys_log("🔄 [信号复位]_reset_all_ready+flages 已重置所有 Law 信号旗语", level="DEBUG")


    def _detect_market_patterns(self, new_bar):
        """
        [信号层-总闸] 态势感知雷达：全量法则探测
        职责：调用所有形态探测器，更新信号旗语 (ready_to_xxx_lawX)
        """
        if new_bar is None:
            return
        if hasattr(new_bar, 'empty') and new_bar.empty:
            return
                
        self.last_confirmed_tail = self.current_tail_type
        self._reset_all_ready_flags()
        self._detect_tail_bars(new_bar)

        self._detect_law1(new_bar)        # Elephant Bar (大象柱)
        self._detect_law2(new_bar)        # Color Change (颜色改变)
        self._detect_law3(new_bar)        # 3-5 Bars (回调反转)
        self._detect_law4(new_bar)        # RBI/GBI (忽略柱/影线延续)
        self._detect_law5(new_bar)               # 20MA Cross (价格均线穿越 - 无需传参)
        self._detect_law6(new_bar)        # Home Run (本垒打)
        #self._detect_law7(new_bar)        # 200MA Reversion (预留占位 - 无需传参)
        self._detect_law8(new_bar)        # Fabulous 42 (Fab 42)
        self._detect_180_reversal(new_bar)      # 180 反转识别 (无需传参)


    def analyze_signals(self, current_price, vix):
        """
        [决策层-A] 信号分拣与宏观审计
        职责：分拣信号、MA200趋势过滤、VIX熔断
        """
        # 1. 基础环境与数据保鲜审计
        if self.suspend_today: 
            self.sys_log(f"🚫 {self.symbol} 暂停交易。不再进行信号分析，只是观察记录信号", level="ERROR")
            return None
        if self.ma200 is None: return None

        side, label, raw_sl = None, "", 0.0

        # --- 优先级分拣 (Velez 梯队逻辑) ---
        # 梯队 I: 强力爆发
        if self.ready_to_long_law1 or self.ready_to_short_law1:
            side = "LONG" if self.ready_to_long_law1 else "SHORT"
            label = "Law1-LONG" if self.ready_to_long_law1 else "Law1-SHORT"
            raw_sl = self.law1_sl  # ✨ 引用固化快照
        elif self.ready_to_long_180 or self.ready_to_short_180:
            side = "LONG" if self.ready_to_long_180 else "SHORT"
            label = "V180-LONG" if self.ready_to_long_180 else "V180-SHORT"
            raw_sl = self.v180_sl  # ✨ 引用固化快照    
        
        # 梯队 II: 趋势回踩
        elif self.ready_to_long_law6 or self.ready_to_short_law6:
            side = "LONG" if self.ready_to_long_law6 else "SHORT"
            label = "Law6-LONG" if self.ready_to_long_law6 else "Law6-SHORT"
            raw_sl = self.law6_sl  # ✨ 引用固化快照

        elif self.ready_to_long_law3 or self.ready_to_short_law3:
            side = "LONG" if self.ready_to_long_law3 else "SHORT"
            label = "Law3-LONG" if self.ready_to_long_law3 else "Law3-SHORT"
            raw_sl = self.law3_sl  # ✨ 引用固化快照

        elif self.ready_to_long_law4 or self.ready_to_short_law4:
            side = "LONG" if self.ready_to_long_law4 else "SHORT"
            label = "Law4-LONG" if self.ready_to_long_law4 else "Law4-SHORT"
            raw_sl = self.law4_sl  # ✨ 引用固化快照

        # 梯队 III: 基础确认
        elif self.ready_to_long_law8 or self.ready_to_short_law8:
            side = "LONG" if self.ready_to_long_law8 else "SHORT"
            label = "Law8-LONG" if self.ready_to_long_law8 else "Law8-SHORT"
            raw_sl = self.law8_sl  # ✨ 引用固化快照

        elif self.ready_to_long_law5 or self.ready_to_short_law5:
            side = "LONG" if self.ready_to_long_law5 else "SHORT"
            label = "Law5-LONG" if self.ready_to_long_law5 else "Law5-SHORT"
            raw_sl = self.law5_sl  # ✨ 引用固化快照
            
        elif self.ready_to_long_law2 or self.ready_to_short_law2: # 补全 Law2
            side = "LONG" if self.ready_to_long_law2 else "SHORT"
            label = "Law2-LONG" if self.ready_to_long_law2 else "Law2-SHORT"
            raw_sl = self.law2_sl  # ✨ 引用固化快照

        if not side: return None

        # 2. 宏观合规性审计
        # A. MA200 过滤 (拒绝与大趋势MA200相反的下单信号)
        if side == 'LONG' and current_price < self.ma200:
            self.log("⚠️ [MA200警告] 多头信号在200MA下方，但微观结构健康，允许执行", level="WARN")
            #self.log(f"🚫 [拒绝信号] {label} 原因: 多头信号在 MA200 下方", level="DEBUG")
            #return None
        if side == 'SHORT' and current_price > self.ma200:
            self.log("⚠️ [MA200警告] 空头信号在200MA上方，但微观结构健康，允许执行", level="WARN")
            #self.log(f"🚫 [拒绝信号] {label} 原因: 空头信号在 MA200 上方", level="DEBUG")
            #return None

        # B. VIX 熔断
        if vix > 30.0:
            self.log(f"🚫 [拒绝信号] {label} 原因: 恐慌指数VIX({vix}) 极高风险", level="DEBUG")
            return None

        if side == None: return
        if label == "": return
        if raw_sl == 0.0: return None

        # 3. 封装输出
        return {
            "side": side,
            "label": label,
            "raw_sl": raw_sl,
            "entry_price": current_price,
            "status": "GO"
        }

    
    async def plan_trade(self, packet, snapshot: ContextSnapshot):
        """
        [决策层-B] 战术精算与工单拟定 V5.1 (纯净精算版)
        职责：
        1. 物理对账：基于 snapshot 事实计算可用加仓空间。
        2. 变量统一：全面采用 label 命名体系。
        3. 职责隔离：仅输出指令包，不修改 self 内存，确保无副作用。
        """
        if not packet or packet.get("status") != "GO":
            return None

        # --- 1. 基础维度提取 (统一 Label) ---
        side = packet["side"]
        label = packet["label"]
        entry = packet["entry_price"]
        raw_sl = packet["raw_sl"]
        action = "BUY" if side == "LONG" else "SELL"
        
        # --- 2. 风险规模精算 (变量重名：is_pyramid_label) ---
        label_upper = label.upper()
        # 修正：统一使用 label 后缀进行性格判定
        is_pyramid_label = any(k in label_upper for k in ["PYRAMID", "LAW2", "LAW#2"])
        current_risk_money = self.risk_unit * 0.5 if is_pyramid_label else self.risk_unit
        
        # 物理空间核算 (基于 snapshot 绝对事实)
        current_holding_abs = snapshot.abs_pos
        gap_qty = max(0, self.max_qty - current_holding_abs)

        # --- 3. 价格空间与步长精算 ---
        atr = self.effective_atr
        min_gap = round(1.0 * atr, 2)
        tp_step = round(2.0 * atr, 2)
        
        # 参考基准：加仓看物理均价，开仓看当前现价
        reference_price = snapshot.avg_cost if (snapshot.has_position and snapshot.avg_cost > 0) else entry
        
        # 计算止损位与目标位 (局部变量，不触碰 self)
        if side == 'LONG':
            sl = round(min(raw_sl, reference_price - min_gap), 2)
            calc_tp1 = round(reference_price + tp_step, 2)
            # 加仓约束：tp1 不得低于旧有目标（如果有）
            tp1 = max(calc_tp1, getattr(self, 'tp2', 0)) if snapshot.has_position else calc_tp1
            tp2 = round(tp1 + tp_step, 2)
        else:
            sl = round(max(raw_sl, reference_price + min_gap), 2)
            calc_tp1 = round(reference_price - tp_step, 2)
            tp1 = min(calc_tp1, getattr(self, 'tp2', 0)) if (snapshot.has_position and getattr(self, 'tp2', 0) > 0) else calc_tp1
            tp2 = round(tp1 - tp_step, 2)

        # --- 4. 股数控制与资金审计 ---
        risk_dist = abs(entry - sl)
        if risk_dist <= 0: return None
        
        suggested_shares = int(current_risk_money // risk_dist)
        
        # 物理对账：开仓 vs 加仓 (50% 物理红线)
        if not snapshot.has_position:
            shares = min(suggested_shares, self.max_qty)
        else:
            # 审计：加仓量 <= 现有实盘 50% 且不超过剩余额度
            shares = min(suggested_shares, gap_qty, int(current_holding_abs * 0.5))

        if shares < 10: return None

        # 异步资金对账
        try:
            av_funds = await get_account_available_funds()
            safe_funds = av_funds * (1 - getattr(self, 'capital_buffer', 0.05))
            required_margin = (entry * shares) * getattr(self, 'margin_requirement', 0.3)
            if required_margin > safe_funds:
                self.log(f"🚨 [精算拦截] 资金不足! 需:${required_margin:.2f}", level="WARN")
                return None
        except Exception as e:
            self.log(f"⚠️ [精算异常] 无法验证保证金: {e}", level="ERROR")
            return None

        # --- 5. 战术参数生成 ---
        is_high_priority = any(k in label for k in ["Law1", "V180"])
        # 激进定价策略

        if is_high_priority:
            lmt_price = round(entry + 0.05 if side == 'LONG' else entry - 0.05, 2)  
        else:
            lmt_price = round(entry + 0.02 if side == 'LONG' else entry - 0.02, 2)
        if side == "LONG":
            plan_loss = round(lmt_price - sl,2)     #做多：预计的亏损
            plas_profit = round(tp1-lmt_price,2)    #做多：预计的盈利
        else:
            plan_loss = round(sl - lmt_price,2)     #做空：预计的亏损
            plas_profit = round(lmt_price - tp1,2)  #做空: 预计的盈利
        

        # --- 6. 📝 封装工单蓝图 (纯原子输出) ---
        instruction = {
            "side": side, 
            "action": action, 
            "shares": shares, 
            "lmt_price": lmt_price, 
            "sl": sl,
            "tp1": tp1, 
            "tp2": tp2,
            "tp1_qty": int((current_holding_abs + shares) * 0.5), # 计算拟定成交后的 TP1 止盈价格
            "priority": "Urgent" if is_high_priority else "Normal",
            "label": label,
            "trigger_price": entry # 给执行层留下的审计锚点
        }
        
        self.log(f"📝[入场指令(1)]根据信号{label}准备{action}， {shares} 股，计划价格 {lmt_price}", level="INFO")
        self.log(f"📝[入场指令(2)]止损价格设为:{sl}, 第1止盈目标价格:{tp1}, 预计盈亏比: {plan_loss}/{plas_profit}", level="INFO")
        return instruction

    
    async def run_decision_pipeline(self, current_price, vix, snapshot: ContextSnapshot):
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
        if not packet: return

        try:
            # --- 2. 虚拟持仓与并发拦截 (V5.0 核心) ---
            # A. 检查柜台是否有正在执行的“进场/加仓”挂单
            # 逻辑：只要 live_orders 里有非止损单，说明意图正在执行，拒绝产生新意图
            has_intent_in_flight = any(
                o.parentId == 0 and o.orderType not in ['STP', 'STP LMT'] 
                for o in snapshot.live_orders
            )
            if has_intent_in_flight:
                # self.sys_log(f"🛰️ [决策拦截] 柜台已有在途指令，放弃信号 {packet['label']}", level="DEBUG")
                return

            # B. 准入逻辑重塑：空仓入场 或 符合条件的加仓
            # 逻辑：利用快照判定
            can_enter_new = not snapshot.has_position and self.state == "OPEN_STAGE"
            
            # 加仓准入：已减仓(tp1_filled) 且 当前持仓 < 上限
            can_pyramid = (snapshot.has_position and 
                           getattr(self, 'tp1_filled', False) and 
                           snapshot.abs_pos < self.max_qty)

            if not (can_enter_new or can_pyramid):
                return

            # --- 3. 交易精算 (Snapshot 注入) ---
            # 注意：plan_trade 内部也需要同步适配 snapshot 参数（下一步重塑）
            instruction = await self.plan_trade(packet, snapshot)
            if not instruction:
                self.sys_log(f"⚠️ [放弃入场机会] {packet['label']} 风险收益比不佳，不下单", level="DEBUG")
                return
            
            # --- 4. 时间闸门拦截 ---
            now_et = datetime.now(EASTERN_TZ).time()
            if not (time(10, 0) <= now_et <= time(15, 30)):
                self.sys_log(f"🚫 [时间禁令] 当前时间{now_et}，程序在9:30-10:00以及15:30-15:58两个时间段禁止入场交易，仅记录信号或者被动止盈止损。", level="INFO")
                return

            # --- 5. 物理执行 (原子发射) ---
            # execute_trade 内部不再绑定复杂回调，只管发射，由节拍器闭环
            await self.execute_trade(instruction)


        except Exception as e:
            self.sys_log(f"💥 [决策中心崩溃] 指令传导中断: {e}", level="ERROR")


    async def _trailing_stop(self, current_price, snapshot: ContextSnapshot):
        """
        [策略层-追踪引擎] Velez极简版（0.5×ATR保本 + 1.5×ATR动态跟踪）
        职责：仅返回建议止损价，不操作订单
        """
        side = snapshot.direction
        cost = snapshot.avg_cost
        atr = self.effective_atr
        current_sl = None
        if snapshot.active_stop_order:
            current_sl = snapshot.active_stop_order.auxPrice  # IBKR后台真实止损价
            

        if side == 'LONG':
            # 保本位定义（固定0.1美元）
            breakeven = round(cost + 0.1, 2)
            
            # 🌟 优化：利用current_sl判断阶段（快照驱动）
            is_breakeven_achieved = (current_sl is not None) and (current_sl >= breakeven)
            # 1. 先检查 1.5ATR 追踪（高优先级）
            if current_price >= cost + atr * 1.5:
                trail_sl = max(round(current_price - atr * 1.5, 2), round(cost, 2))
                # 仅当新止损高于旧止损时才采纳
                if current_sl is None or trail_sl > current_sl:
                    self.sys_log(f"🔍 [快照] 止损单Ref: {snapshot.active_stop_order.orderRef}", level="DEBUG")
                    self.sys_log(
                    f"🧭 [1.5ATR跟踪] 价格{current_price:.2f}≥成本+1.5ATR，"
                    f"止损移至 {trail_sl:.2f} (原:{current_sl})",
                    level="INFO"
                )
                    return trail_sl
                else:
                    pass
                    #self.sys_log(f"🧭 [1.5ATR跟踪] 价格{current_price:.2f}≥成本+1.5ATR，但新止损低于旧止损，不调整", level="DEBUG")
                    
            # 2. 1.5ATR条件不满足，且未达到保本位 → 检查0.5ATR保本
            if not is_breakeven_achieved and current_price >= cost + atr * 0.5:
                if current_sl is None or breakeven > current_sl:
                    self.sys_log(f"🔍 [快照] 止损单Ref: {snapshot.active_stop_order.orderRef}", level="DEBUG")
                    self.sys_log(
                        f"🛡️ [0.5ATR保本] 价格{current_price:.2f}≥成本{cost:.2f}+0.5ATR({atr:.2f})，"
                        f"止损移至保本位 {breakeven:.2f} (原:{current_sl})",
                        level="DEBUG"
                    )
                    return breakeven
            else:
                #self.sys_log(f"🛡️ [0.5ATR保本] 价格{current_price:.2f}还没有超过保本位成本{cost:.2f}+0.5ATR({atr:.2f})，不调整", level="DEBUG")                
                return None
        # === 做空场景（对称逻辑）===
        elif side == 'SHORT':         
            breakeven = round(cost - 0.1, 2)
            is_breakeven_achieved = (current_sl is not None) and (current_sl <= breakeven)   
            
            if current_price <= cost - atr * 1.5:
                trail_sl = min(round(current_price + atr * 1.5, 2), round(cost, 2))
                if current_sl is None or trail_sl < current_sl:  # 仅下移
                    self.sys_log(f"🔍 [快照] 止损单Ref: {snapshot.active_stop_order.orderRef}", level="DEBUG")
                    self.sys_log(
                        f"🧭 [1.5ATR跟踪] 价格{current_price:.2f}≤成本-1.5ATR，"
                        f"止损移至 {trail_sl:.2f} (原:{current_sl})",
                        level="INFO"
                    )
                    return trail_sl
            if not is_breakeven_achieved and current_price <= cost - atr * 0.5:        
                if current_sl is None or current_sl > breakeven:
                    self.sys_log(f"🔍 [快照] 止损单Ref: {snapshot.active_stop_order.orderRef}", level="DEBUG")
                    self.sys_log(
                       f"🛡️ [0.5ATR保本] 价格{current_price:.2f}≤成本{cost:.2f}-0.5ATR({atr:.2f})，"
                       f"止损移至保本位 {breakeven:.2f} (原:{current_sl})",
                        level="DEBUG"
                    )
                    return breakeven
                else:
                    return  None
            
        # 无调整需求
        #if current_sl is not None:
        #    self.sys_log(f"🧭 [TS] 无需调整 (当前止损:{current_sl:.2f}|价格:{current_price:.2f})", level="DEBUG")
        #else:
        #    self.sys_log(f"🧭 [TS] 无活跃止损单 (价格:{current_price:.2f})", level="DEBUG")
        return None
        


    async def _update_stop(self, price=None, volume=None, force=False, snapshot: Optional[ContextSnapshot] = None):
        """
        [肢体层-物理阀门 V7.7] 
        集成点：1. 股数对账 2. 影子变量同步 3. 原子性锁 4. 单向防呆
        """
        if snapshot is None:
            self.sys_log("❌ [_update_stop] 关键错误：未传入快照，拒绝执行止损单修改", level="ERROR")
            return
        try:
            # --- 1. 定位物理对象 ---
            order = snapshot.active_stop_order if snapshot else None
            if not order:
                # 即使没有活跃止损单，如果实仓还在，也要转入 add_stop 补防
                if snapshot and snapshot.abs_pos > 0:
                    await self.add_stop(snapshot)
                return
            
            self.sys_log(f"🔍 [update_stop] 止损单详情: ID={order.orderId}, Action={order.action}, Qty={order.totalQuantity}, Price={order.auxPrice}, Type={order.orderType}, Side={snapshot.direction}", level="DEBUG")
            old_qty = order.totalQuantity
            old_sl = order.auxPrice
            needs_update = False
            # 纠偏(force)时使用极小阈值，追踪时使用 0.05 防抖
            STEP_THRESHOLD = 0.001 if force else 0.05 
            side = snapshot.direction

            # --- 2. 价格精算与单向防呆 ---
            if price is not None:
                new_sl = round(price, 2)
                # 严禁止损向亏损方向移动（做多只能上移，做空只能下移）
                if side == 'LONG' and new_sl >= old_sl + STEP_THRESHOLD:
                    order.auxPrice = new_sl
                    needs_update = True
                elif side == 'SHORT' and new_sl <= old_sl - STEP_THRESHOLD:
                    order.auxPrice = new_sl
                    needs_update = True

            # --- 3. 股数强制对账 ---
            if volume is not None:
                if snapshot.entry_orders:  # 有未完成的入场单（主单在途）
                    self.sys_log(
                        f"🛡️ [_update_stop] 检测到{len(snapshot.entry_orders)}笔在途入场的开仓单或者加仓单)，"
                        f"禁止对止损单股数调整，只有入场订单全部成交之后才允许修改止损单的股数",
                        level="DEBUG"
                    )
                else:  # 主单已完全成交（开仓完成/加仓完成/TP1成交后）
                    target_qty = int(volume)
                    if target_qty > 0 and order.totalQuantity != target_qty:
                        order.totalQuantity = target_qty
                        needs_update = True
            # --- 4. 冲突拦截与物理提交 ---
            if needs_update:
            # 计算在途的止盈单股数（排除止损单）
                tp_in_flight = sum(
                    o.totalQuantity for o in snapshot.closing_orders
                    if o.orderType not in ['STP', 'STP LMT']   # 排除止损单
                )

                if tp_in_flight >= snapshot.abs_pos and snapshot.abs_pos > 0:
                    self.sys_log(f"🚨 [拦截修改止损单] 发现止盈单在途 ({tp_in_flight})，暂缓修改", level="DEBUG")
                    return


                # 物理锁定
                self.is_exiting = True 
                self.ib.placeOrder(self.contract, order)
                self.sys_log(f"⚡ [止损单调整成功] {order.action} 从原来的{old_qty} 变更为{order.totalQuantity}股 @ 价格从{old_sl} 变更为 {order.auxPrice} | Force:{force}", level="INFO")
                
                self.final_stop_price = order.auxPrice  # 记忆最新物理止损价
                
                self._temp_order_audit.update({
                    'label': 'Update_Stop_Final',
                    'order_id': order.orderId,
                    'last_s_aux': order.auxPrice
                })


        except Exception as e:
            self.sys_log(f"❌ [_update_stop] 止损单更新异常，错误代码: {e}", level="ERROR")
    


    async def _submit_tp(self, qty: int, price: Optional[float] = None, snapshot: Optional[ContextSnapshot] = None, stage: str = "TP1"):
        """
        [执行层-平仓执行器 V5.0 极致精简版] 
        职责：
        1. 物理准入：基于 snapshots 预计算的在途量进行核算。
        2. 意图发射：仅负责下单，不再手动维护股数累加。
        """
        try:
            # 1. 物理余量核算 (利用缓存的最新快照事实)
            if snapshot is None:  # 仅当传入None时才回退
                snapshot = getattr(self, 'latest_snapshot', None)
            if not snapshot or not snapshot.has_position:
                return None

            # 可平仓的余量 = 快照里面的持仓数量 - 后台已经在跑的平仓单的股票数量
            in_flight_closing_qty = sum(
                                        o.totalQuantity for o in snapshot.closing_orders 
                                        if getattr(o, 'orderRef', '').startswith('TP_')
                                    )
            available_to_close = snapshot.abs_pos - in_flight_closing_qty
            self.sys_log(
                f"🔍 [TP核算] 持仓:{snapshot.abs_pos} | 止盈单:{in_flight_closing_qty} | "
                f"可平余量:{available_to_close} | 请求量:{qty}",
                level="DEBUG"
            )
            if qty <= 0 or available_to_close <= 0:
                self.sys_log(
                    f"⚠️ [TP拦截] 可平余量不足 | 持仓:{snapshot.abs_pos} | 止盈单:{in_flight_closing_qty}",
                    level="DEBUG"
                )
                return None

            # 2. 意图锁定 (逻辑闸门)
            self.is_exiting = True
            final_qty = int(min(qty, available_to_close))
            
            # 3. 确定动作与价格
            action = 'SELL' if snapshot.direction == 'LONG' else 'BUY'
            
            if price is not None:
                final_price = round(price - 0.01, 2) if action == 'SELL' else round(price + 0.01, 2)
                m_order = LimitOrder(action, final_qty, final_price)
                m_order.algoStrategy = 'Adaptive'
                m_order.algoParams = [TagValue('adaptivePriority', 'Normal')]
                m_order.orderRef = f"{stage}_{self.symbol}"   # 例如 "TP1_AMZN"
            else:
                m_order = MarketOrder(action, final_qty)
                m_order.algoStrategy = 'Adaptive'
                m_order.algoParams = [TagValue('adaptivePriority', 'Urgent')]
                m_order.orderRef = f"{stage}_{self.symbol}"   # 例如 "TP1_AMZN"

            # 4. 物理提交
            trade = self.ib.placeOrder(self.contract, m_order)
            
            # ⚠️ V5.0 核心改动：不再绑定 local 回调，靠 filled_flag 摇铃
            self.filled_flag = True 
            
            self.sys_log(f"📉 [止盈单TP] {action} {final_qty}股提交给TWS | 剩余可平余量:{available_to_close - final_qty}", level="DECISION")
            return trade

        except Exception as e:
            self.is_exiting = False 
            self.sys_log(f"❌ [_submit_tp] 严重异常: {e}", level="ERROR")
            return None


    async def _detect_take_profit(self, current_price: float, snapshot: Optional[ContextSnapshot]):
    
        """
        [指挥部-止盈决策] 
        职责：判定 TP1/TP2 触碰事实，并下达平仓指令。
        """
        # 1. 消除红线：空值防御
        if snapshot is None:
            self.sys_log("❌ [_detect_take_profit] 关键错误：未传入快照，拒绝执行止盈探测", level="ERROR")
            return
        if snapshot.tp1_active:
            self.sys_log("🛡️ [TP拦截] TP1已提交，等待成交或撤销", level="DEBUG")
            return
        if snapshot.tp2_active:
            self.sys_log("🛡️ [TP拦截] TP2已提交，等待成交或撤销", level="DEBUG")
            return    
        try:
            # === 关键修复：使用current_price替代bars_reference ===
            curr_low = current_price
            curr_high = current_price
             # 备用：若bars_reference存在，用5秒bar极值增强敏感度
            bars = getattr(self, 'bars_reference', None)
            if bars and hasattr(bars, '__len__') and len(bars) > 0:
                try:
                    curr_low = min(curr_low, bars[-1].low)
                    curr_high = max(curr_high, bars[-1].high)
                except Exception as e:
                    self.sys_log(f"⚠️ [TP] bars_reference异常: {e}", level="DEBUG")
            
            ## 判断当前阶段：TP1 还是 TP2
            # 规则：持仓 > 70% 且 无活跃TP1止盈单 → TP1；否则 → TP2
            is_tp1 = (snapshot.abs_pos > self.last_trade_qty * 0.7) and not snapshot.tp1_active
            target_price = self.tp1 if is_tp1 else self.tp2
            stage = "TP1" if is_tp1 else "TP2"
            
            # 🔥 增加审计日志（必现问题定位）
            #self.sys_log(
            #    f"🔍 [TP探测] 方向:{snapshot.direction} | "
            #    f"当前价:{current_price:.2f} | 5秒Bar:[{curr_low:.2f}, {curr_high:.2f}] | "
            #    f"目标价:{target_price:.2f}({stage}) | "
            #    f"持仓:{snapshot.abs_pos}/{self.last_trade_qty}",
            #    level="DEBUG"
            #)
            # 4. 触碰判定逻辑 (带 Buffer 防滑)
            is_hit = False
            # 价格触碰判定（保持原逻辑）...
            tp_buffer = max(self.effective_atr * 0.5, 0.05)
            if snapshot.direction == 'LONG':
                is_hit = curr_high >= target_price - tp_buffer
            else:
                is_hit = curr_low <= target_price + tp_buffer

            if not is_hit:
                return

            # 计算止盈股数
            tp_qty = int(snapshot.abs_pos * 0.5) if is_tp1 else int(snapshot.abs_pos)
            tp_qty = max(tp_qty, 1)

            self.sys_log(f"🎯 [提交止盈单] {stage} 计划止盈价格{target_price:.2f} | 计划止盈平仓{tp_qty}股 | 现有持仓{snapshot.abs_pos}")
            trade = await self._submit_tp(qty=tp_qty, price=target_price, snapshot=snapshot, stage=stage)
            if trade:
                # 注意：提交后，下一周期快照就会包含该止盈单，从而阻止重复提交
                pass

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
        if self.is_processing_order: return
        if not instruction: return
        self.is_processing_order = True
        current_active_trade = None 
        
        try:
            # --- 1. 参数解构 ---
            action = instruction["action"]
            qty = instruction["shares"]
            lmt_price = instruction["lmt_price"]
            entry_ref = instruction["trigger_price"]
            sl, tp1, tp2 = instruction["sl"], instruction["tp1"], instruction["tp2"]
            tp1_qty = instruction["tp1_qty"]
            label, priority = instruction["label"], instruction["priority"]            
            rev_action = 'SELL' if action == 'BUY' else 'BUY'
            # 计算辅助价 (StopLimit 专用：触碰即发单)
            aux_price = round(entry_ref, 2)
            timestamp = int(time_module.time())  # 秒级时间戳（避免毫秒重复风险）
            safe_label = label.replace(' ', '_').replace('-', '_')[:10]  # 清理特殊字符+截断
            
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
                active_stop = next((t for t in self.ib.openTrades() 
                                  if t.contract.symbol == self.symbol and 
                                  t.order.orderType in ['STP', 'STP LMT'] and t.isActive()), None)
            if active_stop and active_stop.order.totalQuantity == 0:
                self.sys_log(f"⚠️ [加仓异常] 检测到无效止损单(股数=0)，强制重建", level="WARN")
                active_stop = None  # 回退到开新仓逻辑
            # --- 4. 构造主订单 ---
            p_order = StopLimitOrder(action, qty, lmt_price, aux_price)
            p_order.algoStrategy = 'Adaptive'
            p_order.algoParams = [TagValue('adaptivePriority', priority)]
            # ✅ 开新仓：父 transmit False，等子单带着一起发
            # ✅ 加仓：没有新子单挂钩父单，所以父必须 transmit True
            p_order.transmit = True if active_stop else False

            # ✅ 为主订单设置orderRef（开仓/加仓区分）
            # orderRef格式---开仓主单:E_{symbol}_{label[:8]}_{timestamp},E=Entry，时间戳秒级
            # orderRef格式---加仓主单:A_{symbol}_{label[:8]}_{timestamp},A=Add,
            # orderRef格式---新开止损单:S_{symbol}_{label[:8]}_{timestamp},S=Stop
            # orderRef格式---保留原orderRef,IBKR禁止修改已提交订单的orderRef
            # orderRef格式---止盈单 TP1_{symbol} / TP2_{symbol}
            if active_stop:
                p_order.orderRef = f"A_{self.symbol}_{safe_label}_{timestamp}"[:32]  # A=Add
            else:
                p_order.orderRef = f"E_{self.symbol}_{safe_label}_{timestamp}"[:32]  # E=Entry
            self.sys_log(f"🔖 [订单标识] 主单Ref: {p_order.orderRef}", level="DEBUG")
            # 执行物理下单，立即拿到 trade 对象及其 OrderId
            current_active_trade = self.ib.placeOrder(self.contract, p_order)

            p_id = getattr(current_active_trade.order, "orderId", 0) or 0
            if p_id <= 0:
                await asyncio.sleep(0)  # 让出一个事件循环节拍，等待回填
                p_id = getattr(current_active_trade.order, "orderId", 0) or 0

            if p_id <= 0:
                p_id = getattr(p_order, "orderId", 0) or 0  # 兜底：有时回填在 p_order 上

            if p_id <= 0:
                raise RuntimeError("IB orderId not assigned (p_id<=0), abort bracket to avoid orphan stop.")

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
                original_ref = getattr(s_order, 'orderRef', 'N/A')[:32]
                self.sys_log(f"🧱 [止损保护] 原Ref:{original_ref} | 止损股数{old_sl_qty}→{new_sl_qty}", level="INFO")
                self.sys_log(f"📦[加仓单提交] ID:{p_order.orderId} | Ref:{p_order.orderRef} | {action} {qty}股 @ {lmt_price}", level="INFO")
                self.sys_log(f"🛡️[止损单同步] ID:{s_order.orderId} | 原Ref:{original_ref} | {rev_action} {new_sl_qty}股 @ {s_order.auxPrice}", level="INFO")
            
            else:
                # B. 【开新仓状态】：新建随动止损单，挂钩主单 ID
                s_order = StopOrder(rev_action, qty, round(sl, 2))
                s_order.parentId = p_id
                s_order.transmit = True 
                s_order.orderRef = f"S_{self.symbol}_{safe_label}_{timestamp}"[:32]  # S=Stop
                self.sys_log(f"🔖 [订单标识] 止损单Ref: {s_order.orderRef}", level="DEBUG")
                
                self.ib.placeOrder(self.contract, s_order)
                self.sys_log(f"🛡️ [止损保护] 止损单#{s_order.orderId}(Ref:{s_order.orderRef}) 与主订单#{p_id}建立Bracket关系", level="INFO")
                self.sys_log(f"✅ [{label}] 开仓单和止损单已提交", level="INFO")
                self.sys_log(f"📦[开仓单提交] ID:{p_order.orderId} | Ref:{p_order.orderRef} | {action} {qty}股 @ {lmt_price}", level="INFO")
                self.sys_log(f"🛡️[止损单同步] ID:{s_order.orderId} | Ref:{s_order.orderRef} | {rev_action} {qty}股 @ {s_order.auxPrice}", level="INFO")

            if p_order.transmit:
                self.sys_log(f"📡 [订单发送] 主单#{p_id} 独立发送给TWS (加仓订单)", level="DEBUG")
            else:
                self.sys_log(f"📡 [订单发送] 主单#{p_id} 与止损单同时发送给TWS (开新仓订单)", level="DEBUG")
            # --- 5. 【核心】原子化审计指纹刻录 ---
            # 这一步是 feed 给 _sync_position 的唯一真相源
            self._temp_order_audit = {
                'order_id': p_id,
                'label': label,
                'trigger_price': entry_ref,
                'last_p_lmt': p_order.lmtPrice,
                'last_s_aux': s_order.auxPrice
            }

            # --- 6. 状态跳变与计时开始 ---
            
            self.filled_flag = True # 摇铃，驱动下一秒进行物理确认           
            #self.sys_log(f"🚀 [{label}] 发射指令已送达柜台！ID:{p_id} 量:{qty} 价:{lmt_price}", level="INFO")

        except Exception as e:
            # 异常时逻辑自愈：回归待机，释放锁
            self.state = "OPEN_STAGE"
            if current_active_trade and current_active_trade.isActive():
                self.ib.cancelOrder(current_active_trade.order)
                self.sys_log("⚠️ [下单故障] 下单指令无法送达IBKR服务器，尝试撤回下单指令", level="WARN")
            self.is_processing_order = False
            self.sys_log(f"❌ [execute_trade 崩溃] 原因: {e}", level="ERROR")
            self.reset_context()

        finally:
            # 保证锁的释放
            await asyncio.sleep(0.1)
            self.is_processing_order = False
            

    async def clear_pos(self, snapshot):
        # 紧急清仓动作        
        # --- 0. 肃清残留：发送新平仓指令前，先撤销所有可能存在的离场挂单 ---
        if snapshot.live_trades:
            for t in snapshot.live_trades:
                self.ib.cancelOrder(t.order)
            await asyncio.sleep(0.1) # 短暂等待撤单指令发出
        # 从snapshot快照里面拉取最新持仓事实
        abs_pos = snapshot.abs_pos
        if abs_pos == 0: return
        qty = abs_pos
        action = 'SELL' if snapshot.direction == 'LONG' else 'BUY'
        # 获取当前最后一笔成交价作为精算基准
        tickers = await self.ib.reqTickersAsync(self.contract)
        ticker = tickers[0] if tickers else None
        trigger_p = ticker.last if (ticker and ticker.last > 0) else (ticker.close if ticker else 0)  
        self.sys_log(f"📡 [Clear_pos] 尝试强制平仓 | 实仓股数: {abs_pos} | 参考价: {trigger_p}", level="INFO")

        # 拟定工单：优先尝试激进限价单，失败则上市价单
        if ticker and (ticker.bid if action == 'SELL' else ticker.ask):
            lmt_price = round(ticker.bid - 0.05 if action == 'SELL' else ticker.ask + 0.05, 2)
            close_order = LimitOrder(action, qty, lmt_price)
        else:
            close_order = MarketOrder(action, qty)
        # ✅ 核心增强：为紧急平仓单设置唯一orderRef（与check_and_exit统一格式）
        timestamp = int(time_module.time())  # 秒级时间戳
        order_ref = f"CL_{self.symbol}_Close_{timestamp}"[:32]  # CL=Close（与收盘平仓单统一前缀）
        close_order.orderRef = order_ref  # ✅ 关键：设置orderRef

        trade = self.ib.placeOrder(self.contract, close_order)
        self.filled_flag = True 
        # 记录强平审计快照 (对齐标准结构)
        self._temp_order_audit = {
            'order_id': trade.order.orderId, 
            'label': "MKT-Close-Force",
            'trigger_price': trigger_p,
            'last_p_lmt': 0.0, 
            'last_s_aux': 0.0
        }
        # ✅ 增强日志：记录orderRef便于审计（与check_and_exit风格统一）
        self.sys_log(
            f"📦 [提交强制平仓单] ID:{trade.order.orderId} | Ref:{order_ref} | "
            f"{action} {qty}股 @ 参考价:{trigger_p}",
            level="CRITICAL"  # 紧急清仓使用CRITICAL级别（高于INFO）
        )
        # 阻塞式等待成交 (最多等5 秒)
        wait_timer = 0
        while not trade.isDone() and wait_timer < 5:
            await asyncio.sleep(1)
            wait_timer += 1
        if not trade.isDone():
            self.sys_log(
                f"⏳ [强制平仓超时] OID:{trade.order.orderId}(Ref:{order_ref}) 未完全成交",
                level="WARN"
            )
        else:
            self.sys_log(
                f"✅ [强制平仓完成] OID:{trade.order.orderId}(Ref:{order_ref}) 已成交",
                level="SUCCESS"
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
            if getattr(o, 'is_partially_filled', False) or self.order_fill_map.get(o.orderId, 0) > 0:
                self.sys_log(
                    f"🛡️ [撤单拦截] OID:{o.orderId} 已部分成交({self.order_fill_map.get(o.orderId,0)}股)，跳过撤单",
                    level="WARN"
                )
                continue
            trade = next((t for t in snapshot.live_trades if t.order.orderId == o.orderId), None)
            if trade and getattr(trade.orderStatus, 'status', '') in ('Submitted', 'PreSubmitted', 'PendingSubmit'):
                try:
                    self.ib.cancelOrder(o)
                    cancelled_orders.append(o.orderId)
                    self.order_fill_map.pop(o.orderId, None)
                    self.sys_log(f"✅ [撤单] orderId={o.orderId} | qty={o.totalQuantity} | 原因:{reason}", level="DEBUG")
                except Exception as e:
                    self.sys_log(f"⚠️ [撤单失败] orderId={o.orderId}: {str(e)[:80]}", level="ERROR")
        
        # === 核心修正：根据场景差异化重置状态 ===
        if is_adding:
            # ✅ 加仓撤单：仅放弃加仓意图，回归纯持仓状态
            self.state = "HOLDING_STAGE"  # 保持持仓意图
            self.sys_log(f"🔄 [加仓撤单] 放弃加仓意图，回归持仓状态 | 原因:{reason}", level="INFO")
        else:
            # 入场撤单：完全放弃交易意图
            self.state = "OPEN_STAGE"
            self.sys_log(f"🔄 [入场撤单] 完全放弃交易意图 | 原因:{reason}", level="INFO")
        
        self.order_place_time = 0
        self._temp_order_audit = {}
        self.is_processing_order = False
        self.filled_flag = True  # 触发下一次快照清理
        
        # 兜底日志
        remaining = len(snapshot.entry_orders) - len(cancelled_orders)
        if remaining > 0:
            self.sys_log(f"🛡️ [兜底] {remaining}笔订单可能未取消，5秒内通过cond_02清理", level="WARN")
    
    def _chase_order(self, snapshot: ContextSnapshot, audit: dict):
        """
        [治理层] 动能单追单公共函数
        职责：执行激进价格追单 + 止损单风险间隙守恒同步
        适用场景：cond_06_02 (入场追单) / cond_08_02 (开仓追单)
        
        核心原则：方向判定基于物理订单事实 (order.action)，而非字符串猜测 (label)
        """
        # === 阶段1：方向判定（基于物理订单事实）===
        if not snapshot.entry_orders:
            self.sys_log("❌ [_chase_order] 无入场订单，无法判定交易方向", level="ERROR")
            return False
        
        # ✅ 黄金标准：直接读取订单的 action 字段（BUY/SELL）
        p_order = snapshot.entry_orders[0]  # 第一个入场单即主单
        is_buy = (p_order.action == 'BUY')
        side_str = "LONG" if is_buy else "SHORT"
        
        # === 阶段2：主订单定位 ===
        p_target_id = audit.get('order_id')
        if not p_target_id:
            self.sys_log("❌ [_chase_order] 审计指纹缺失 order_id", level="ERROR")
            return False
        
        p_trade = next((t for t in snapshot.live_trades if t.order.orderId == p_target_id), None)
        if not p_trade:
            # 诊断：打印所有活跃订单供排查
            order_ids = [t.order.orderId for t in snapshot.live_trades]
            self.sys_log(
                f"⚠️ [_chase_order] 找不到主订单ID {p_target_id} | 活跃订单: {order_ids}",
                level="ERROR"
            )
            return False
        
        # === 阶段3：对手价获取 ===
        ticker = self.ib.ticker(self.contract)
        opp_price = ticker.ask if is_buy else ticker.bid
        
        # 价格兜底逻辑（防None/0）
        if not opp_price or opp_price <= 0:
            opp_price = ticker.last if (ticker and ticker.last > 0) else ticker.close
        if not opp_price or opp_price <= 0:
            self.sys_log("🚫 [_chase_order] 无法获得有效市场价格（Ask/Bid/Last均无效）", level="ERROR")
            return False
        
        # === 阶段4：价格穿透调优 ===
        orig_p_lmt = audit.get('last_p_lmt', p_trade.order.lmtPrice)
        orig_s_aux = audit.get('last_s_aux', 0.0)
        
        # 价格穿透：对手价 + 0.05 缓冲区（确保吃掉盘口厚度）
        new_p_lmt = round(opp_price + 0.05, 2) if is_buy else round(opp_price - 0.05, 2)
        new_s_aux = orig_s_aux  # 默认不更新止损价
        
        
        # === 阶段6：主订单追单 ===
        p_order = p_trade.order
        p_order.lmtPrice = new_p_lmt
        p_order.algoStrategy = 'Adaptive'
        p_order.algoParams = [TagValue('adaptivePriority', 'Urgent')]
        self.ib.placeOrder(self.contract, p_order)
        
        # === 阶段7：审计指纹更新 ===
        self._temp_order_audit.update({'last_p_lmt': new_p_lmt })
        #    'last_s_aux': new_s_aux
        
        
        # === 阶段8：成功日志 ===
        self.sys_log(
            f"⚡ [追单提交TWS] {side_str} |市场价:{opp_price:.2f} → 订单新入场价:{new_p_lmt:.2f} | ",level="WARN"
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
        self.last_cond = getattr(self, 'current_cond', "Cond_01_IDLE")
        self.current_cond = "Cond_Unknown"
        intent = self.state              # 内存意图：OPEN_STAGE / ORDER_SENT / HOLDING_STAGE
        has_pos = snapshot.has_position  # 物理存在性：True(有仓), False(空仓)
        abs_pos = snapshot.abs_pos      # 物理持仓量
        side = snapshot.direction
        # c_entry: 所有入场/加仓方向的挂单数量， 是订单的数量，不是股票数量
        c_entry = len(snapshot.entry_orders)        
        # c_closing: 离场单订单的总数 (止损 + 止盈)
        c_closing = len(snapshot.closing_orders)        
        count = c_entry + c_closing              # 柜台总活跃单据数
        has_orders = (count > 0)
        # stop_qty: 所有止损单的总股数 (STP / STP LMT)
        stop_qty = sum(abs(o.totalQuantity) for o in snapshot.closing_orders if o.orderType in ['STP', 'STP LMT'])

        # tp_qty: 显式统计平仓类单据 (LMT 止盈 或 MKT 紧急平仓)
        # 逻辑：只要是 LMT 或 MKT 的离场单，我们就认为它是在“看守”获利目标的单子
        tp_qty = sum(abs(o.totalQuantity) for o in snapshot.closing_orders if o.orderType in ['LMT', 'MKT'])
        
        
        # --- 0.1 时间与标签审计 (执法刻度) ---
        # 计算指令发出后的生存时长：time_module.time() 是当前物理时间，order_place_time 是下单瞬间的时间锚点
        
        p_time = getattr(self, 'order_place_time', 0) if intent == "ORDER_SENT" else 0
        if p_time > 0:
            elapsed = time_module.time() - p_time
        else:
            elapsed = 0  # 逻辑安全点：无下单则无耗时
        active_timing = (p_time > 0 and intent == "ORDER_SENT")
        # 溯源订单性格：从临时审计字典中提取该订单对应的信号名称 (如 "Elephant Bar" 或 "Law#3")
        audit = getattr(self, '_temp_order_audit', {})
        label = audit.get('label', 'UNKNOWN') if intent == "ORDER_SENT" else "IDLE"
        # --- 0.2 策略性格识别与同步宽限期 (执法分级) ---
        # 动能型信号特征：此类信号追求“破位即成交”，对排队容忍度极低 (10s)
        
        is_momentum =  any(k in label for k in ["Elephant", "V180", "Law1"])
        is_pullback =  any(k in label for k in ["Law#3", "Law3", "Pullback"])  
        # 宽限期：仅在下单后的 10s 内，且物理上尚未成交时锁定

                
        # --- 0.3 超时执法特征判定 (状态机跳变触发器) ---
        # 追单触发器：动能信号排队超过 10s，物理特征将滑向 Condition_11 (LMT -> MKT)
        # 撤单触发器：回调信号 25s 未成 或 任何信号 45s 未成，物理特征将滑向 Condition_12 (Cancel & Reset)
        is_grace = (elapsed < 10)  # 订单发出之后给10秒的观察期，等待成交
        over_grace = (elapsed >= 10)  #订单发出时间超过10秒
        to_momentum_chase =  is_momentum and (elapsed > 10)
        to_logic_cancel =  ((is_pullback and elapsed > 25) or (elapsed > 45))
        
        # ========================================================================================
        # --- 第 1 部分：信号探测 (定义 意图/持仓/挂单 三个维度一共12个互斥象限，以及下面的二级分类) ---
        # =========================================================================================
        cond_01 = (intent == "OPEN_STAGE" and not has_pos and not has_orders)   #无意图，无头寸，无在途订单  ---标准待机
        cond_02 = (intent == "OPEN_STAGE" and not has_pos and     has_orders)   #无意图，无头寸，有在途订单  ---可能是外部认为挂单
        cond_03 = (intent == "OPEN_STAGE" and     has_pos and not has_orders)   #无意图，有头寸，无在途订单  ---僵尸持仓
        cond_04 = (intent == "OPEN_STAGE" and     has_pos and     has_orders)   #无意图，有头寸，有在途订单  ---意图与实际错位，需要再细分情况
        
        cond_05 = (intent == "ORDER_SENT" and not has_pos and not has_orders)   #意图:已下单， 无头寸，无在途订单   --- 意图丢失

        cond_06 = (intent == "ORDER_SENT" and not has_pos and     has_orders)   #意图:已下单， 无头寸，有在途订单   --- 正常入场挂单
        cond_06_01 = (cond_06 and c_entry==1 and elapsed<= 10 and not snapshot.has_partial_fill)          # 主订单发出后的10秒等候成交时间
        cond_06_02 = (cond_06 and c_entry==1 and elapsed > 10 and elapsed <=25 and not snapshot.has_partial_fill ) # 主订单发出后的25秒等候成交时间
        cond_06_03 = (cond_06 and c_entry==1 and elapsed > 25 and elapsed <=45 and not snapshot.has_partial_fill) # 主订单发出后的 45秒等候成交时间
        cond_06_04 = (cond_06 and c_entry==1 and elapsed > 45 and not snapshot.has_partial_fill)      # 主订单提交已经超过45秒未成交，撤单
        cond_06_partial = (cond_06 and snapshot.has_partial_fill)
        cond_06_05 = (cond_06 and c_entry > 1 and not snapshot.has_partial_fill)  #后台出现 2个以上的主订单，异常情况，报错
        cond_06_06 = (cond_06 and not any([cond_06_01,cond_06_02,cond_06_03,cond_06_04,cond_06_05,cond_06_partial]))   #意想不到的状况,报警
        
        cond_07 = (intent == "ORDER_SENT" and     has_pos and not has_orders)   #意图:已下单， 有头寸，无在途订单   --- 刚成交，意图还没更改
        # cond_07 这种情况，首先把self.state 改成"HOLDING_STAGE",然后 树立起 filled_flag, 然后需要追加止损单保护头寸"

        # cond_08 意图:已下单，有头寸，有在途订单（加仓/开仓象限）
        cond_08 = (intent == "ORDER_SENT" and has_pos and has_orders)

        # --- 8-A: ⚡ 极端风险：加仓裸奔（止损单消失）---
        cond_08_naked_push = (cond_08 and c_entry > 0 and stop_qty == 0)

        # --- 8-B: 🏗️ 标准加仓：加仓单在途且防线完备 ---
        cond_08_normal_push = (cond_08 and c_entry > 0 and stop_qty > 0)

        # 【关键优化】按时间阈值分层（与 cond_06 完全对齐）
        cond_08_01 = (cond_08_normal_push and elapsed <= 10)                      # 10秒黄金撮合期
        cond_08_02 = (cond_08_normal_push and elapsed > 10 and elapsed <= 25)    # 10-25秒：动能单追单
        cond_08_03 = (cond_08_normal_push and elapsed > 25 and elapsed <= 45)    # 25-45秒：回调单撤单
        cond_08_04 = (cond_08_normal_push and elapsed > 45)                       # >45秒：强制撤单

        # --- 8-C: ⚖️ 成交纠偏：加仓单刚成交，进入股数对账期 ---
        cond_08_fill_sync = (cond_08 and c_entry == 0)  # 主单已成交，仅剩止损单

        cond_08_05 = (cond_08_fill_sync and stop_qty < abs_pos)   # 止损缺口 → 纠偏
        cond_08_06 = (cond_08_fill_sync and stop_qty > abs_pos)   # 止损过量 → 纠偏
        cond_08_07 = (cond_08_fill_sync and stop_qty == abs_pos)  # 完美对齐 → 转正 ✅
        
        # --- 8-D: 🚨 未定义状态兜底 ---
        cond_08_08 = (cond_08 and not any([
            cond_08_naked_push,
            cond_08_01, cond_08_02, cond_08_03, cond_08_04,
            cond_08_05, cond_08_06, cond_08_07
        ]))        
        # --- cond_09：意图持仓，无头寸，无订单 ---
        # 逻辑：账户已清空，但内存 state 还没来得及 reset
        cond_09 = (intent == "HOLDING_STAGE" and not has_pos and not has_orders) 
        # 这种状态下，通常直接执行 self.reset_context() 即可

        # --- cond_10：意图持仓，无头寸，有在途订单 ---
        # 逻辑：头寸可能被止损/手动平仓了，但柜台还残留着之前的保护单或开仓单
        cond_10 = (intent == "HOLDING_STAGE" and not has_pos and     has_orders)   #意图:持仓，无头寸，有在途订单  --- 已清仓，还有挂单，意图也未更改，需再细分
        cond_10_01 = (cond_10 and stop_qty > 0)     # 仓位没了，但止损单还在（最常见的残留风险）
        cond_10_02 = (cond_10 and tp_qty > 0)       # 仓位没了，但止盈单还在
        cond_10_03 = (cond_10 and c_entry > 0)      # 仓位没了，但之前的开仓挂单还没撤销
        cond_10_04 = (cond_10 and not any([cond_10_01, cond_10_02, cond_10_03])) # 异常挂单

        # --- cond_11：意图持仓，有头寸，无在途订单 (🚨 绝对裸奔区) ---
        # 逻辑：这就是我们之前讨论的“绝对孤儿”，没有任何保护，没有任何进攻
        cond_11 = (intent == "HOLDING_STAGE" and     has_pos and not has_orders)   #意图:持仓，有头寸，无在途订单  --- 有持仓，无加仓单，无止盈单，也无止损单
        # 这种情况在 manage_position 中直接触发后补一个止损单 1.0*ATR，或者离场。
        
        # --- cond_12：意图持仓，有头寸，有在途订单 (核心治理区) ---
        # 逻辑：系统正常运行的主要区域，需要精细化对账
        cond_12 = (intent == "HOLDING_STAGE" and has_pos and has_orders)
        
        # 12-A：止损单状态 (基于 stop_qty)
        cond_12_01 = (cond_12 and stop_qty == 0)             # 有持仓有单，但止损单缺失（可能是只有止盈或只有加仓）
        cond_12_02 = (cond_12 and stop_qty > 0 and stop_qty < abs_pos)  # 止损单股数不足 (缺口)
        cond_12_03 = (cond_12 and stop_qty > 0 and stop_qty > abs_pos)  # 止损单股数过多 (过量)
        
        # 12-B：止盈单状态 (基于 tp_qty)
        cond_12_04 = (cond_12 and tp_qty > 0)                # 止盈单正在护航中
        
        # 12-C：稳态判定
        cond_12_05 = (cond_12 and stop_qty == abs_pos)       # 止损完全覆盖，标准稳态
        cond_12_06 = (cond_12 and stop_qty == abs_pos and tp_qty > 0) # 止损止盈全方位覆盖
        
        # 12-D：加仓单干预 (如果在 HOLDING 阶段又触发了加仓逻辑)
        cond_12_07 = (cond_12 and c_entry > 0)               # 持仓期间有新的加仓单在排队

        
        # ======================================================================
        # --- 第 2 部分：外科手术式治理 (每种 Condition 独立代码块，全量日志输出) ---
        # ======================================================================

        # 2.A 部分  ---  OPEN_STAGE 治理 (待机与自愈) ---
        if cond_01:
            self.current_cond = "cond_01"
            self.filled_flag = False
            # 稳态待机：不采取任何物理动作
        elif cond_02:
            self.current_cond = "cond_02"
            self.sys_log(f"⚠️ [Cond_02] 发现残留挂单(Count={count})，执行强制清理...", level="WARN")
            for t in snapshot.live_trades: self.ib.cancelOrder(t.order)
            self.filled_flag = False
            # 对后台的活跃的挂单进行撤单操作，发出指令，结果要等到下一次(大约5秒之后)take_snapshot的时候，再来查看
        elif cond_03:   # --- 状态 03：僵尸持仓 (无意图，有头寸，无订单) ---
            self.current_cond = "cond_03"
            self.sys_log(f"🚨 [Cond_03] 僵尸持仓报警：发现未知头寸({abs_pos}股)，立即启动紧急平仓并归位！", level="CRITICAL")
            # 树立起 filled_flag, 但是在本函数内不做操作，交给manage_position函数去调用 clear_pos()函数清仓
            self.filled_flag = True
            self.is_exiting = True
            
        
        elif cond_04:   # --- 状态 04：失控持仓 (无意图，有头寸，有挂单) --
            self.current_cond = "cond_04"
            self.sys_log(f"🚨 [Cond_04] 系统失控报警：无意图但有仓({abs_pos}股)且有单({count})！执行清场手术", level="CRITICAL")            
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
            self.sys_log(f"♻️ [Cond_05] 意图丢失(无单无仓)，执行逻辑复位", level="WARN")
            self.reset_context()
            self.filled_flag = False

        elif cond_06: # 入场挂单象限
            if cond_06_01:
                self.current_cond = "cond_06_01"
                self.sys_log(f"⏱️ [Cond_06_01] 订单提交后还没超过10秒，耐心等待", level="WARN")
                self.filled_flag = False
                # 正常等待：is_grace 保护期内，不干扰撮合
            elif cond_06_02:
                self.current_cond = "cond_06_02"    #订单已经提交 >10秒 但是 <=25秒
                if is_momentum :  # 如果是动能单，就修改订单价格，激进入场。 不是动能单就继续等待。
                    self.sys_log(f"⚡ [Cond_06_02] 动能订单入场超时({int(elapsed)}s)，改价格追单", level="WARN")
                    self._chase_order(snapshot, audit)
                    self.filled_flag = False
                else:  #不是动能订单，就继续等候
                    self.sys_log(f"⏱️ [Cond_06_02] 非动能单，订单提交后还没超过25秒，耐心等待", level="WARN")
                    self.filled_flag = False
                
            elif cond_06_03:    # 主订单已经提交超过 25秒，但是还不到 45秒。 如果是动能单或者是pullback单，就撤单。其余类型订单继续等候
                self.current_cond = "cond_06_03"
                if is_pullback:
                    self.sys_log(f"⏱️ [Cond_06_03] pullback订单，订单提交超过25秒，撤单", level="WARN")
                    self._cancel_orders(snapshot, reason="25s超时(回调)", is_adding=False)  # ❌ 入场场景
                    self.filled_flag = False
                else:  # 其他普通订单，继续等待，从下单之后25秒到45秒的区间
                    self.sys_log(f"⏱️ [Cond_06_03] 非pullback订单，订单提交后还没超过45秒，耐心等待", level="WARN")
                    self.filled_flag = False
            elif cond_06_04:        # # 主订单已经提交超过 45秒，无论什么类型的订单一律撤单
                self.current_cond = "cond_06_04"
                elapsed_sec = int(elapsed)
                self.sys_log(
                    f"⏱️ [Cond_06_04] 订单已经提交超过{elapsed_sec}秒，信号 {label} 超时，执行撤单)",
                    level="WARN"
                    )              
                self._cancel_orders(snapshot, reason="45s超时强制撤单") 
                self.filled_flag = False
            elif cond_06_partial:
                self.current_cond = "cond_06_partial"
                self.sys_log(
                    f"⏳ [部分成交] OID:{snapshot.partially_filled_orders[0].order.orderId} "
                    f"已成交{snapshot.partially_filled_orders[0].orderStatus.filled}股/"
                    f"{snapshot.partially_filled_orders[0].order.totalQuantity}股 | "
                    f"elapsed:{int(elapsed)}s",
                    level="INFO"
                )
                # 不撤单，仅延长观察期
                if elapsed > 60:  # 延长至60秒
                    self.sys_log("⚠️ [部分成交超时] 剩余部分60秒未成交，尝试撤单剩余量", level="WARN")
                    self._cancel_orders(snapshot, reason="45s超时强制撤单") 
                    self.filled_flag = False
                else:
                    self.filled_flag = False
            elif cond_06_05: #后台出现 2个以上的主订单，异常情况，报错
                self.current_cond = "cond_06_05"
                self.sys_log(f"🚨  [Cond_06_05] 后台快照显示有({c_entry})个入场订单，请检查TWS order窗口", level="ERROR")
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
                            level="DEBUG"
                        )
                    self.filled_flag = False
                except Exception as e:
                    self.sys_log(f"⚠️ [Cond_06_05-Debug] 打印订单信息失败: {e}", level="ERROR")
            elif cond_06_06:
                self.current_cond = "cond_06_06"
                self.sys_log(f"⚠️ [Cond_06_06] 探测到未定义的入场挂单状态组合,NOT 06_01/02/03/04/06", level="ERROR")
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
                            level="DEBUG"
                        )
                    self.filled_flag = False
                except Exception as e:
                    self.sys_log(f"⚠️ [Cond_06_06-Debug] 打印订单信息失败: {e}", level="ERROR")
        elif cond_07:
            self.current_cond = "cond_07"
            self.sys_log(f"🏗️ [Cond_07] 入场单成交瞬间！开始确权与身份转正", level="INFO")
            # 核心转正动作
            self.state = "HOLDING_STAGE"
            self.actual_filled_qty = abs_pos
            self.avg_fill_price = snapshot.avg_cost
            self.position_side = snapshot.direction
            self.last_trade_qty = abs_pos
            self.order_place_time = 0
            self.filled_flag = True # 🚨 摇铃：让 manage_position 立即补上止损单

        elif cond_08: # 开仓象限治理
            # === 8-A: 裸奔加仓，没有止损单保护===
            if cond_08_naked_push:
                self.current_cond = "cond_08_naked"
                self.sys_log(f"🚨 [Cond_08] 加仓裸奔风险！立刻摇铃补防", level="CRITICAL")
                self.filled_flag = True 
            
            # === 8-B: 标准加仓单在途处理（按时间分层）===
            elif cond_08_01:
                self.current_cond = "cond_08_01"
                self.filled_flag = False
                # 正常等待：10秒黄金撮合期内不干扰
            
            elif cond_08_02:
                self.current_cond = "cond_08_02"
                self.filled_flag = False
                if is_momentum:  # 仅动能单追单
                    self.sys_log(f"⚡ [Cond_08_02] 动能加仓超时({int(elapsed)}s)，执行追单", level="WARN")
                    self._chase_order(snapshot, audit)
                else:
                    self.sys_log(f"⏱️ [Cond_08_02] 非动能加仓，10-25秒内耐心等待", level="WARN")
            
            elif cond_08_03:
                self.current_cond = "cond_08_03"
                if is_pullback:  # 仅回调单撤单
                    self.sys_log(f"⏱️ [Cond_08_03] 回调加仓超时25秒，执行撤单", level="WARN")
                    self._cancel_orders(snapshot, reason="25s超时(加仓回调)", is_adding=True)  # ✅ 加仓场景
                    self.filled_flag = False
                else:
                    self.sys_log(f"⏱️ [Cond_08_03] 非回调加仓，25-45秒内继续等待", level="WARN")
                    self.filled_flag = False
            
            elif cond_08_04:
                self.current_cond = "cond_08_04"
                elapsed_sec = int(elapsed)
                if snapshot.has_partial_fill:
                    self.sys_log(
                        f"⏳ [加仓单部分成交] 已成交{snapshot.partially_filled_orders[0].orderStatus.filled}股，"
                        f"剩余{snapshot.partially_filled_orders[0].orderStatus.remaining}股继续等待",
                        level="INFO"
                    )
                    self.filled_flag = False
                else:
                    self.sys_log(f"⏱️ [Cond_08_04] 开仓或加仓单超时{elapsed_sec}秒，强制撤单", level="WARN")
                    self._cancel_orders(snapshot, reason="45s超时(加仓强制)")
                    self.filled_flag = False

            # === 8-C: 成交纠偏处理（股数对账）===
            elif cond_08_05:
                self.current_cond = "cond_08_05"
                self.sys_log(f"⚖️ [Cond_08_05] 开仓或加仓成交，止损不足({stop_qty} < {abs_pos})，补齐止损", level="INFO")
                self.filled_flag = True  # 摇铃触发纠偏
            
            elif cond_08_06:
                self.current_cond = "cond_08_06"
                self.sys_log(f"⚖️ [Cond_08_06] 开仓或加仓成交，止损过量({stop_qty} > {abs_pos})，削减止损", level="INFO")
                self.filled_flag = True  # 摇铃触发纠偏
            
            elif cond_08_07:
                self.current_cond = "cond_08_07"
                self.state = "HOLDING_STAGE"
                self.actual_filled_qty = abs_pos
                self.avg_fill_price = snapshot.avg_cost
                self.position_side = snapshot.direction
                self.last_trade_qty = abs_pos
                self.order_place_time = 0
                self.filled_flag = False  # 摇铃复位
                # === 加仓后止盈处理 ===
                if self.tp1_filled:  # 之前 TP1 已成交，现在是加仓
                    # 1. 撤销所有现存止盈单（通过遍历 snapshot.closing_orders）
                    for o in snapshot.closing_orders:
                        if getattr(o, 'orderRef', '').startswith(('TP1_', 'TP2_')):
                            self.ib.cancelOrder(o)
                            self.sys_log(f"🧹 [加仓] 撤销旧止盈单 {o.orderId}")
                    # 2. 重新计算止盈价位（沿用原步长）
                    original_step = abs(self.tp2 - self.tp1)  # 保存原步长
                    self.tp1 = self.tp2  # 原TP2 → 新TP1
                    self.tp2 = round(self.tp1 + (original_step if snapshot.direction == 'LONG' else -original_step), 2)
                    self.sys_log(
                        f"⚖️ [Cond_08_07] 加仓订单成交，止损单股数与持仓完美对齐({stop_qty}={abs_pos})，转正至HOLDING_STAGE",
                        level="INFO"
                    )
                    self.sys_log(f"🔄 [Cond_08_07]加仓后对止盈价格重置,原TP2({self.tp1:.2f})→新TP1 | 新TP2={self.tp2:.2f} (步长={original_step:.2f})", level="DEBUG")
                    # 3. 重置 tp1_filled（新头寸的 TP1 尚未成交）
                    self.tp1_filled = False
                else:     # 之前的TP1没有成交，应该是新开仓
                    self.tp1_filled = False  # 确保状态正确（本来应是False）
                    self.sys_log(
                        f"⚖️ [Cond_08_07] 开仓订单成交，止损单股数与持仓完美对齐({stop_qty}={abs_pos})，转正至HOLDING_STAGE",
                        level="INFO"
                    )
                    self.sys_log(f"🔄 [Cond_08_07] 开仓订单初始止盈价位 TP1:{self.tp1:.2f} TP2:{self.tp2:.2f} (无需重置)", level="DEBUG")
            # === 8-D: 未定义状态兜底 ===
            elif cond_08_08:
                self.current_cond = "cond_08_08"
                self.sys_log(f"⚠️ [Cond_08_08] 未定义加仓状态组合，执行强制撤单", level="ERROR")
                self._cancel_orders(snapshot, reason="异常状态强制撤单")
                self.filled_flag = False
                try:
                    for t in snapshot.live_trades:
                        o = t.order
                        self.sys_log(
                            f"🔎 [Cond_08_08-Debug] orderId={o.orderId}, action={o.action}, "
                            f"type={o.orderType}, parentId={o.parentId}, qty={o.totalQuantity}",
                            level="DEBUG"
                        )
                except Exception as e:
                    self.sys_log(f"⚠️ [Cond_08_08-Debug] 打印订单失败: {e}", level="ERROR")
        # --- 2.C 部分：HOLDING_STAGE 治理 (守护与对账) ---
        elif cond_09:
            self.current_cond = "cond_09"
            self.sys_log(f"🏁 [Cond_09] 持仓已结清，执行内存归位", level="INFO")
            self.state="OPEN_STAGE"
            self.reset_context()
            self.filled_flag = False 

        elif cond_10:
            detail ="cond_10"
            if cond_10_01: 
                self.current_cond = "cond_10_01"
                detail = "cond_10_01 止损单残留"
            elif cond_10_02: 
                self.current_cond = "cond_10_02"
                detail = "cond_10_02 止盈单残留"
            elif cond_10_03: 
                self.current_cond = "cond_10_03"
                detail = "cond_10_03 开仓单残留"
            elif cond_10_04: 
                self.current_cond = "cond_10_04"
                detail = "cond_10_04 未知异常单据"
            self.sys_log(f"🧹 [Cond_10] 发现 {detail}，执行全量物理清场", level="WARN")
            for t in snapshot.live_trades: self.ib.cancelOrder(t.order)
            self.filled_flag = False

        elif cond_11:
            self.current_cond = "cond_11"
            self.sys_log(f"🚨 [Cond_11] 绝对孤儿单探测！无任何防护单，立即补救", level="CRITICAL")
            # 状态同步
            self.actual_filled_qty = abs_pos
            self.avg_fill_price = snapshot.avg_cost
            # 摇铃：由接下来的逻辑根据 ATR 补齐 final_stop_price 并下单
            self.filled_flag = True 

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
                self.sys_log(f"🏗️ [Cond_12_07] 进攻：持仓中且加仓单正在排队", level="DEBUG")
                self.filled_flag = False 

            # [12_01] 有单但无止损：属于严重防御缺失（可能只有止盈或加仓挂单）
            elif cond_12_01:
                self.current_cond = "cond_12_01"
                self.sys_log(f"🚨 [Cond_12_01] 持仓中防御单缺失！(只有止盈或加仓单)，立即摇铃补防", level="CRITICAL")
                self.filled_flag = True

            # [12_02] 止损不足：股数缺口
            elif cond_12_02:
                self.current_cond = "cond_12_02"
                self.sys_log(f"⚖️ [Cond_12_02] 止损股数不足：持仓 {abs_pos} vs 止损 {stop_qty}，准备纠偏", level="WARN")
                self.filled_flag = True

            # [12_03] 止损过量：冗余风险
            elif cond_12_03:
                self.current_cond = "cond_12_03"
                self.sys_log(f"⚖️ [Cond_12_03] 止损股数过量：持仓 {abs_pos} vs 止损 {stop_qty}，准备削减", level="WARN")
                self.filled_flag = True

            # [12_06] 特等稳态：止损对齐 + 止盈护航
            elif cond_12_06:
                self.current_cond = "cond_12_06"
                # 稳态不摇铃，仅做减仓事实探测
                if abs_pos <= (self.last_trade_qty * 0.7) and not self.tp1_filled:
                    self.tp1_filled = True
                    self.sys_log(f"🔑 [Cond_12_06] 稳态：止损+止盈全方位护航中", level="INFO")
                self.filled_flag = False 
            
            # [12_05] 标准稳态：止损完全对齐
            elif cond_12_05:
                self.current_cond = "cond_12_05"
                # 稳态不摇铃，仅做减仓事实探测
                if abs_pos <= (self.last_trade_qty * 0.7) and not self.tp1_filled:
                    self.tp1_filled = True
                    self.sys_log(f"🔑 [Cond_12_05] 稳态：止损单 1:1 覆盖中", level="INFO")
                self.filled_flag = False 
            
            # [12_04] 仅止盈监控（作为 05/06 的补充审计）
            elif cond_12_04:
                self.current_cond = "cond_12_04"
                self.sys_log(f"🛡️ [Cond_12_04] 止盈单在位巡航", level="DEBUG")
                self.filled_flag = False 
            # 异常边界哨兵
            else:
                self.current_cond = "cond_12_unhandled"
                self.sys_log(f"❓ [Cond_12] 探测到未定义子状态组合，维持现状", level="ERROR")
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
        self._save_audit_log(self.current_cond)
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
            if snapshot.direction == 'LONG':
                target_price = snapshot.avg_cost - atr_buffer
            else:
                target_price = snapshot.avg_cost + atr_buffer
            
            # 格式化价格
            target_price = round(target_price, 2)

            # --- 3. 构造与提交订单 ---
            action = 'SELL' if snapshot.direction == 'LONG' else 'BUY'
            
            # ✅ 核心增强：为紧急止损单设置唯一orderRef
            timestamp = int(time_module.time())  # 秒级时间戳
            # 格式：ES_{symbol}_AddStop_{timestamp} (ES=Emergency Stop)
            order_ref = f"ES_{self.symbol}_AddStop_{timestamp}"[:32]  # 严格≤32字符
            # 独立止损单，不绑定 parentId
            new_stop = StopOrder(action, qty, target_price)
            new_stop.orderRef = order_ref  # ✅ 关键：设置orderRef
            # --- 4. 物理执行与意图转正 ---
            # 物理加锁：防止在订单确认前产生重复指令
            self.is_exiting = True 
            self.ib.placeOrder(self.contract, new_stop)
            self.final_stop_price = target_price
            
            self._temp_order_audit.update({
                'label': 'Emergency_AddStop',
                'last_p_lmt': 0, # 止损单无主单限价
                'last_s_aux': target_price
            })
            # ✅ 增强日志：记录orderRef便于审计
            self.sys_log(
                f"🛡️ [补防执行] 提交独立止损单 | Ref:{order_ref} | "
                f"{action} {qty}股 @ {target_price} | 意图转正为 ORDER_SENT",
                level="WARN"
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
        
        if not self.ib.isConnected() :
            return
        if self.is_exiting and snapshot.abs_pos == 0:
            return


        # 统一获取当前价格（用于阶段 B 探测）        
        if hasattr(self, 'bars_reference') and self.bars_reference and len(self.bars_reference) > 0:
            current_price = self.bars_reference[-1].close
        else:
            ticker = self.ib.ticker(self.contract)
            current_price = ticker.last if (ticker and ticker.last > 0) else ticker.close
        
        price_valid = (current_price is not None and current_price > 0)
        # ======================================================================
        # --- 大厅 A：【防御治理手术室】 (物理对账与生存保障) ---
        # 核心逻辑：凡是涉及“改变物理单据数量”的动作，执行后立即 return 等待下一拍对账。
        # ======================================================================
        # 1. 紧急清场 (最高优先级)
        # [判定] 针对 cond_11 (绝对裸奔) 的空间二次审计：若无保护且已亏损过大，视为致命伤
        is_broken_orphan = False
    
        if price_valid and self.current_cond == "cond_11":
            risk_threshold = 1.0 * self.effective_atr
            is_long_broken = (snapshot.direction == 'LONG' and current_price <= (self.avg_fill_price - risk_threshold))
            is_short_broken = (snapshot.direction == 'SHORT' and current_price >= (self.avg_fill_price + risk_threshold))
            if is_long_broken or is_short_broken:
                is_broken_orphan = True

        # 触发清场的 Cond 分布：
        # cond_03: [僵尸持仓] 无意图、有头寸、无订单
        # cond_04: [系统失控] 无意图、有头寸、有残留挂单
        # Broken_Orphan: [裸奔破位] 有头寸、无止损、现价已穿透 1.0*ATR
        if self.current_cond in ["cond_03", "cond_04"] or is_broken_orphan:
            reason = "Broken_Orphan" if is_broken_orphan else self.current_cond
            self.sys_log(f"🔥 [手术A-紧急平仓] 触发原因: {reason}，立即市价清场", level="CRITICAL")
            await self.clear_pos(snapshot)
            self.filled_flag = False
            return  #清仓结束之后，立刻从当前函数返回，不再操心下面的其他事宜

        # 2. 补建防线 (第二优先级)
        # 触发补单的 Cond 分布：
        # cond_07: [成交瞬间] 入场单刚 fill，state 未转正前发现的保护空白
        # cond_11: [绝对裸奔] 正常持仓期间，所有止损/止盈单据离奇消失
        # cond_12_01: [防御缺失] 柜台有止盈或加仓单，但唯独缺失止损单
        if (self.current_cond in ["cond_07", "cond_11", "cond_12_01"]) and snapshot.abs_pos > 0:
            self.sys_log(f"🛡️ [手术B-补建止损单] 诊断标签: {self.current_cond}", level="WARN")
            await self.add_stop(snapshot)
            self.filled_flag = False 
            return # 提交完补建的止损单之后，也从本函数返回，不再操心下面的其他事宜

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
        need_sync_vol = any(c in self.current_cond for c in ["cond_08_naked", "cond_08_05", "cond_08_06", "cond_12_02", "cond_12_03"])
        
        if (self.filled_flag or need_sync_vol) and snapshot.abs_pos > 0:
            self.sys_log(f"⚖️ [手术C-纠偏止损单里的股数] 诊断标签: {self.current_cond}，同步股数至 {snapshot.abs_pos}", level="INFO")
            # 纠偏手术：价格暂按内存 final_stop_price，重点是把 volume 修正到 abs_pos
            await self._update_stop(price=self.final_stop_price, volume=snapshot.abs_pos, snapshot=snapshot)
            self.filled_flag = False
            return

        # ======================================================================
        # --- 大厅 C：【收益治理巡航厅】 (结算、追踪与止盈) ---
        # 只有在物理对账“稳态”或“终局”时才进入。
        # ======================================================================       
        
        if price_valid and snapshot.abs_pos > 0: # 只要有持仓事实，就无条件开启止盈扫描
            # (1) 追踪止损：微调价格 (非物理股数变动，不 return，允许继续探测止盈)
            suggested_sl = await self._trailing_stop(current_price, snapshot)
            if suggested_sl is not None and suggested_sl > 0:
                # 追踪属于价格维护，不设 force，内部有 0.05 步长保护
                self.sys_log(f"准备调用_update_Stop函数调整止损价格，suggested_sl=={suggested_sl}",level="DEBUG")
                await self._update_stop(price=suggested_sl, volume=snapshot.abs_pos, force=False, snapshot=snapshot)
            
            # (2) 止盈探测：触碰判定
            await self._detect_take_profit(current_price, snapshot)
        self.filled_flag = False
        return self.current_cond

    def take_snapshot(self):
        try:
            # --- 1. 获取持仓镜像 ---
            positions = self.ib.positions()
            tws_p = next((p for p in positions if p.contract.symbol == self.symbol), None)
            fact_pos = tws_p.position if tws_p else 0.0   
            fact_avg_cost = tws_p.avgCost if tws_p else 0.0

            # --- 2. 获取活跃 Trade 镜像 ---
            # 拿到的是 Trade 对象，它包裹着 Order
            live_trades = [
                t for t in self.ib.openTrades() 
                if t.contract.symbol == self.symbol and t.isActive()
            ]

            # --- 3. 封装并返回 ---
            # snapshot 内部会自动生成 live_orders, entry_orders 和 closing_orders
            snapshot = ContextSnapshot(
                fact_pos=fact_pos,
                avg_cost=fact_avg_cost,
                live_trades=live_trades
            )
            
            return snapshot
        except Exception as e:
            self.sys_log(f"❌ [take_snapshot] IBKR订单和持仓拍照失败。报错代码: {e}", level="ERROR")
            return None

    def reset_context(self):
        """
        [系统级复位 - V5.0 物理对账版] 
        职责：彻底清理物理挂单，并回归 OPEN_STAGE。不再依赖内存影子变量。
        """
        try:
            self.sys_log(f"♻️ [全量复位] 启动。正在清理物理残存并重置状态机...", level="INFO")
            
            # 1. 物理单据全量清理 (SSOT：直接对柜台开刀)
            open_trades = self.ib.openTrades()
            for t in open_trades:
                if t.contract.symbol == self.symbol and t.isActive():
                    self.sys_log(f"🧹 [清理] 撤销柜台残留单(ID:{t.order.orderId} 类型:{t.order.orderType})", level="WARN")
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
            self.initial_stop_price = 0.0
            self.final_stop_price = 0.0
            self.avg_fill_price = 0.0
            self.actual_filled_qty = 0
            self.last_trade_qty = 0
            self._loss_recorded_orders.clear()
            
            # 5. 清理审计属性 (回归初始模板)
            self._temp_order_audit = {
                'order_id': 0, 
                'label': 'IDLE', 
                'trigger_price': 0.0,
                'last_p_lmt': 0.0, 
                'last_s_aux': 0.0
            }
            
            self.latest_snapshot = None
            self.order_place_time = 0  # ✨ 补充：时间锚点必须归零
            self.order_fill_map.clear()
            
            # 6. 法则旗语清理
            self._reset_all_ready_flags()

            self.sys_log("✅ [交易完成] 状态机回归 OPEN_STAGE,self.is_exiting = False,self.is_processing_order = False。", level="INFO")

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
        self.sys_log(f"🚨 [Market Close] 收到收盘指令，开始物理清场 {self.symbol}...", level="Schedule")
        
        # 1. 物理撤单：强制排空柜台所有挂单 (SSOT原则)
        try:
            open_trades = await self.ib.reqOpenOrdersAsync()
            my_active_trades = [t for t in open_trades if t.contract.symbol == self.symbol and t.isActive()]
            
            if my_active_trades:
                self.sys_log(f"🧹 [物理清理] 发现 {len(my_active_trades)} 笔在途挂单，正在强制撤销...", level="INFO")
                for t in my_active_trades:
                    self.ib.cancelOrder(t.order)
                await asyncio.sleep(1) # 给柜台物理撤单留出通讯时间
            else:
                self.sys_log(f"✅ [物理清理] 柜台无活跃挂单。", level="DEBUG")
        except Exception as e:
            self.sys_log(f"⚠️ 撤单过程异常: {e}", level="ERROR")

        # 2. 物理平仓：基于 ib.positions() 事实执行 3 次强平尝试
        for attempt in range(3):
            # 实时拉取最新持仓事实
            positions = await self.ib.reqPositionsAsync()
            pos = next((p for p in positions if p.contract.symbol == self.symbol), None)
            
            if not pos or pos.position == 0:
                self.sys_log(f"🎉 [清场对账] 确认 {self.symbol} 账户已空。", level="INFO")
                break
            
            # 确定物理动作 (基于正负号，不看 position_side)
            action = 'SELL' if pos.position > 0 else 'BUY'
            qty = abs(pos.position)
            
            # 获取盘口价作为精算基准
            tickers = await self.ib.reqTickersAsync(self.contract)
            ticker = tickers[0] if tickers else None
            trigger_p = ticker.last if (ticker and ticker.last > 0) else (ticker.close if ticker else 0)
            # ✅ 核心增强：生成收盘平仓单orderRef（32字符内）
            timestamp = int(time_module.time())  # 秒级时间戳
            order_ref = f"CL_{self.symbol}_Close_{timestamp}"[:32]  # CL=Close
            self.sys_log(f"📡 [强平执行] 尝试第 {attempt+1} 次 | Ref:{order_ref} | 实仓:{pos.position} | 参考价:{trigger_p}", level="INFO")
            # 拟定工单：优先尝试激进限价单，失败则上市价单
            if ticker and (ticker.bid if action == 'SELL' else ticker.ask):
                lmt_price = round(ticker.bid - 0.05 if action == 'SELL' else ticker.ask + 0.05, 2)
                close_order = LimitOrder(action, qty, lmt_price)
            else:
                close_order = MarketOrder(action, qty)
            
            # 物理发射 (不再绑定回调，靠 filled_flag 触发下一节拍)
            trade = self.ib.placeOrder(self.contract, close_order)
            self.filled_flag = True 
            # 记录强平审计快照 (对齐标准结构)
            self._temp_order_audit = {
                'order_id': trade.order.orderId, 
                'label': "MKT-Close-Force",
                'trigger_price': trigger_p,
                'last_p_lmt': 0.0, 
                'last_s_aux': 0.0
            }
            self.sys_log(f"📦 [强平单提交] ID:{trade.order.orderId} | Ref:{order_ref} | {action} {qty}股", level="INFO")
            # 阻塞式等待成交 (最多等 10 秒)
            wait_timer = 0
            while not trade.isDone() and wait_timer < 5:
                await asyncio.sleep(2)
                wait_timer += 1
            
            if not trade.isDone():
                self.sys_log(f"⏳ [强平超时] OID:{trade.order.orderId} 未完全成交，准备重试...", level="WARN")

        # 3. 终点审计
        self.save_trade_logs()
        self.state = "OPEN_STAGE"
        self.sys_log(f"🏁 [财务闭环] {self.symbol} 状态机锁定为 CLOSED。", level="Schedule")


    async def _check_pending_fill(self, exec_id):
        await asyncio.sleep(self._pending_timeout)
        if exec_id in self._pending_fills:
            fill_info, _ = self._pending_fills.pop(exec_id)

            # 根据 order_ref 判断是否为平仓单
            order_ref = fill_info.get('order_ref', '')
            is_closing = (order_ref.startswith('S_') or 
                        order_ref.startswith('TP') or 
                        order_ref.startswith('CL'))

            if is_closing:
                # 平仓：使用快照数据计算盈亏（佣金未知，设为 0）
                if fill_info['position_side'] == 'LONG' and fill_info['side'] == 'SLD':
                    pnl = (fill_info['price'] - fill_info['avg_cost']) * fill_info['qty']
                elif fill_info['position_side'] == 'SHORT' and fill_info['side'] == 'BOT':
                    pnl = (fill_info['avg_cost'] - fill_info['price']) * fill_info['qty']
                else:
                    pnl = 0.0
            else:
                # 开仓：盈亏为 0
                pnl = 0.0

            self.log(f"⚠️ 佣金报告超时 (execId={exec_id})，使用程序计算盈亏", level="WARN")
            self.log_trade(
                time_str=fill_info['time'].astimezone(EASTERN_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                action=fill_info['side'],
                qty=fill_info['qty'],
                price=fill_info['price'],
                pnl=pnl,
                commission=0.0,
                exec_id=exec_id,
                label=fill_info['label'],
                order_id=fill_info['order_id'],
                order_ref=fill_info['order_ref']
            )


    def on_commission_report(self, commission_report):
        """IB API 推送佣金报告时的回调"""
        exec_id = commission_report.execId
        if exec_id in self._pending_fills:
            fill_info, _ = self._pending_fills.pop(exec_id)
            self._process_fill(exec_id, fill_info, commission_report)

    def _process_fill(self, exec_id, fill_info, report):
        """使用佣金报告处理成交记录"""
        pnl = report.realizedPNL if report.realizedPNL < 1e300 else 0.0
        commission = report.commission

        self.log_trade(
            time_str=fill_info['time'].astimezone(EASTERN_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            action=fill_info['side'],
            qty=fill_info['qty'],
            price=fill_info['price'],
            pnl=pnl,
            commission=commission,
            exec_id=exec_id,
            label=fill_info['label'],
            order_id=fill_info['order_id'],
            order_ref=fill_info['order_ref'] 
        )



