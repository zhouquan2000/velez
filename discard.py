from __future__ import annotations

"""
discard.py —— 僵尸代码隔离区

以下函数/方法经扫描确认不再被任何代码调用(死代码)，从原文件移出暂存于此，
以便运行一周验证确实无影响后再删除本文件。

每个函数块标注了「来源文件 + 原行号 + 说明」。若需恢复，按标注放回原文件
对应的 TradingContext 类里即可。

移出时间：2026-10-16
"""

# ============================================================
# estimate_ibkr_commission
#   来源: velez_bot/trading_context.py  (原行号 518-538)
#   说明: 估算 IBKR 阶梯佣金与监管费。佣金现改用 IBKR 实际回报(IBKR_Comm_Only/Realized)，本函数无调用。
# ============================================================
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


# ============================================================
# _chase_order_old
#   来源: backtest/trading_context.py  (原行号 4887-5012)
#   说明: 动能单追单的旧版本，已被无后缀的 _chase_order 取代，无调用。
# ============================================================
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


# ============================================================
# estimate_ibkr_commission
#   来源: backtest/trading_context.py  (原行号 423-443)
#   说明: 与 velez_bot/trading_context.py 中同名函数完全相同的拷贝，同样无调用。
# ============================================================
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

