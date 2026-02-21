from .shared import *

class ContextSnapshot:
    def __init__(self, fact_pos: float, avg_cost: float, live_trades: list):
        """
        [审计层-快照对象 V5.1]
        职责：
        1. 接收物理 Trade 列表，自动导出 Order 列表。
        2. 保留所有 07-29 版本的衍生属性（Qty 统计、止损单识别）。
        3. 为 Cond_11 追单提供 parentId 溯源支持。
        """
        # --- 1. 基础物理事实 ---
        self.fact_pos = fact_pos          # TWS 原始持仓 (Float)
        self.avg_cost = avg_cost          # TWS 账面成本
        self.abs_pos = abs(fact_pos)      # 绝对持仓量 (用于数学计算)
        self.has_position = self.abs_pos > 0
        
        # 识别方向
        if fact_pos > 0:
            self.direction = "LONG"
        elif fact_pos < 0:
            self.direction = "SHORT"
        else:
            self.direction = "NONE"

        # --- 2. 物理对象解构 ---
        self.live_trades = live_trades    # 保留完整 Trade 对象 (用于 ID 溯源)
        self.live_orders = [t.order for t in live_trades] # 衍生 Order 列表 (向下兼容)
        self.partially_filled_orders = [
            t for t in live_trades 
            if t.orderStatus.filled > 0 and t.orderStatus.remaining > 0
        ]
        self.has_partial_fill = len(self.partially_filled_orders) > 0
        # --- 3. [核心增强] 精准拆分“入场意图”与“出场意图” ---
        # 在 ContextSnapshot.__init__ 中替换订单分类逻辑
        if self.direction == "NONE":
            # ✅ 修正：空仓时严格按 parentId 区分
            # 主单：parentId 为 0 或 None（IBKR 有时返回 None）
            self.entry_orders = [
                o for o in self.live_orders 
                if getattr(o, 'parentId', 0) in (0, None)
            ]
            # 子单：parentId ≠ 0（止损/止盈单）
            self.closing_orders = [
                o for o in self.live_orders 
                if getattr(o, 'parentId', 0) not in (0, None)
            ]
        else:
            # 有仓时：parentId + action 双重判定
            self.entry_orders = [
                o for o in self.live_orders 
                if (getattr(o, 'parentId', 0) in (0, None) and
                    ((self.direction == "LONG" and o.action == "BUY") or
                    (self.direction == "SHORT" and o.action == "SELL")))
            ]
            self.closing_orders = [
                o for o in self.live_orders 
                if (getattr(o, 'parentId', 0) not in (0, None) or  # 止损单（parentId≠0）
                    ((self.direction == "LONG" and o.action == "SELL") or  # 止盈单（方向相反）
                    (self.direction == "SHORT" and o.action == "BUY")))
            ]
        # --- 4. 衍生数值计算 (保留原有逻辑) ---
        self.in_flight_closing_qty = sum(o.totalQuantity for o in self.closing_orders)
        self.in_flight_entry_qty = sum(o.totalQuantity for o in self.entry_orders)

        # 识别当前有效的活跃止损单
        self.active_stop_order = next(
            (o for o in self.live_orders if o.orderType in ['STP', 'STP LMT']), 
            None
        )
        # 识别止盈单
        self.tp1_active = any(
            getattr(o, 'orderRef', '').startswith('TP1_') for o in self.closing_orders
        )
        self.tp2_active = any(
            getattr(o, 'orderRef', '').startswith('TP2_') for o in self.closing_orders
        )


    def __repr__(self):
        return f"<Snapshot Pos:{self.fact_pos} Cost:{self.avg_cost} Trades:{len(self.live_trades)}>"
# ======================================================================
# --- 📦 类定义：TradingContext ---
# ======================================================================

