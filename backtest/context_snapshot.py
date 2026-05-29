# ======================================================================
# context_snapshot.py - 精确导入清单 (修正版)
# ======================================================================

from typing import Optional, List, Dict, Any  # 类型提示
from .shared import (
    sys_log,
    ENTRY_TYPE_ABBREV,
    ENTRY_TYPE_FULL,
    ACTION_MAP,
)


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
        self.fact_pos = fact_pos  # TWS 原始持仓 (Float)
        self.avg_cost = avg_cost  # TWS 账面成本
        self.abs_pos = abs(fact_pos)  # 绝对持仓量 (用于数学计算)
        self.has_position = self.abs_pos > 0

        # 识别方向
        if fact_pos > 0:
            self.direction = "LONG"
        elif fact_pos < 0:
            self.direction = "SHORT"
        else:
            self.direction = "NONE"

        # --- 2. 物理对象解构 ---
        self.live_trades = live_trades  # 保留完整 Trade 对象 (用于 ID 溯源)
        self.live_orders = [t.order for t in live_trades]  # 衍生 Order 列表 (向下兼容)
        self.partially_filled_orders = [
            t
            for t in live_trades
            if t.orderStatus.filled > 0 and t.orderStatus.remaining > 0
        ]
        self.has_partial_fill = len(self.partially_filled_orders) > 0

        # --- 3. [核心增强] 精准拆分“入场意图”与“出场意图” ---
        # 在 ContextSnapshot.__init__ 中替换订单分类逻辑
        if self.direction == "NONE":
            # ✅ 修正：空仓时严格按 parentId 区分
            # 主单：parentId 为 0 或 None（IBKR 有时返回 None）
            self.entry_orders = [
                o for o in self.live_orders if getattr(o, "parentId", 0) in (0, None)
            ]
            # 子单：parentId ≠ 0（止损/止盈单）
            self.closing_orders = [
                o
                for o in self.live_orders
                if getattr(o, "parentId", 0) not in (0, None)
            ]
        else:
            # 有仓时：parentId + action 双重判定
            self.entry_orders = [
                o
                for o in self.live_orders
                if (
                    getattr(o, "parentId", 0) in (0, None)
                    and (
                        (self.direction == "LONG" and o.action == "BUY")
                        or (self.direction == "SHORT" and o.action == "SELL")
                    )
                )
            ]
            self.closing_orders = [
                o
                for o in self.live_orders
                if (
                    getattr(o, "parentId", 0) not in (0, None)  # 止损单（parentId≠0）
                    or (
                        (
                            self.direction == "LONG" and o.action == "SELL"
                        )  # 止盈单（方向相反）
                        or (self.direction == "SHORT" and o.action == "BUY")
                    )
                )
            ]
        # --- 4. 衍生数值计算 (保留原有逻辑) ---
        self.in_flight_closing_qty = sum(o.totalQuantity for o in self.closing_orders)
        self.in_flight_entry_qty = sum(o.totalQuantity for o in self.entry_orders)

        # 识别当前有效的活跃止损单
        self.active_stop_order = None
        for o in self.live_orders:
            ref = getattr(o, "orderRef", "") or ""
            otype = (getattr(o, "orderType", "") or "").upper()
            parent_id = getattr(o, "parentId", 0)
            # ✅ 必须是真正的 child order（Bracket Stop）
            if (
                otype in ("STP", "STP LMT")
                and parent_id not in (0, None)  # ← 关键修复
                and (ref.startswith("S_") or ref.startswith("ES_"))
            ):
                self.active_stop_order = o
                break

        # 识别止盈单
        self.tp1_active = any(
            getattr(o, "orderRef", "").startswith("TP1_") for o in self.closing_orders
        )
        self.tp2_active = any(
            getattr(o, "orderRef", "").startswith("TP2_") for o in self.closing_orders
        )
        # ✅ 最后调用 _parse_order_refs（此时 entry_orders 已存在）
        self._parse_order_refs()

    def __repr__(self):
        return f"<Snapshot Pos:{self.fact_pos} Cost:{self.avg_cost} Trades:{len(self.live_trades)}>"

    def _parse_order_refs(self):
        """
        [核心增强] 从 orderRef 解析订单特征
        orderRef 格式：{type}_{symbol}_{label}_{entry_type}_{timestamp}
        type: E=Entry, A=Add, S=Stop, TP1/TP2=TakeProfit,ES=Emergency Stop
        """
        self.order_features = {}  # 存储解析结果

        for order in self.live_orders:
            order_ref = getattr(order, "orderRef", "")
            if not order_ref:
                continue

            parts = order_ref.split("_")
            if len(parts) >= 4:
                order_type = parts[0]  # E/A/S/TP1/TP2/ES
                symbol = parts[1]  # AAPL,AMZN,AMD,NVDA
                label = parts[2] if len(parts) >= 3 else "Unknown"
                abv_action = parts[3]  # BOT/SLD
                action = ACTION_MAP.get(abv_action, abv_action.upper())
                # entry_type 通常是倒数第二部分（timestamp 之前）
                entry_type_abbrev = parts[-2] if len(parts) >= 4 else "ukn"
                entry_type = ENTRY_TYPE_FULL.get(
                    entry_type_abbrev, entry_type_abbrev.upper()
                )
                timestr = parts[-1]
                # 存储到字典，供 sync_position 查询
                self.order_features[order.orderId] = {
                    "order_type": order_type,  # E/A/S/TP1/TP2
                    "symbol": symbol,
                    "label": label,
                    "action": action,
                    "abv_action": abv_action,
                    "entry_type": entry_type,  # Breakout/GiftZone/Pullback
                    "entry_type_abbrev": entry_type_abbrev,  # 保留缩写（调试用）
                    "timestr": timestr,
                    "orderRef": order_ref,
                }
            else:
                sys_log(
                    f"⚠️ [orderRef 解析失败] orderId={order.orderId}, ref={order_ref}, parts={len(parts)}",
                    level="WARN",
                )
        # 🔥 便捷属性：主订单的 entry_type（供 sync_position 直接使用）
        if self.entry_orders:
            # 🔥 优先选择 order_type="E" 或 "A" 的订单
            main_order = next(
                (
                    o
                    for o in self.entry_orders
                    if self.order_features.get(o.orderId, {}).get("order_type")
                    in ["E", "A"]
                ),
                self.entry_orders[0],  # 兜底：用第一个
            )
            main_order_id = main_order.orderId
            self.main_order_id = main_order.orderId
            self.main_order_entry_type = self.order_features.get(main_order_id, {}).get(
                "entry_type", "Unknown"
            )
            self.main_order_label = self.order_features.get(main_order_id, {}).get(
                "label", "Unknown"
            )
            self.main_order_action = self.order_features.get(main_order_id, {}).get(
                "action", "Unknown"
            )
        else:
            self.main_order_id = None
            self.main_order_entry_type = "Unknown"
            self.main_order_label = "Unknown"
            self.main_order_action = "Unknown"

        # --- B. 止损单 (从 closing_orders 找 S 或 ES) 🔥 修正点 ---
        self.stop_order_id = None
        self.stop_order_label = None
        self.stop_order_action = None
        self.stop_order_entry_type = None  # 建议增加
        if self.closing_orders:
            stop_order = next(
                (
                    o
                    for o in self.closing_orders
                    if self.order_features.get(o.orderId, {}).get("order_type")
                    in ["S", "ES"]
                ),
                None,  # 没找到止损单则为 None
            )

            if stop_order:
                stop_feat = self.order_features.get(stop_order.orderId, {})
                self.stop_order_id = stop_order.orderId
                self.stop_order_label = stop_feat.get("label", "Unknown")  # 如 Law8L
                self.stop_order_action = stop_feat.get(
                    "action", "Unknown"
                )  # 如 BUY 或 SELL
                self.stop_order_entry_type = stop_feat.get("entry_type", "Unknown")
