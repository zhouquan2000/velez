#!/usr/bin/env bash
# ============================================================
# run_one.sh —— 单只股票运行脚本
# 用法:
#   ./run_one.sh              # 默认 AAPL / clientid 31
#   ./run_one.sh AMD 32       # 指定 股票代码 与 clientid
# ============================================================
set -e

SYMBOL="${1:-AAPL}"
CLIENTID="${2:-31}"
# 第 3 个参数可选：端口。不传 = auto（自动识别 TWS Windows/Linux 与 IB Gateway）
PORT_ARG="${3:-auto}"

cd ~/velez/velez
source .venv/bin/activate
echo "🚀 启动 $SYMBOL (clientId=$CLIENTID, port=$PORT_ARG) ..."
python main.py --symbol "$SYMBOL" --clientid "$CLIENTID" --port "$PORT_ARG"
