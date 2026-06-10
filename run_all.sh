#!/usr/bin/env bash
# ============================================================
# run_all.sh —— 用 tmux 一键启动 8 只股票，每只独占一个窗口
#
# 用法:
#   ./run_all.sh            启动并进入 tmux 总控台
#
# tmux 常用快捷键 (前缀键默认 Ctrl+b)：
#   Ctrl+b  n / p      下一个 / 上一个 窗口(股票)
#   Ctrl+b  0..7       直接跳到第 N 个窗口
#   Ctrl+b  w          列出所有窗口选择
#   Ctrl+b  d          脱离(detach)，程序继续后台运行
#   重新进入:  tmux attach -t velez
#   全部关闭:  tmux kill-session -t velez
# ============================================================

PROJECT_DIR=~/velez/velez
SESSION=velez

# 股票 -> clientId 映射 (顺序即窗口顺序)
PAIRS=(
  "AAPL:31"
  "AMD:32"
  "AMZN:33"
  "GOOG:34"
  "META:35"
  "MSFT:36"
  "NVDA:37"
  "TSLA:38"
)

# 若同名会话已存在，避免重复启动
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "⚠️  tmux 会话 '$SESSION' 已存在。"
  echo "    进入查看:  tmux attach -t $SESSION"
  echo "    全部关闭:  tmux kill-session -t $SESSION"
  exit 1
fi

first=1
for pair in "${PAIRS[@]}"; do
  symbol="${pair%%:*}"
  clientid="${pair##*:}"
  cmd="cd $PROJECT_DIR && source .venv/bin/activate && python main.py --symbol $symbol --clientid $clientid"

  if [ $first -eq 1 ]; then
    tmux new-session -d -s "$SESSION" -n "$symbol"
    first=0
  else
    tmux new-window -t "$SESSION" -n "$symbol"
  fi
  tmux send-keys -t "${SESSION}:${symbol}" "$cmd" C-m
done

echo "✅ 已在 tmux 会话 '$SESSION' 中启动 ${#PAIRS[@]} 只股票，正在进入总控台..."
tmux select-window -t "${SESSION}:0"
tmux attach -t "$SESSION"
