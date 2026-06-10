#!/usr/bin/env bash
# ============================================================
# run_all_wt.sh —— 用 Windows Terminal 启动 8 只股票
#                   每只股票占用一个独立的标签页(tab)
#
# 用法:
#   ./run_all_wt.sh
#
# 说明: 需要 Windows Terminal (wt.exe)。每个 tab 跑完/出错后
#       会停留在 bash 提示符(exec bash)，方便查看输出，不会自动关闭。
# ============================================================

PROJECT_DIR=~/velez/velez

# 股票 -> clientId 映射 (顺序即标签页顺序)
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

args=()
first=1
for pair in "${PAIRS[@]}"; do
  symbol="${pair%%:*}"
  clientid="${pair##*:}"
  inner="cd $PROJECT_DIR && source .venv/bin/activate && python main.py --symbol $symbol --clientid $clientid; exec bash"

  if [ $first -eq 1 ]; then
    args+=( new-tab --title "$symbol" wsl.exe -- bash -lic "$inner" )
    first=0
  else
    # ';' 是 wt.exe 的「新标签页」分隔符
    args+=( ";" new-tab --title "$symbol" wsl.exe -- bash -lic "$inner" )
  fi
done

echo "✅ 正在通过 Windows Terminal 启动 ${#PAIRS[@]} 个标签页..."
wt.exe "${args[@]}"
