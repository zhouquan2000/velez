#!/usr/bin/env python3
"""
测试键盘 Ctrl+C 是否能产生 SIGINT 信号并送达前台进程。
用法: 在 VSC 终端里运行  python3 test_ctrlc.py ，然后按 Ctrl+C
"""
import signal
import time
import os


def handler(sig, frame):
    print("\n✅✅✅ 收到 SIGINT (Ctrl+C) 信号！", flush=True)
    print("结论: 键盘 → VSC终端 → 进程 这条链路正常", flush=True)
    os._exit(0)


signal.signal(signal.SIGINT, handler)

print(f"测试进程 PID={os.getpid()}", flush=True)
print("请按 Ctrl+C，观察是否出现 ✅✅✅ 提示...", flush=True)
print("(如果按了多次、等了 10 秒都没反应，说明 Ctrl+C 没送达这个进程)", flush=True)

while True:
    time.sleep(1)
