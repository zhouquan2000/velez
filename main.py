# 2026-02-22 在Kevin Zhou的帮助下，用git把原来的Test-Paper-08-12.py拆分成几个.py程序，
# 主程序 main.py 放在 TWS_IB_Project\velez下面，作为每次提交运行的主程序
# 其他6个拆分的.py程序都放在 TWS_IB_Project\velez\velez_bot子目录下面：
#  __init__.py; app.py; context_snapshot.py; events.py; shared.py; trading_context.py
#  提交运行程序的方式： 在vsc的 terminal里面， 先运行
#  & C:/Data/IB_Robot/venv/Scripts/Activate.ps1 激活虚拟环境
# 然后再运行： (venv) PS C:\Data\IB_Robot\TWS_IB_Project\velez> python main.py --symbol AAPL --clinedid 31
# 其中 --symbol 空格后面是股票代码， --clientid 空格后面是 连接IBKR服务器的cliendID的序号
# Test-Paper-08-12.py   2026-01-15 开始重构---2026-02-20 最后update
# 对原来的 Test-Paper-AAPL-OliverLaw_06-9.py 进行重大升级改造---重构程序
# 目前已经包括了Oliver Velez的全部八条交易法则里面的7条，除了
# 除了Law #7: 200MA Reversion，因为该法则必要性：低。NASDAQ 高成长股常年偏离 200MA，逆势风险极大。

from velez_bot.app import run

if __name__ == "__main__":
    run()
