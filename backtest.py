import pandas as pd
import matplotlib.pyplot as plt

from Strategy.stock_pool_strategy import stock_pool, find_out_stocks
from DataCrawler.database import DB_CONN
from DataCrawler.stock_util import get_trading_dates
from IndexComputing.sharpe_ratio_computing import compute_sharpe_ratio
from IndexComputing.max_drawdown_computing import compute_drawdown
from IndexComputing.ir_computing import compute_ir

from Signal.k_ma10_factor import is_k_down_break_ma10,is_k_up_break_ma10
from Signal.macd_factor import is_macd_gold, is_macd_dead


"""
完成策略的回测，绘制以沪深300为基准的收益曲线，并计算策略评价指标：
年化收益、最大回撤、夏普比率
"""

def backtest(begin_date, end_date):
    """
    策略回测。结束后打印出收益曲线(沪深300基准)、年化收益、最大回撤、

    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """

    # 初始现金1000万
    cash = 1E7
    # 单只股票的仓位是20万
    single_position = 2E5

    # 时间为key的净值、收益和同期沪深基准
    df_profit = pd.DataFrame(columns=['net_value', 'profit', 'hs300'])
    # 时间为key的单日收益和同期沪深基准
    df_day_profit = pd.DataFrame(columns=['profit', 'hs300'])

    # 获取回测开始日期和结束之间的所有交易日，并且是按照正序排列
    all_dates = get_trading_dates(begin_date, end_date)

    # 获取沪深300的在回测开始的第一个交易日的值
    hs300_begin_value = DB_CONN['daily'].find_one(
        {'code': '000300', 'index': True, 'date': all_dates[0]},
        projection={'close': True})['close']

    # 获取回测周期内的股票池数据，
    # adjust_dates：正序排列的调整日列表；
    # date_codes_dict： 调整日和当期的股票列表组成的dict，key是调整日，value是股票代码列表
    adjust_dates, date_codes_dict = stock_pool(begin_date, end_date)

    # 股票池上期股票代码列表
    last_phase_codes = None
    # 股票池当期股票代码列表
    this_phase_codes = None
    # 待卖的股票代码集合
    to_be_sold_codes = set()
    # 待买的股票代码集合
    to_be_bought_codes = set()
    # 持仓股票dict，key是股票代码，value是一个dict，
    # 三个字段分别为：cost - 持仓成本，volume - 持仓数量，last_value：前一天的市值
    holding_code_dict = dict()
    # 前一个交易日
    last_date = None
    # 前一天的总资产值，初始值为初始总资产
    last_total_capital = 1e7
    # 前一天的HS300值，初始值为第一天的值
    last_hs300_close = hs300_begin_value
    # 净值
    net_value = 1
    # 在交易日的顺序，一天天完成信号检测
    for _date in all_dates:
        print('Backtest at %s.' % _date)

        # 当期持仓股票的代码列表
        before_sell_holding_codes = list(holding_code_dict.keys())

        """
        持仓股的除权除息处理
        如果当前不是第一个交易日，并且有持仓股票，则处理除权除息对持仓股的影响
        这里的处理只考虑复权因子的变化，而实际的复权因子变化有可能是因为除权、除息以及配股，
        那么具体的持仓股变化要根据它们的不同逻辑来处理
        """
        if last_date is not None and len(before_sell_holding_codes) > 0:
            # 从daily数据集中查询出所有持仓股的前一个交易日的复权因子
            last_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': before_sell_holding_codes}, 'date': last_date, 'index': False},
                projection={'code': True, 'au_factor': True})

            # 构造一个dict，key是股票代码，value是上一个交易日的复权因子
            code_last_aufactor_dict = dict([(daily['code'], daily['au_factor']) for daily in last_daily_cursor])

            # 从daily数据集中查询出所有持仓股的当前交易日的复权因子
            current_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': before_sell_holding_codes}, 'date': _date, 'index': False},
                projection={'code': True, 'au_factor': True})

            # 一只股票一只股票进行处理
            for current_daily in current_daily_cursor:
                # 当前交易日的复权因子
                current_aufactor = current_daily['au_factor']
                # 股票代码
                code = current_daily['code']
                # 从持仓股中找到该股票的持仓数量
                last_volume = holding_code_dict[code]['volume']
                # 如果该股票存在前一个交易日的复权因子，则对持仓股数量进行处理
                if code in code_last_aufactor_dict:
                    # 上一个交易日的复权因子
                    last_aufactor = code_last_aufactor_dict[code]
                    # 计算复权因子变化后的持仓股票数量，如果复权因子不发生变化，那么持仓数量是不发生变化的
                    # 相关公式是：
                    # 市值不变：last_close * last_volume = pre_close * current_volume
                    # 价格的关系：last_close * last_aufactor = pre_close * current_aufactor
                    # 转换之后得到下面的公式：
                    current_volume = int(last_volume * (current_aufactor / last_aufactor))
                    # 改变持仓数量
                    holding_code_dict[code]['volume'] = current_volume
                    print('持仓量调整：%s, %6d, %10.6f, %6d, %10.6f' %
                          (code, last_volume, last_aufactor, current_volume, current_aufactor))

        """
        卖出的逻辑处理：
        卖出价格是当日的开盘价，卖出的数量就是持仓股的数量，卖出后获得的资金累加到账户的可用现金上
        """

        print('待卖股票池：', to_be_sold_codes, flush=True)
        # 如果有待卖股票，则继续处理
        if len(to_be_sold_codes) > 0:
            # 从daily数据集中查询所有待卖股票的开盘价，这里用的不复权的价格，以模拟出真实的交易情况
            sell_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': list(to_be_sold_codes)}, 'date': _date, 'index': False, 'is_trading': True},
                projection={'open': True, 'code': True}
            )

            # 一只股票一只股票处理
            for sell_daily in sell_daily_cursor:
                # 待卖股票的代码
                code = sell_daily['code']
                # 如果股票在持仓股里
                if code in before_sell_holding_codes:
                    # 获取持仓股
                    holding_stock = holding_code_dict[code]
                    # 获取持仓数量
                    holding_volume = holding_stock['volume']
                    # 卖出价格为当日开盘价
                    sell_price = sell_daily['open']
                    # 卖出获得金额为持仓量乘以卖出价格
                    sell_amount = holding_volume * sell_price
                    # 卖出得到的资金加到账户的可用现金上
                    cash += sell_amount

                    # 获取该只股票的持仓成本
                    cost = holding_stock['cost']
                    # 计算持仓的收益
                    single_profit = (sell_amount - cost) * 100 / cost
                    print('卖出 %s, %6d, %6.2f, %8.2f, %4.2f' %
                          (code, holding_volume, sell_price, sell_amount, single_profit))

                    # 删除该股票的持仓信息
                    del holding_code_dict[code]
                    to_be_sold_codes.remove(code)

        print('卖出后，现金: %10.2f' % cash)

        """
        买入的逻辑处理：
        买入的价格是当日的开盘价，每只股票可买入的金额为20万，如果可用现金少于20万，就不再买入了
        """
        print('待买股票池：', to_be_bought_codes, flush=True)
        # 如果待买股票集合不为空，则执行买入操作
        if len(to_be_bought_codes) > 0:
            # 获取所有待买入股票的开盘价
            buy_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': list(to_be_bought_codes)}, 'date': _date, 'is_trading': True, 'index': False},
                projection={'code': True, 'open': True}
            )

            # 处理所有待买入股票
            for buy_daily in buy_daily_cursor:
                # 判断可用资金是否够用
                if cash > single_position:
                    # 获取买入价格
                    buy_price = buy_daily['open']
                    # 获取股票代码
                    code = buy_daily['code']
                    # 获取可买的数量，数量必须为正手数
                    volume = int(int(single_position / buy_price) / 100) * 100
                    # 买入花费的成本为买入价格乘以实际的可买入数量
                    buy_amount = buy_price * volume
                    # 从现金中减去本次花费的成本
                    cash -= buy_amount
                    # 增加持仓股中
                    holding_code_dict[code] = {
                        'volume': volume,         # 持仓量
                        'cost': buy_amount,       # 持仓成本
                        'last_value': buy_amount  # 初始前一日的市值为持仓成本
                    }

                    print('买入 %s, %6d, %6.2f, %8.2f' % (code, volume, buy_price, buy_amount))

        print('买入后，现金: %10.2f' % cash)

        # 持仓股代码列表
        holding_codes = list(holding_code_dict.keys())

        """
        股票池调整日的处理逻辑：
        如果当前日期是股票池调整日，那么需要获取当期的备选股票列表，同时找到
        本期被调出的股票，如果这些被调出的股票是持仓股，则需要卖出
        """
        # 判断当前交易日是否为股票池的调整日
        if _date in adjust_dates:
            print('股票池调整日：%s，备选股票列表：' % _date, flush=True)

            # 如果上期股票列表存在，也就是当前不是第一期股票，则将
            # 当前股票列表设为上期股票列表
            if this_phase_codes is not None:
                last_phase_codes = this_phase_codes

            # 获取当期的股票列表
            this_phase_codes = date_codes_dict[_date]
            print(this_phase_codes, flush=True)

            # 如果存在上期的股票列表，则需要找出被调出的股票列表
            if last_phase_codes is not None:
                # 找到被调出股票池的股票列表
                out_codes = find_out_stocks(last_phase_codes, this_phase_codes)
                # 将所有被调出的且是在持仓中的股票添加到待卖股票集合中
                for out_code in out_codes:
                    if out_code in holding_code_dict:
                        to_be_sold_codes.add(out_code)

        # 检查是否有需要第二天卖出的股票
        # 持仓列表的股票，出现卖出信号
        for holding_code in holding_codes:
            if is_macd_dead(holding_code, _date):
                to_be_sold_codes.add(holding_code)

        # 检查是否有需要第二天买入的股票
        to_be_bought_codes.clear()
        if this_phase_codes is not None:
            # 在当前备选股里，但是非持仓股，并且出现了买入信号
            for _code in this_phase_codes:
                if _code not in holding_codes and is_macd_gold(_code, _date):
                    to_be_bought_codes.add(_code)

        # 计算总资产
        total_value = 0

        # 获取所有持仓股的当日收盘价
        holding_daily_cursor = DB_CONN['daily'].find(
            {'code': {'$in': holding_codes}, 'date': _date},
            projection={'close': True, 'code': True}
        )

        # 计算所有持仓股的总市值
        for holding_daily in holding_daily_cursor:
            code = holding_daily['code']
            holding_stock = holding_code_dict[code]
            # 单只持仓的市值等于收盘价乘以持仓量
            value = holding_daily['close'] * holding_stock['volume']
            # 总市值等于所有持仓股市值的累加之和
            total_value += value

            # 计算单只股票的持仓收益
            profit = (value - holding_stock['cost']) * 100 / holding_stock['cost']
            # 计算单只股票的单日收益
            one_day_profit = (value - holding_stock['last_value']) * 100 / holding_stock['last_value']
            # 更新前一日市值
            holding_stock['last_value'] = value
            print('持仓: %s, %10.2f, %4.2f, %4.2f' %
                  (code, value, profit, one_day_profit))

        # 总资产等于总市值加上总现金
        total_capital = total_value + cash

        # 获取沪深300的当日收盘值
        hs300_current_value = DB_CONN['daily'].find_one(
            {'code': '000300', 'index': True, 'date': _date},
            projection={'close': True})['close']

        print('收盘后，现金: %10.2f, 总资产: %10.2f' % (cash, total_capital))
        last_date = _date
        # 将当日的净值、收益和沪深300的涨跌幅放入DataFrame
        df_profit.loc[_date] = {
            'net_value': round(total_capital / 1e7, 2),
            'profit': round(100 * (total_capital - 1e7) / 1e7, 2),
            'hs300': round(100 * (hs300_current_value - hs300_begin_value) / hs300_begin_value, 2)
        }
        # 计算单日收益
        df_day_profit.loc[_date] = {
            'profit': round(100 * (total_capital - last_total_capital) / last_total_capital, 2),
            'hs300': round(100 * (hs300_current_value - last_hs300_close) / last_hs300_close, 2)
        }
        # 暂存当日的总资产和HS300，作为下一个交易日计算单日收益的基础
        last_total_capital = total_capital
        last_hs300_close = hs300_current_value

    print('累积收益', flush=True)
    print(df_profit, flush=True)
    print('单日收益', flush=True)
    print(df_day_profit, flush=True)

    # 计算最大回撤
    drawdown = compute_drawdown(df_profit['net_value'])
    # 计算年化收益和夏普比率
    annual_profit, sharpe_ratio = compute_sharpe_ratio(net_value, df_day_profit)
    # 计算信息率
    ir = compute_ir(df_day_profit)

    print('回测结果 %s - %s，年化收益： %7.3f，最大回撤：%7.3f，夏普比率：%4.2f，信息率：%4.2f' %
          (begin_date, end_date, annual_profit, drawdown, sharpe_ratio, ir))

    df_profit.plot(title='Backtest Result', y=['profit', 'hs300'], kind='line')
    plt.show()




if __name__ == "__main__":
    backtest('2015-01-01', '2015-12-31')
