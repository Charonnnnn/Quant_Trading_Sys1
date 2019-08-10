from DataCrawler.database import DB_CONN
from pymongo import DESCENDING

def is_k_up_break_ma10(code, _date):
    """
    判断某只股票在某日是否满足K线上穿10日均线

    :param code: 股票代码
    :param _date: 日期
    :return: True/False
    """

    # 从后复权的日行情数据集中根据股票代码、日期和是否为正常交易的条件查询一条数据，
    # 如果能找到数据，则认为当日股票是正常交易状态，否则为停牌
    current_daily = DB_CONN['daily_hfq'].find_one(
        {'code': code, 'date': _date, 'is_trading': True})

    # 没有找到股票的日行情数据，则认为不符合日线收盘价上穿10日均线的条件
    if current_daily is None:
        print('计算信号，K线上穿MA10，当日没有K线，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 从后复权的日行情数据集中查询是11条数据，因为要计算连着两个交易日的10日均线价格，所以是需要11条数据。
    # 才能保证取到的是邻近的10个交易日的数据。
    daily_cursor = DB_CONN['daily_hfq'].find(
        {'code': code, 'date': {'$lte': _date}},
        limit=11,
        # 10日均线计算的时候是包含当日在内的向前的连续10个交易日的收盘价的平均值，所以要按照时间倒序排列，
        sort=[('date', DESCENDING)],
        # 计算价格均线时，只需要用到价格，并且如果连续10个交易日内有停牌情况，则不进行计算，
        # 所以projection里只需要close和is_trading。
        projection={'code': True, 'close': True, 'is_trading': True}
    )

    # 从游标取出日行情数据放进列表中
    dailies = [x for x in daily_cursor]

    # 如果数据不满足11个，也就说，无法计算两个交易日的MA10，则认为不符合上穿的条件
    if len(dailies) < 11:
        print('计算信号，K线上穿MA10，前期K线不足，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 查询时是倒序排列的，而计算MA10时是向前10根，所以要将顺序反转
    dailies.reverse()

    # 计算前一个交易日收盘价和MA10的关系
    last_close_2_last_ma10 = compare_close_2_ma_10(dailies[0:10])
    # 计算当前交易日收盘价和MA10的关系
    current_close_2_current_ma10 = compare_close_2_ma_10(dailies[1:])

    # 将关键数据打印出来，以便于比对
    print('计算信号，K线上穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10)), flush=True)

    # 前一日或者当日任意一天的收盘价和MA10的关系不存在，则都认为不符合上穿的条件
    if last_close_2_last_ma10 is None or current_close_2_current_ma10 is None:
        return False

    # 只有前一日收盘价小于等于10，且当前交易日的收盘价大于MA10，则认为当日收盘价上穿MA10
    is_break = (last_close_2_last_ma10 <= 0) & (current_close_2_current_ma10 == 1)

    print('计算信号，K线上穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s，突破：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10), str(is_break)), flush=True)

    # 返回判断结果
    return is_break


def is_k_down_break_ma10(code, _date):
    """
    判断某只股票在某日是否满足K线下穿10日均线

    :param code: 股票代码
    :param _date: 日期
    :return: True/False
    """
    # 从后复权的日行情数据集中根据股票代码、日期和是否为正常交易的条件查询一条数据，
    # 如果能找到数据，则认为当日股票是正常交易状态，否则为停牌
    current_daily = DB_CONN['daily'].find_one(
        {'code': code, 'date': _date, 'is_trading': True})

    # 没有找到股票的日行情数据，则认为不符合日线收盘价下穿10日均线的条件
    if current_daily is None:
        print('计算信号，K线下穿MA10，当日没有K线，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 从后复权的日行情数据集中查询是11条数据，因为要计算连着两个交易日的10日均线价格，所以是需要11条数据。
    # 才能保证取到的是邻近的10个交易日的数据。
    daily_cursor = DB_CONN['daily_hfq'].find(
        {'code': code, 'date': {'$lte': _date}},
        limit=11,
        # 10日均线计算的时候是包含当日在内的向前的连续10个交易日的收盘价的平均值，所以要按照时间倒序排列，
        sort=[('date', DESCENDING)],
        # 计算价格均线时，只需要用到价格，并且如果连续10个交易日内有停牌情况，则不进行计算，
        # 所以projection里只需要close和is_trading。
        projection={'code': True, 'close': True, 'is_trading': True}
    )

    # 从游标取出日行情数据放进列表中
    dailies = [x for x in daily_cursor]

    # 如果数据不满足11个，也就说，无法计算两个交易日的MA10，则认为不符合下穿的条件
    if len(dailies) < 11:
        print('计算信号，K线下穿MA10，前期K线不足，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 查询时是倒序排列的，而计算MA10时是向前10根，所以要将顺序反转
    dailies.reverse()

    # 计算前一个交易日收盘价和MA10的关系
    last_close_2_last_ma10 = compare_close_2_ma_10(dailies[0:10])
    # 计算当前交易日收盘价和MA10的关系
    current_close_2_current_ma10 = compare_close_2_ma_10(dailies[1:])

    # 前一日或者当日任意一天的收盘价和MA10的关系不存在，则都认为不符合下穿的条件
    if last_close_2_last_ma10 is None or current_close_2_current_ma10 is None:
        return False

    # 只有前一日收盘价大于等于10，且当前交易日的收盘价小于MA10，则认为当日收盘价下穿MA10
    is_break = (last_close_2_last_ma10 >= 0) & (current_close_2_current_ma10 == -1)

    print('计算信号，K线下穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s, 突破：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10), str(is_break)), flush=True)

    return is_break


def compare_close_2_ma_10(dailies):
    """
    比较当前的收盘价和MA10的关系
    :param dailies: 日线列表，10个元素，最后一个是当前交易日
    :return: 0 相等，1 大于， -1 小于, None 结果未知
    """
    current_daily = dailies[9]
    close_sum = 0
    for daily in dailies:
        # 10天当中，只要有一天停牌则返回False
        if 'is_trading' not in daily or daily['is_trading'] is False:
            return None

        # 用后复权累计
        close_sum += daily['close']

    # 计算MA10
    ma_10 = close_sum / 10

    # 判断收盘价和MA10的大小
    post_adjusted_close = current_daily['close']
    differ = post_adjusted_close - ma_10

    # print('计算信号，股票： %s, 收盘价：%7.2f, MA10: %7.2f, 差值：%7.2f' %
    #       (code, post_adjusted_close, ma_10, differ), flush=True)
    if differ > 0:
        return 1
    elif differ < 0:
        return -1
    else:
        return 0


