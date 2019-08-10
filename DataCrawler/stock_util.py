from pymongo import ASCENDING
from DataCrawler.database import DB_CONN,TU_PRO
from datetime import datetime, timedelta
import tushare as ts

def get_trading_dates(begin_date=None, end_date=None):
    """
    获取指定日期范围的按照正序排列的交易日列表
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表
    """

    # 当前日期
    now = datetime.now()
    # 开始日期，默认今天向前的365个自然日
    if begin_date is None:
        # 当前日期减去365天
        one_year_ago = now - timedelta(days=365)
        # 转化为str类型
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    # # 用上证综指000001作为查询条件，因为指数是不会停牌的，所以可以查询到所有的交易日
    # daily_cursor = DB_CONN.daily.find(
    #     {'code': '000001', 'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
    #     sort=[('date', ASCENDING)],
    #     projection={'date': True, '_id': False})
    #
    # # 转换为日期列表
    # dates = [x['date'] for x in daily_cursor]

    trade_cal = ts.trade_cal()
    dates = trade_cal[(trade_cal.calendarDate >= begin_date) & (trade_cal.calendarDate <= end_date) & (trade_cal.isOpen == 1)]['calendarDate'].values

    return dates


def get_all_codes():
    """
    获取所有股票代码列表

    :return: 股票代码列表
    """
    # return ts.get_stock_basics().index.values.tolist()

    # 通过distinct函数拿到所有不重复的股票代码列表
    return DB_CONN.daily_hfq.distinct('code')


if __name__ == '__main__':
    res = get_all_codes()
    # print(DB_CONN.daily_hfq.distinct('code'))

    # dates = get_trading_dates('2018-09-22','2019-07-26')
    # print(dates)

    # calendarDate isOpen
    # trade_cal = ts.trade_cal()
    # date_range = trade_cal[(trade_cal.calendarDate >= '2018-09-22') & (trade_cal.calendarDate <= '2019-07-26') & (trade_cal.isOpen == 1)]['calendarDate'].values
    # print(date_range)

    # date1 = TU_PRO.trade_cal(start_date='20180922',end_date='20190726')
    # date1 = date1[date1['is_open'] ==1]['cal_date'].values.tolist()
    # print(date1)

    # data = get_all_codes()
    # print(len(data),data)

    # data1 = ts.get_stock_basics().index.values.tolist()
    # print(len(data1),data1)

    # data = TU_PRO.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # print(data)