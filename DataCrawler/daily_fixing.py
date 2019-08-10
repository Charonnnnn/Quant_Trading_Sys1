from datetime import datetime, timedelta
from pymongo import UpdateOne, ASCENDING
from DataCrawler.database import DB_CONN
from DataCrawler.stock_util import get_trading_dates, get_all_codes
import traceback
"""
对日行情数据做进一步的处理：
1. fill_is_trading_between - 填充is_trading字段，is_trading用来区分某只股票在某个交易日是否为停牌
2. fill_daily_k_at_suspension_days - 填充停牌日的行情数据
3. fill_au_factor_pre_close - 填充复权因子和前收
"""


def fill_is_trading_between(start_date=None, end_date=None):
    """
    填充指定时间段内的is_trading字段，表示是否交易的状态，True - 交易  False - 停牌
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    all_dates = get_trading_dates(start_date, end_date)
    for date in all_dates:
        # 填充数据集
        fill_single_date_is_trading(date, 'daily')
        fill_single_date_is_trading(date, 'daily_hfq')

def fill_single_date_is_trading(date,collection_name):
    """
    填充某一个日行情的数据集的is_trading
    :param date: 日期
    :param collection_name: 集合名称
    """
    print('填充字段， 字段名: is_trading，日期：%s，数据集：%s' %(date, collection_name), flush=True)

    '''
    `projection` (optional):: a list of field names that should be
    returned in the result set or a dict specifying the fields
    to include or exclude. If `projection` is a list "_id" will
    always be returned. Use a dict to exclude fields from
    the result (e.g. projection={'_id': False}).  返回内容
    `batch_size` (optional): Limits the number of documents returned in
            a single batch.
    '''
    daily_cursor = DB_CONN[collection_name].find({'date':date}, projection={'code':True, 'volume':True, '_id':True}, batch_size=1000)

    update_requests = []
    for daily in daily_cursor:
        # 当日成交量大于0, 则为交易状态(非停牌)
        is_trading = daily['volume'] > 0

        update_requests.append(UpdateOne(
                {'code': daily['code'], 'date': date},
                {'$set': {'is_trading': is_trading}}))
    if len(update_requests) > 0:
        update_result = DB_CONN[collection_name].bulk_write(update_requests,ordered=False)
        print('填充字段， 字段名: is_trading，日期：%s，数据集：%s，更新：%4d条' %
              (date, collection_name, update_result.modified_count), flush=True)

def fill_daily_k_at_suspension_days(start_date, end_date):
    '''
    填充指定日期范围内，股票停牌日的行情数据。
    填充时，停牌的开盘价、最高价、最低价和收盘价都为最近一个交易日的收盘价，成交量为0，
    is_trading是False

    :param
    start_date: 开始日期
    :param
    end_date: 结束日期
    '''
    # 当前日期的前一天
    before = datetime.now() - timedelta(days=1)
    # 找到据当前最近一个交易日的所有股票的基本信息
    basics = []
    while 1:
        # 转化为str
        last_trading_date = before.strftime('%Y-%m-%d')
        # 因为TuShare的基本信息最早知道2016-08-09，所以如果日期早于2016-08-09
        # 则结束查找
        if last_trading_date < '2016-08-09':
            break

        # 找到当日的基本信息
        basic_cursor = DB_CONN['basic'].find(
            {'date': last_trading_date},
            # 填充时需要用到两个字段股票代码code和上市日期timeToMarket，
            # 上市日期用来判断
            projection={'code': True, 'timeToMarket': True, '_id': False},
            # 一次返回5000条，可以降低网络IO开销，提高速度
            batch_size=5000)

        # 将数据放到basics列表中
        basics = [basic for basic in basic_cursor]

        # 如果查询到了数据，在跳出循环
        if len(basics) > 0:
            break

        # 如果没有找到数据，则继续向前一天
        before -= timedelta(days=1)

    # 获取指定日期范围内所有交易日列表
    all_dates = get_trading_dates(start_date, end_date)

    # 填充daily数据集中的停牌日数据
    fill_daily_k_at_suspension_days_at_date_one_collection(basics, all_dates, 'daily')
    # 填充daily_hfq数据中的停牌日数据
    fill_daily_k_at_suspension_days_at_date_one_collection(basics, all_dates, 'daily_hfq')

def fill_daily_k_at_suspension_days_at_date_one_collection(basics, all_dates, collection):
    """
    更新单个数据集的单个日期的数据
    :param basics:
    :param all_dates:
    :param collection:
    :return:
    """
    code_last_trading_daily_dict = dict()
    for date in all_dates:
        update_requests = []
        last_daily_code_set = set(code_last_trading_daily_dict.keys())
        for basic in basics:
            code = basic['code']
            # 如果循环日期小于
            if date < basic['timeToMarket']:
                print('日期：%s, %s 还没上市，上市日期: %s' % (date, code, basic['timeToMarket']), flush=True)
            else:
                # 找到当日数据
                daily = DB_CONN[collection].find_one({'code': code, 'date': date})
                if daily is not None:
                    code_last_trading_daily_dict[code] = daily
                    last_daily_code_set.add(code)
                else:
                    if code in last_daily_code_set:
                        last_trading_daily = code_last_trading_daily_dict[code]
                        suspension_daily_doc = {
                            'code': code,
                            'date': date,
                            'close': last_trading_daily['close'],
                            'open': last_trading_daily['close'],
                            'high': last_trading_daily['close'],
                            'low': last_trading_daily['close'],
                            'volume': 0,
                            'is_trading': False
                        }
                        update_requests.append(
                            UpdateOne(
                                {'code': code, 'date': date},
                                {'$set': suspension_daily_doc},
                                upsert=True))
        if len(update_requests) > 0:
            update_result = DB_CONN[collection].bulk_write(update_requests, ordered=False)
            print('填充停牌数据，日期：%s，数据集：%s，插入：%4d条，更新：%4d条' %
                  (date, collection, update_result.upserted_count, update_result.modified_count), flush=True)



def fill_au_factor_pre_close(start_date, end_date):
    """
    为daily数据集填充：
    1. 复权因子au_factor，复权的因子计算方式：au_factor = hfq_close/close
    2. 前收pre_close = close(-1) * au_factor(-1)/au_factor
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    all_codes = get_all_codes()
    print(all_codes)

    for code in all_codes:
        hfq_daily_cursor = DB_CONN['daily_hfq'].find(
            {'code': code, 'date': {'$lte': end_date, '$gte': start_date}},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True})

        date_hfq_close_dict = dict([(x['date'], x['close']) for x in hfq_daily_cursor])

        daily_cursor = DB_CONN['daily'].find(
            {'code': code, 'date': {'$lte': end_date, '$gte': start_date}, 'index': False},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True}
        )

        last_close = -1
        last_au_factor = -1

        update_requests = []
        for daily in daily_cursor:
            date = daily['date']
            try:
                close = daily['close']

                doc = dict()

                # 复权因子 = 当日后复权价格 / 当日实际价格
                au_factor = round(date_hfq_close_dict[date] / close, 2)
                doc['au_factor'] = au_factor
                # 当日前收价 = 前一日实际收盘价 * 前一日复权因子 / 当日复权因子 (可直接用shift()获取前日收盘价)
                if last_close != -1 and last_au_factor != -1:
                    pre_close = last_close * last_au_factor / au_factor
                    doc['pre_close'] = round(pre_close, 2)

                last_au_factor = au_factor
                last_close = close

                update_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date, 'index': False},
                        {'$set': doc}))
            except:
                print('计算复权因子时发生错误，股票代码：%s，日期：%s' % (code, date), flush=True)
                traceback.print_exc()
                # 恢复成初始值，防止用错
                last_close = -1
                last_au_factor = -1

        if len(update_requests) > 0:
            update_result = DB_CONN['daily'].bulk_write(update_requests, ordered=False)
            print('填充复权因子和前收，股票：%s，更新：%4d条' %
                  (code, update_result.modified_count), flush=True)




if __name__ == '__main__':
    fill_au_factor_pre_close('2019-01-01', '2019-08-03')

    # Tushare里没有停牌(交易量为0)的数据
    # fill_is_trading_between('2019-07-09', '2019-07-30')
    # fill_daily_k_at_suspension_days('2019-07-09', '2019-07-30')

    import tushare as ts
    # da = ts.get_k_data('600647',index=True)
    # da = ts.get_hist_data('600647')
    # print(da)
    # print(da[da['volume']==0])
    # li = get_all_codes()
    # print(li)
    #
    # da = ts.get_k_data('603810',start='2019-07-01',end='2019-07-30')
    # print(da)
    # print(da[da['volume']==0])
    # for i in li:
    #     da = ts.get_k_data(i)
    #     print(da[da['volume']==0])
