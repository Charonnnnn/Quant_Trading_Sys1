from DataCrawler.database import DB_CONN
from DataCrawler.stock_util import get_all_codes

from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback

def compute_boll(start_date, end_date):
    """
    计算指定日期内的Boll突破上轨和突破下轨信号，并保存到数据库中，
    方便查询使用
    :param start_date: 开始日期
    :param end_date: 结束日期
    """

    all_codes = get_all_codes()
    N = 20

    for index,code in enumerate(all_codes):
        try:
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': start_date, '$lte': end_date}},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            )

            # df_daily = DataFrame([daily for daily in daily_cursor])
            df_daily = DataFrame(list(daily_cursor))

            if df_daily.index.size < N:
                print('数据量不够： %s, 只有: %d' % (code,df_daily.index.size), flush=True)
                continue

            # 计算MB，盘后计算，这里用当日的Close
            df_daily['MB'] = df_daily['close'].rolling(N).mean()
            # 计算STD20，计算20日的标准差
            df_daily['std'] = df_daily['close'].rolling(N).std()

            # 计算UP，上轨
            df_daily['UP'] = df_daily['MB'] + 2 * df_daily['std']
            # 计算down，下轨
            df_daily['DOWN'] = df_daily['MB'] - 2 * df_daily['std']

            df_daily.set_index(['date'], inplace=True)


            # 将close移动一个位置，变为当前索引位置的前收
            last_close = df_daily['close'].shift(1)
            # 将上轨移一位，前一日的上轨和前一日的收盘价都在当日了
            shifted_up = df_daily['UP'].shift(1)
            # 突破上轨，是向上突破，条件是前一日收盘价小于前一日上轨，当日收盘价大于当日上轨
            df_daily['up_mask'] = (last_close <= shifted_up) & (df_daily['close'] > shifted_up)

            # 将下轨移一位，前一日的下轨和前一日的收盘价都在当日了
            shifted_down = df_daily['DOWN'].shift(1)
            # 突破下轨，是向下突破，条件是前一日收盘价大于前一日下轨，当日收盘价小于当日下轨
            df_daily['down_mask'] = (last_close >= shifted_down) & (df_daily['close'] < shifted_down)

            # 对结果进行过滤，只保留向上突破或者向上突破的数据
            df_daily = df_daily[df_daily['up_mask'] | df_daily['down_mask']]

            # 从DataFrame中扔掉不用的数据
            df_daily.drop(['close', 'std', 'MB', 'UP', 'DOWN'], 1, inplace=True)

            # 将信号保存到数据库
            update_requests = []
            for date in df_daily.index:
                # 保存的数据包括股票代码、日期和信号类型，结合数据集的名字，就表示某只股票在某日
                doc = {
                    'code': code,
                    'date': date,
                    # 方向，向上突破 up，向下突破 down
                    'direction': 'up' if df_daily.loc[date]['up_mask'] else 'down'
                }
                update_requests.append(
                    UpdateOne(doc, {'$set': doc}, upsert=True))

            if len(update_requests) > 0:
                update_result = DB_CONN['boll'].bulk_write(update_requests, ordered=False)
                print('SAVE BOLL, 第%d个, 股票代码: %s, 插入: %4d, 更新: %4d' %
                      (index+1, code, update_result.upserted_count, update_result.modified_count),
                      flush=True)

        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()


def is_boll_break_up(code, date):
    """
    查询某只股票是否在某日出现了突破上轨信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现了突破上轨信号，False - 没有出现突破上轨信号
    """
    count = DB_CONN['boll'].count({'code': code, 'date': date, 'direction': 'up'})
    return count == 1


def is_boll_break_down(code, date):
    """
    查询某只股票是否在某日出现了突破下轨信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现了突破下轨信号，False - 没有出现突破下轨信号
    """
    count = DB_CONN['boll'].count({'code': code, 'date': date, 'direction': 'down'})
    return count == 1


if __name__ == '__main__':
    # 计算指定时间内的boll信号
    compute_boll('2019-05-01', '2019-07-26')