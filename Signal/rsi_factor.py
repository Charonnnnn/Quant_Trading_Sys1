from pandas import DataFrame
from pymongo import ASCENDING, UpdateOne
import traceback

from DataCrawler.database import DB_CONN
from DataCrawler.stock_util import get_all_codes

def compute_rsi(start_date,end_date):
    """
    计算指定时间段内的RSI信号，并保存到数据库中
    :param start_date: 开始日期
    :param end_date: 结束日期
    """

    all_codes = get_all_codes()
    # all_codes = ['6001318']

    # 计算RSI
    N = 12

    for index,code in enumerate(all_codes):
        try:
            daily_cursor = DB_CONN['daily'].find({'code':code,'date':{'$gte':start_date,'$lte':end_date},'index':False},sort=[('date',ASCENDING)],projection={'date':True,'close':True,'_id':True})

            df_daily = DataFrame(list(daily_cursor))

            # 如果查询出的行情数量还不足以计算N天的平均值，则不再参与计算
            if df_daily.index.size < N:
                print('数据量不够： %s, 只有: %d' % (code,df_daily.index.size), flush=True)
                continue

            df_daily.set_index(['date'],inplace=True)
            # 将close移一位作为当日的pre_close
            df_daily['pre_close'] = df_daily['close'].shift(1)
            # 计算当日的涨跌幅：(close - pre_close) * 100 / pre_close
            df_daily['change_pct'] = (df_daily['close'] - df_daily['pre_close'])*100 / df_daily['pre_close']
            # 只保留上涨的日期的涨幅 (涨幅大于0)
            df_daily['up_pct'] = DataFrame({'up_pct': df_daily['change_pct'], 'zero': 0}).max(1)

            # 计算RSI mean(up_change, N) * 100 / mean(abs(change),N)
            df_daily['RSI'] = df_daily['up_pct'].rolling(N).mean() * 100 / abs(df_daily['change_pct']).rolling(N).mean()

            # 将RSI移一位作为当日的PREV_RSI
            df_daily['PREV_RSI'] = df_daily['RSI'].shift(1)

            # 超买，RSI下穿80，作为卖出信号
            df_daily_over_bought = df_daily[(df_daily['RSI'] < 80) & (df_daily['PREV_RSI'] >=80)]

            # 超卖，RSI上穿20，作为买入信号
            df_daily_over_sold = df_daily[(df_daily['RSI'] > 20) & (df_daily['PREV_RSI'] <= 20)]

            # 保存结果到数据库，要以code和date创建索引，db.rsi.createIndex({'code': 1, 'date': 1})
            update_requests = []
            # 超买数据，以code和date为key更新数据，signal为over_bought
            for date in df_daily_over_bought.index:
                update_requests.append(UpdateOne(
                    {'code':code,'date':date},
                    {'$set':{'code': code, 'date': date, 'signal': 'over_bought'}},upsert=True
                ))
            # 超卖数据，以code和date为key更新数据，signal为over_sold
            for date in df_daily_over_sold.index:
                update_requests.append(UpdateOne(
                    {'code':code,'date':date},
                    {'$set':{'code': code, 'date': date, 'signal': 'over_sold'}},upsert=True
                ))
            if len(update_requests) > 0:
                update_result = DB_CONN['rsi'].bulk_write(update_requests,ordered=False)
                print('Save RSI, 第%d个, 股票代码：%s, 插入：%4d, 更新：%4d' %
                      (index+1,code, update_result.upserted_count, update_result.modified_count), flush=True)
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()


def is_rsi_over_sold(code, date):
    """
    判断某只股票在某个交易日是出现了超卖信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现了超卖信号，False - 没有出现超卖信号
    """
    count = DB_CONN['rsi'].count({'code': code, 'date': date, 'signal': 'over_sold'})
    return count == 1


def is_rsi_over_bought(code, date):
    """
    判断某只股票在某个交易日是出现了超买信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现了超买信号，False - 没有出现超买信号
    """
    count = DB_CONN['rsi'].count({'code': code, 'date': date, 'signal': 'over_bought'})
    return count == 1


if __name__ == '__main__':
    compute_rsi('2019-05-01', '2019-07-26')
