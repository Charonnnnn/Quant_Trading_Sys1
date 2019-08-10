from datetime import datetime, timedelta

from pymongo import UpdateOne

from DataCrawler.database import DB_CONN,TU_PRO
from DataCrawler.stock_util import get_trading_dates

import tushare as ts
import traceback
import json

def crawl_basic(start_date=None, end_date=None):
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # 2019-07-30
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    # print(start_date,end_date)

    all_dates = get_trading_dates(start_date,end_date)
    # print(all_dates)
    for date in all_dates:
        try:
            print(date)
            crawl_basic_at_date(date)
        except:
            print('抓取股票基本信息时出错，日期：%s' % date, flush=True)
            traceback.print_exc()

def crawl_basic_at_date(date):
    df_basics = ts.get_stock_basics(date)  # 目前只能提供2016-08-09之后的历史数据

    # 如果当日没有基础信息，在不做操作
    if df_basics is None:
        return
    DB_CONN['basic'].insert_many(json.loads(df_basics.to_json(orient='records')))

    # df_basics['timeToMarket'].apply(lambda x: datetime.strptime(str(x),'%Y%m%d').strftime('%Y-%m-%d'))

    # update_requests = []
    # # 获取所有股票代码集合
    # codes = set(df_basics.index)
    # print(type(codes),len(codes))
    # for code in codes:
    #     doc = dict(df_basics.loc[code])
    #     try:
    #         # 将上市日期，20180101转换为2018-01-01的形式
    #         time_to_market = datetime.strptime(str(doc['timeToMarket']),'%Y%m%d').strftime('%Y-%m-%d')
    #
    #         # 将总股本和流通股本转为数字 numpy.float64 => float
    #         totals = float(doc['totals'])
    #         outstanding = float(doc['outstanding'])
    #
    #         # 更新字典
    #         doc.update({'code':code, 'date':date, 'timeToMarket':time_to_market, 'totals':totals, 'outstanding':outstanding })
    #
    #         update_requests.append(UpdateOne({'code': code, 'date': date},{'$set': doc}, upsert=True))  # upsert - 如果没有记录就insert
    #     except Exception as err:
    #         print('发生异常%s，股票代码：%s，日期：%s' % (err,code, date), flush=True)
    #         print(doc, flush=True)
    #
    # # 如果抓到了数据
    # if len(update_requests) > 0:
    #     update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)
    #
    #     print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %(date, update_result.upserted_count, update_result.modified_count), flush=True)

if __name__ == '__main__':
    # d = ts.get_stock_basics('2019-08-02')
    # print(d.columns)
    # print(d.shape)
    # print(d['timeToMarket'])
    # d['timeToMarket'].map(lambda x: datetime.strptime(str(x),'%Y%m%d').strftime('%Y-%m-%d'))
    # print(d['timeToMarket'])
    # print(d[['timeToMarket', 'totals', 'outstanding','esp','pb','pe']])
    # print(d.totals[0],type(d.totals[0]),float(d.totals[0]))

    # df = TU_PRO.daily_basic(ts_code='', trade_date='20190802', fields='ts_code,total_share,float_share,pe,pb')
    # print(df.columns)
    # print(df.shape)
    # print(df[df['total_share'].isnull()])


    crawl_basic('2019-01-01', '2019-08-03')
