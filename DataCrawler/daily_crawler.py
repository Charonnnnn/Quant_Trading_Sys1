
from pymongo import UpdateOne
import json
import tushare as ts
from datetime import datetime
import traceback

from DataCrawler.database import DB_CONN

class DailyCrawler():
    def __init__(self):
        # 创建数据集
        self.daily = DB_CONN['daily']
        self.daily_hfq = DB_CONN['daily_hfq']

    def crawl_index(self,start_date,end_date):
        '''
        抓取指数的日K数据。
        指数行情的主要作用：
        1. 用来生成交易日历
        2. 回测时做为收益的对比基准
        :param start_day: 开始日期
        :param end_date: 结束日期
        :return:
        '''
        # 上证（000001）、沪深300（000300、深成（399001））、中小板（399005），创业板（399006）
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        now = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = now
        if end_date is None:
            end_date = now

        for index,code in enumerate(index_codes):
            # index=True时，接口会自动匹配指数代码, (即 是指数时 才需要index=True)
            # 例如，要获取上证综指行情，调用方法为：ts.get_k_data('000001', index=True)
            df_daily = ts.get_k_data(code,index=True,start=start_date,end=end_date)
            # self.save_data(code, df_daily, self.daily, {'index': True})
            df_daily['index'] = True
            res = DB_CONN['daily'].insert_many(json.loads(df_daily.to_json(orient='records')))
            print('保存日线指数, 第%d个, 代码： %s, 插入：%4d条' %(index+1,code,len(res.inserted_ids)),flush=True)


    def crawl(self, start_date=None, end_date=None):
        """
        抓取股票的日K数据，主要包含了不复权和后复权两种

        :param begin_date: 开始日期
        :param end_date: 结束日期
        """
        # 获取所有股票的基本信息
        stock_df = ts.get_stock_basics()
        # 将基本信息的索引列表转化为股票代码列表
        codes = list(stock_df.index)

        now = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = now
        if end_date is None:
            end_date = now

        for index,code in enumerate(codes):
            try:
                # 抓取不复权的价格
                # qfq-前复权 hfq-后复权 None-不复权，默认为qfq
                df_daily = ts.get_k_data(code, autype=None, start=start_date, end=end_date)
                # self.save_data(code, df_daily, self.daily, {'index': False})
                df_daily['index'] = False
                res = DB_CONN['daily'].insert_many(json.loads(df_daily.to_json(orient='records')))
                print('保存日线数据, 第%d个, 代码： %s, 插入：%4d条' %(index+1,code,len(res.inserted_ids)),flush=True)

                # 抓取后复权的价格
                df_daily_hfq = ts.get_k_data(code, autype='hfq', start=start_date, end=end_date)
                # self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False})
                df_daily_hfq['index'] = False
                res = DB_CONN['daily_hfq'].insert_many(json.loads(df_daily_hfq.to_json(orient='records')))
                print('保存日线后复权数据, 第%d个, 代码： %s, 插入：%4d条' %(index+1,code,len(res.inserted_ids)),flush=True)

            except Exception as err:
                traceback.print_exc()
                print('代码: %s 插入发生错误 %s, 可能由于该股票与该段时间内没有数据(停牌或其他原因...)'%(code,err))

    def save_data(self,code, df_daily, collection, extra_fields=None):
        '''
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        :return:
        '''
        update_requests = []

        for df_index in df_daily.index:
            doc = dict(df_daily.loc[df_index])
            doc['code'] = code

            # 如果指定了其他字段，则更新dict
            if extra_fields is not None:
                doc.update(extra_fields)

            # 生成一条数据库的更新请求
            # 注意：
            # 需要在code、date、index三个字段上增加索引，否则随着数据量的增加，
            # 写入速度会变慢，创建索引的命令式：
            # db.daily.createIndex({'code':1,'date':1,'index':1})
            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )

        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)

if __name__ == '__main__':
    # data = ts.get_k_data('000300',index=True,start='2019-07-01',end='2019-07-30')
    # print(data.columns, data.shape, data)
    dc = DailyCrawler()
    dc.crawl_index('2019-01-01', '2019-08-03')   # 抓取当日指数
    dc.crawl('2019-01-01', '2019-08-03')         # 抓取当日K线
