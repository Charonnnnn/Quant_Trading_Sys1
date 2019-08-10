from DataCrawler.database import DB_CONN
from DataCrawler.stock_util import get_all_codes

from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback

def compute_macd(start_date, end_date):
    """
    计算给定周期内的MACD金叉和死叉信号，把结果保存到数据库中
    :param start_date: 开始日期
    :param end_date: 结束日期
    """

    short_period = 12
    long_period = 26
    m_for_diff_period = 9

    codes = get_all_codes()

    # codes = ['000939'] # 002604
    for indexx,code in enumerate(codes):
        try:
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code':code, 'date':{'$gte':start_date,'$lte':end_date}},
                sort=[('date',ASCENDING)],
                projection ={'date':True,'close':True, '_id':True}
            )

            # 转成DataFrame
            # df_daily = DataFrame(list(daily_cursor))
            df_daily = DataFrame([daily for daily in daily_cursor])
            # 设置date为索引
            df_daily.set_index(['date'],inplace=True)
            print(df_daily)

            # 如果查询出的行情数量还不足以计算N天的平均值，则不再参与计算
            if df_daily.index.size < short_period :
                print('数据量不够： %s, 只有: %d' % (code,df_daily.index.size), flush=True)
                continue
            '''
            计算EMA
            alpha = 2/(N+1)
            EMA(i) = (1 - alpha) * EMA(i-1) + alpha * CLOSE(i)
                   = alpha * (CLOSE(i) - EMA(i-1)) + EMA(i-1)
            '''
            index = 0
            EMA1 = []             # 短时EMA列表
            EMA2 = []            # 长时EMA列表
            for date in df_daily.index:
                # 第一天EMA就是当日的close(收盘价)
                if index == 0:
                    EMA1.append(df_daily.loc[date]['close'])
                    EMA2.append(df_daily.loc[date]['close'])
                else:
                    EMA1.append(2/(short_period+1) * (df_daily.loc[date]['close'] - EMA1[index-1]) + EMA1[index-1])
                    EMA2.append(2/(long_period+1) * (df_daily.loc[date]['close'] - EMA2[index-1]) + EMA2[index-1])

                index += 1

            df_daily['EMA1'] = EMA1
            df_daily['EMA2'] = EMA2

            # 计算DIFF, 短时EMA - 长时EMA
            df_daily['DIFF'] = df_daily['EMA1'] - df_daily['EMA2']
            '''
            计算DEA，
            DIFF的EMA，
            计算公式是： EMA(DIFF，M)
            '''
            index = 0
            DEA = []             # DEA列表
            for date in df_daily.index:
                # 第一天EMA就是当日的close(收盘价)
                if index == 0:
                    DEA.append(df_daily.loc[date]['DIFF'])
                else:
                    DEA.append(2/(m_for_diff_period+1) * (df_daily.loc[date]['DIFF'] - DEA[index-1]) + DEA[index-1])

                index += 1
            df_daily['DEA'] = DEA

            # 计算DIFF和DEA的差值 ===> macd
            df_daily['delta'] = df_daily['DIFF'] - df_daily['DEA']
            # 将delta的移一位，那么前一天delta就变成了今天的pre_delta
            df_daily['pre_delta'] = df_daily['delta'].shift(1)
            # 金叉，DIFF上穿DEA，前一日DIFF在DEA下面，当日DIFF在DEA上面
            df_daily_gold = df_daily[(df_daily['pre_delta'] <= 0) & (df_daily['delta'] > 0)]
            # 死叉，DIFF下穿DEA，前一日DIFF在DEA上面，当日DIFF在DEA下面
            df_daily_dead = df_daily[(df_daily['pre_delta'] >= 0) & (df_daily['delta'] < 0)]

            # 保存结果到数据库
            update_requests = []
            for date in df_daily_gold.index:
                # 保存时以code和date为查询条件，做更新或者新建，所以对code和date建立索引
                # 通过signal字段表示金叉还是死叉，gold表示金叉
                update_requests.append(UpdateOne(
                    {'code':code,'date':date},
                    {'$set':{'code': code, 'date': date, 'signal': 'gold'}},
                    upsert = True
                ))

            for date in df_daily_dead.index:
                update_requests.append(UpdateOne(
                    {'code':code,'date':date},
                    {'$set':{'code': code, 'date': date, 'signal': 'dead'}},
                    upsert = True
                ))

            if len(update_requests) >0:
                update_result = DB_CONN['macd'].bulk_write(update_requests, ordered=False)
                print('Save MACD, 第%d个, 股票代码：%s, 插入：%4d, 更新：%4d' %
                      (indexx+1,code, update_result.upserted_count, update_result.modified_count), flush=True)
        except:
            print('错误发生： %s, 在取值日期范围内没有数据' % code, flush=True)
            traceback.print_exc()

def is_macd_gold(code,date):
    """
    判断某只股票在某个交易日是否出现MACD金叉信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 有金叉信号，False - 无金叉信号
    """
    count = DB_CONN['macd'].count({'code':code, 'date':date, 'signal':'gold'})
    return count == 1

def is_macd_dead(code, date):
    """
    判断某只股票在某个交易日是否出现MACD死叉信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 有死叉信号，False - 无死叉信号
    """
    count = DB_CONN['macd'].count({'code': code, 'date': date, 'signal': 'dead'})
    return count == 1

if __name__ == '__main__':
    compute_macd('2019-05-01','2019-07-26')