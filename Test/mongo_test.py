from pymongo import MongoClient,UpdateOne,ASCENDING
import json
import tushare as ts
import pandas as pd

def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo. """
    if username and password:
        mongo_uri = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_test']

df = ts.get_k_data('600848',start='2016-12-22',end='2019-07-30')
# print(df)
# res = DB_CONN['daily'].update_many({'code':'600848'},{'$set':json.loads(df.to_json(orient='columns'))},upsert=True)
# res = DB_CONN['daily'].update(json.loads(df.to_json(orient='records')),upsert=True)
# print(res.matched_count,res)

# res = DB_CONN['daily'].insert_many(json.loads(df.to_json(orient='records')))
# print(len(res.inserted_ids))


daily_cursor = DB_CONN['daily'].find(
        {'code': '600848', 'date': {'$lte': '2019-07-22', '$gte': '2019-07-01'}},
        sort=[('date', ASCENDING)],
        # projection={'date': True, 'close': True}
    )

# dff2 = pd.DataFrame(list(daily_cursor))
# print(dff2)

update_requests = []
for daily in daily_cursor:
    date = daily['date']
    try:
        close = daily['close']
        volume = daily['volume']
        high = daily['high']
        doc = dict()

        au_factor = round(volume / close, 2)
        doc['au_factor'] = au_factor

        pre_close = volume / high / au_factor
        doc['pre_close'] = round(pre_close, 2)


        update_requests.append(
            UpdateOne(
                {'code': '600848', 'date': date},
                {'$set': doc}))
    except Exception as err:
        print('计算复权因子时发生错误%s，股票代码：%s，日期：%s' % (err,'600848', date), flush=True)
        # 恢复成初始值，防止用错
        last_close = -1
        last_au_factor = -1

if len(update_requests) > 0:
    update_result = DB_CONN['daily'].bulk_write(update_requests, ordered=False)
    print('填充复权因子和前收，股票：%s，更新：%4d条' %
          ('600848', update_result.modified_count), flush=True)





# cursor = DB_CONN['daily'].find({'code':'600848'})
# print(type(cursor),cursor)

# dff = pd.DataFrame(list(cursor))
# print(dff)

# dff.to_csv("abc.csv", encoding="utf_8_sig")  # 处理中文乱码问题

# for i in cursor:
#     print(i['date'])