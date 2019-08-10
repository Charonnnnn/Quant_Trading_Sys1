from pymongo import MongoClient
import tushare as ts


# 指定数据库的连接，quant_01是数据库名
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_02']

TU_PRO = ts.pro_api('e8b28f689e178619effb5b72aff3aaed966d087a65084b4688e63fe7')