import json
import urllib3

from pymongo import UpdateOne

from DataCrawler.database import DB_CONN
from DataCrawler.stock_util import get_all_codes

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'

'''
http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?
type=YJBB21_YJBB&token=70f12f2f4f091e459a279469fe49eca5&filter=(scode=600691)&st=reportdate&sr=1&p=1&ps=50&js=var%20QfXnaZJs={pages:(tp),data:%20(x),font:(font)}&rt=52153055'''

def crawl_finance_report():
    # 先获取所有的股票列表
    codes = get_all_codes()

    # 创建连接池
    conn_pool = urllib3.PoolManager()

    # 抓取的财务地址，scode为股票代码 - http://data.eastmoney.com/bbsj/yjbb/600691.html
    url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?' \
          'type=YJBB21_YJBB&token=70f12f2f4f091e459a279469fe49eca5&st=reportdate&sr=-1' \
          '&filter=(scode={0})&p={page}&ps={pageSize}&js={"pages":(tp),"data":%20(x)}'

    response = conn_pool.request('GET', url.replace('{0}', '600691'))
    result = json.loads(response.data.decode('UTF-8'))
    reports = result['data']
    # TODO: 字体反爬处理 - http://fontstore.baidu.com/static/editor/index.html#
    # https://www.cnblogs.com/TM0831/p/10078372.html
    # https://www.jianshu.com/p/ebd73b026ccf
    # https://blog.csdn.net/qq_41733098/article/details/88959897
    # https://www.cnblogs.com/songzhixue/articles/11242696.html
    # https: // cloud.tencent.com / developer / article / 1386548
    print(reports)
    doc = {}
    for report in reports:
        doc = {
            # 报告期
            'report_date': report['reportdate'][0:10],
            # 公告日期
            'announced_date': report['latestnoticedate'][0:10],
            # 每股收益
            'eps': report['basiceps'],
            'code': '600691'
        }
    print(doc)

    # 循环抓取所有股票的财务信息
    # for code in codes:
    #     # 替换股票代码，抓取该只股票的财务数据
    #     response = conn_pool.request('GET', url.replace('{0}', code))
    #
    #     # 解析抓取结果
    #     result = json.loads(response.data.decode('UTF-8'))
    #
    #     # 取出数据
    #     reports = result['data']
    #
    #     # 更新数据库的请求列表
    #     update_requests = []
    #     # 循环处理所有报告数据
    #     for report in reports:
    #         doc = {
    #             # 报告期
    #             'report_date': report['reportdate'][0:10],
    #             # 公告日期
    #             'announced_date': report['latestnoticedate'][0:10],
    #             # 每股收益
    #             'eps': report['basiceps'],
    #             'code': code
    #         }
    #
    #         # 将更新请求添加到列表中，更新时的查询条件为code、report_date，为了快速保存数据，需要增加索引
    #         # db.finance_report.createIndex({'code':1, 'report_date':1})
    #         update_requests.append(
    #             UpdateOne(
    #                 {'code': code, 'report_date': doc['report_date']},
    #                 # upsert=True保证了如果查不到数据，则插入一条新数据
    #                 {'$set': doc}, upsert=True))
    #
    #     # 如果更新数据的请求列表不为空，则写入数据库
    #     if len(update_requests) > 0:
    #         # 采用批量写入的方式，加快保存速度
    #         update_result = DB_CONN['finance_report'].bulk_write(update_requests, ordered=False)
    #         print('股票 %s, 财报，更新 %d, 插入 %d' %
    #               (code, update_result.modified_count, update_result.upserted_count))


# def crawl_single_page(page):
#     """
#     抓取单页数据
#     """
#     url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&js=%7B%22data%22%3A%5B(x)%5D%2C%22recordsTotal%22%3A(tot)%2C%22recordsFiltered%22%3A(tot)%7D&cmd=C._A&sty=FCOIATC&st=(ChangePercent)&sr=-1&p={0}&ps=200'
#         # url = 'http://www.baidu.com'
#     try:
#         # 创建连接池
#         conn_pool = urllib3.PoolManager()
#         response = conn_pool.request('GET', url.replace('{0}', str(page)), headers={'User-Agent': user_agent})
#         return response.data.decode('UTF-8')
#     except:
#         # traceback.print_exc()
#         return None



if __name__ == "__main__":
    crawl_finance_report()
    # da = crawl_single_page(1)
    # print(da)