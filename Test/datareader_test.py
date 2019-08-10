from pandas_datareader import data
import matplotlib.pyplot as plt
import pandas as pd
# stock_code = input("美股直接输入股票代码如GOOG \n港股输入代码+对应股市，如腾讯：0700.hk \n国内股票需要区分上证和深证，股票代码后面加.ss或者.sz\n请输入你要查询的股票代码：")
# stock_code = "0700.hk"
# stock_code = "AAPL"
stock_code = "300481.sz"
start_date = "2014-11-01"
end_date = "2019-07-30"
stock_info = data.get_data_yahoo(stock_code, start_date, end_date)
# 展示前5行
print(stock_info.tail())
# print(stock_info.info())
#  保存为Excel文件和CSV文件
# stock_info.to_excel('%s.xlsx'%stock_code)
# stock_info.to_csv('%s.csv'%stock_code)
# 输出图表
# plt.plot(stock_info['Close'], 'g')
# plt.show()