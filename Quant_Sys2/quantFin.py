#TODO: 停牌相关问题, 数据本地化, 指标计算, 手续费

import pandas as pd 
import matplotlib.pyplot as plt 
import tushare as ts 
import datetime
import numpy as np 

trade_cal = ts.trade_cal()
'''
trade_cal
      calendarDate  isOpen
0       1990-12-19       1
1       1990-12-20       1
2       1990-12-21       1
3       1990-12-22       0
4       1990-12-23       0
5       1990-12-24       1
....
'''

class G:
	pass

class Context:
	def __init__(self,cash,start_date,end_date):
		self.cash = cash
		self.start_date = start_date
		self.end_date = end_date
		self.positions = {}  # '601318':100
		self.benchmark = None
		self.date_range = trade_cal[(trade_cal.calendarDate >= start_date) & (trade_cal.calendarDate <= end_date) & (trade_cal.isOpen == 1)]['calendarDate'].values

g = G()
cash = 100000.0
s_date = '2016-01-01'
e_date = '2019-07-26'

context = Context(cash,s_date,e_date)

def attribute_history(security,count, fields=('open','close','high','low','volume')):
	end_date = (context.dt - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	start_date = trade_cal[(trade_cal.calendarDate <= end_date) & (trade_cal.isOpen == 1)][-count:].iloc[0]['calendarDate']
	# print('!!!!',security,start_date,end_date)
	return attribute_daterange_history(security,start_date,end_date,fields)
	# df = ts.get_hist_data(security, start_date,end_date)
	# return df[list(fields)].sort_index()

def attribute_daterange_history(security,start_date,end_date,fields=('open','close','high','low','volume')):
	try:
		f = open(security+'.csv','r')
		# print(security)
		df = pd.read_csv(f, index_col='date', parse_dates=['date']).sort_index().loc[start_date:end_date]
	except:
		df = ts.get_hist_data(security,start_date,end_date)
	return df[list(fields)].sort_index()


def get_today_data(security):
	today = context.dt.strftime('%Y-%m-%d')
	try:
		f = open(security+'.csv', 'r')
		# print(security)
		df = pd.read_csv(f, index_col='date', parse_dates=['date']).loc[today:today]
	except FileNotFoundError:
		df = ts.get_hist_data(security,today,today)
	return df

def set_benchmark(benchmark):
	context.benchmark = benchmark

def _order(today_data,security,amount):
	if today_data.empty:
		print('股票今日停牌, 无法买卖')
		return False

	if security not in context.positions:
		context.positions[security] = 0

	if context.cash - amount * today_data['close'][0] * 1.0003 < 0:
		amount = int(context.cash / today_data['close'][0] / 1.0003)
		print('现金不足, 已调整为%d'%amount)

	if amount % 100 != 0:
		if amount != -context.positions[security]:
			old_amount = amount
			amount = int(amount / 100) * 100
			print('交易必须为100的整数倍, 已由%d调整为%d'%(old_amount,amount))

	# TODO: 成交量不会超过当天成交量

	if context.positions[security] + amount < 0:
		amount = -context.positions[security]
		print('卖出股票必须不超过持仓数, 已调整为%d'%amount)

	action = '买入' if amount > 0 else '卖出'
	print('%s: %s%s股票%d股, 价格%.2f' %(context.dt.strftime('%Y-%m-%d'), action, security, amount, today_data['close'][0]))

	new_amount = context.positions[security] +amount
	context.positions[security] = new_amount
	if amount > 0:
		context.cash -= amount * today_data['close'][0] * 1.0003
	else:
		context.cash -= amount * today_data['close'][0] * 0.9987

	if context.positions[security] == 0:
		del context.positions[security]


def order(security,amount):  # 买多少股票  (按股数下单)
	# today = context.dt.strftime('%Y-%m-%d')
	# today_data = ts.get_hist_data(security,start_date,end_date)
	today_data = get_today_data(security)
	return _order(today_data, security, amount)
	'''
#买入平安银行股票100股
order('000001.XSHE', 100) # 下一个市价单
	'''

def order_value(security,value):  # 买多少钱的股票  (按价值下单)
	# today = context.dt.strftime('%Y-%m-%d')
	# today_data = ts.get_hist_data(security,start_date,end_date)
	today_data = get_today_data(security)
	commission_ratio = 0.9987 if value < 0 else 1.0003
	amount = int(value / today_data['close'][0] / commission_ratio)
	return _order(today_data, security, amount)
	'''
#卖出价值为10000元的平安银行股票
order_value('000001.XSHE', -10000)
#买入价值为10000元的平安银行股票
order_value('000001.XSHE', 10000)
	'''

def order_target(security,amount):  # 买到多少股  (目标股数下单) 
	# today = context.dt.strftime('%Y-%m-%d')
	# today_data = ts.get_hist_data(security,start_date,end_date)
	if amount < 0:
		print('目标股数不能为负, 已调整为0')
		amount = 0
	today_data = get_today_data(security)
	hold_amount = context.positions[security] if security in context.positions else 0
	delta_amount = amount - hold_amount
	return _order(today_data,security,delta_amount)

	'''
# 卖出平安银行所有股票
order_target('000001.XSHE', 0)
# 买入平安银行所有股票到100股
order_target('000001.XSHE', 100)
	'''

def order_target_value(security,value):  # 买到多少钱的股票  (目标价值下单)
	if value < 0:
		print('目标价值不能为负, 已调整为0')
		value = 0
	# today_data = ts.get_hist_data(security,start_date,end_date)
	today_data = get_today_data(security)
	hold_value= context.positions[security] * today_data['close'][0]
	delta_value = value - hold_value
	commission_ratio = 0.9987 if delta_value < 0 else 1.0003
	delta_amount = int(delta_value / today_data['close'][0] / commission_ratio)
	return _order(today_data,security,delta_amount)
	'''
#卖出平安银行所有股票
order_target_value('000001.XSHE', 0)
#调整平安银行股票仓位到10000元价值
order_target_value('000001.XSHE', 10000)
	'''

def run(context):
	init_value = cash
	plt_df = pd.DataFrame(index=pd.to_datetime(context.date_range),columns=['value'])
	last_prize = {}  # 为停牌用

	for dt in context.date_range:
		context.dt = datetime.datetime.strptime(dt,'%Y-%m-%d')
		handle_data(context)  # 用户策略
		value = context.cash
		for stock in context.positions:
			today_data = get_today_data(stock)
			if not today_data.empty:
				# 股票未停牌
				prize = today_data['close'][0]
				last_prize[stock] = prize
			else:
				# 股票停牌
				print('%s: 股票%s今日停牌' %(context.dt.strftime('%Y-%m-%d'),stock))
				prize = last_prize[stock]
			value += prize * context.positions[stock]
		plt_df.loc[dt,'value'] = value
	plt_df['ratio'] = (plt_df['value'] - init_value) / init_value

	# 计算基准
	if context.benchmark:
		# print('BENCHMARK?',context.benchmark, context.start_date, context.end_date)
		benchmark_df = attribute_daterange_history(context.benchmark, context.start_date, context.end_date)
		# print(benchmark_df.head(10))
		benchmark_init = benchmark_df['close'][0]
		plt_df['benchmark_ratio'] = (benchmark_df['close'] - benchmark_init) / benchmark_init

	# 绘图
	plt_df[['ratio','benchmark_ratio']].plot()
	plt.show()


'''
用户写的函数 -- 初始化 和 策略
'''
def initialize(context):
	g.security = ['601318']
	g.p1 = 5
	g.p2 = 60
	set_benchmark('601318')

def handle_data(context):   # 双均值策略
	n = len(g.security)
	for stock in g.security:
		hist = attribute_history(stock, count=g.p2)
		ma5 = hist['close'][-5:].mean()
		ma60 = hist['close'].mean()

		if ma5 > ma60 and stock not in context.positions:
			order_value(stock,context.cash / n)
		elif ma5 < ma60 and stock in context.positions:
			order_target(stock,0)

if __name__ == '__main__':
	initialize(context)
	run(context)




