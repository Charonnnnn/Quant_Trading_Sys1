
from IndexComputing.annual_profit_coumputing import compute_annual_profit
import pandas as pd

# def compute_sharpe_ratio(net_values):
#     """
#     计算夏普比率
#     :param net_values: 净值列表
#     """
#
#     # 总交易日数
#     trading_days = len(net_values)
#     # 所有收益的DataFrame
#     profit_df = pd.DataFrame(columns={'profit'})
#     # 收益之后，初始化为第一天的收益
#     profit_df.loc[0] = {'profit': round((net_values[0] - 1) * 100, 2)}
#     # 计算每天的收益
#     for index in range(1, trading_days):
#         # 计算每日的收益变化
#         profit = (net_values[index] - net_values[index - 1]) / net_values[index - 1]
#         profit = round(profit * 100, 2)
#         profit_df.loc[index] = {'profit': profit}
#
#     # 计算当日收益标准差
#     profit_std = pow(profit_df.var()['profit'], 1 / 2)
#
#     # 年化收益
#     annual_profit = compute_annual_profit(trading_days, net_values[-1])
#
#     # 夏普比率
#     sharpe_ratio = (annual_profit - 4.75) / (profit_std * pow(245, 1 / 2))
#
#     return annual_profit, sharpe_ratio

def compute_sharpe_ratio(net_value, df_day_profit):
    """
    计算夏普比率
    :param net_value: 最后的净值
    :param df_day_profit: 单日的收益，profit：策略单日收益，hs300：沪深300的单日涨跌幅
    """

    # 总交易日数
    trading_days = df_day_profit.index.size

    # 计算单日收益标准差
    profit_std = round(df_day_profit['profit'].std(), 4)
    print(profit_std)

    # 年化收益
    annual_profit = compute_annual_profit(trading_days, net_value)

    # 夏普比率
    if profit_std != 0:
        sharpe_ratio = (annual_profit - 4.75) / (profit_std * pow(245, 1 / 2))
    else:
        sharpe_ratio = 0

    return annual_profit, sharpe_ratio

if __name__ == '__main__':
    pass