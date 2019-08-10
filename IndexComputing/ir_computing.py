import pandas as pd

def compute_ir(df_day_profit):
    """
    计算信息率
    :param df_day_profit: 单日收益，profit - 策略收益 hs300 - 沪深300的
    :return: 信息率
    """
    # 计算单日的无风险收益率
    base_profit = 4.5 / 245

    df_extra_profit = pd.DataFrame(columns=['profit', 'hs300'])
    df_extra_profit['profit'] = df_day_profit['profit'] - base_profit
    df_extra_profit['hs300'] = df_day_profit['hs300'] - base_profit

    # 计算策略的单日收益和基准单日涨跌幅的协方差
    cov = df_extra_profit['profit'].cov(df_extra_profit['hs300'])
    # 计算策略收益和基准收益沪深300的方差
    var_profit = df_extra_profit['profit'].var()
    var_hs300 = df_extra_profit['hs300'].var()
    # 计算Beta
    beta = cov / var_hs300
    # 残差风险
    omega = pow((var_profit - pow(beta, 2) * var_hs300) * 245, 1/2)
    # Alpha
    alpha = (df_extra_profit['profit'].mean() - (beta * df_extra_profit['hs300'].mean())) * 245
    # 信息率
    if omega != 0:
        ir = round(alpha / omega)
    else:
        ir = 0

    print('cov：%10.4f，var_profit：%10.4f，var_hs300：%10.4f，beta：%10.4f，omega：%10.4f，alpha：%10.4f，ir：%10.4f' %
          (cov, var_profit, var_hs300, beta, omega, alpha, ir), flush=True)

    return ir


if __name__ == "__main__":
    pass