

def compute_annual_profit(trading_days, net_value):
    """
    计算年化收益
    """

    annual_profit = 0
    # 交易日数大于0，才计算年化收益
    if trading_days > 0:
        # 计算年数
        years = trading_days / 245
        # 计算年化收益
        annual_profit = pow(net_value, 1 / years) - 1

    # 将年化收益转化为百分数，保留两位小数
    annual_profit = round(annual_profit * 100, 2)

    return annual_profit

if __name__ == '__main__':
    pass