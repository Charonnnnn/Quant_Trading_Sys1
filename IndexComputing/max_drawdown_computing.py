

def compute_drawdown(net_values):
    """
    计算最大回撤
    :param net_values: 净值列表
    """
    # 最大回撤初始值设为0
    max_drawdown = 0
    # size = len(net_values)
    index = 0
    # 双层循环找出最大回撤
    for net_value in net_values:
        # 计算从当前开始直到结束，和当前净值相比的最大回撤
        for sub_net_value in net_values[index:]:
            # 计算回撤
            drawdown = 1 - sub_net_value / net_value
            # 如果当前的回撤大于已经计算的最大回撤，则当前回撤作为最大回撤
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        index += 1

    return max_drawdown

if __name__ == '__main__':
    pass