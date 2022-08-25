from typing import List
from datetime import datetime
import pathlib
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.transforms as transforms
from src.backtest.ftx_data_types import CombinedModelHedgeType, HedgeTrade, HedgeType, LogState, CombinedModelHedgeTrade


def save_summary(logs: List[LogState], save_path: str) -> dict:
    path = pathlib.Path(save_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    profit = float(logs[-1].profit)
    index = []
    net_deposit = []
    for log in logs:
        index.append(datetime.fromtimestamp(log.timestamp))
        net_deposit.append(log.net_deposit)
    net_deposit_seires = pd.Series(net_deposit, index)
    avg_deposit = net_deposit_seires.resample('1s').last().fillna(method='ffill').mean()
    roi = profit / avg_deposit
    days = (logs[-1].timestamp - logs[0].timestamp) / 86400
    apr = roi / days * 365
    summary = {
        'avg_deposit': avg_deposit,
        'profit': profit,
        'roi': roi,
        'apr': apr}
    with path.open('w') as fp:
        json.dump(summary, fp, indent=2)
    print(f"Save summary to {save_path}")
    return summary


def plot_logs(logs: List[LogState], hedge_trades: List[HedgeTrade], save_path: str, to_show: bool = True):
    path = pathlib.Path(save_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    index = []
    basis = []
    position = []
    net_deposit = []
    profit = []
    for log in logs:
        index.append(datetime.fromtimestamp(log.timestamp))
        basis.append(log.basis)
        position.append(log.spot_position)
        net_deposit.append(log.net_deposit)
        profit.append(log.profit)

    trade_open_index = []
    trade_close_index = []
    trade_open = []
    trade_close = []
    for trade in hedge_trades:
        if trade.hedge_type == HedgeType.OPEN:
            trade_open_index.append(datetime.fromtimestamp(trade.timestamp))
            trade_open.append(trade.basis)
        else:
            trade_close_index.append(datetime.fromtimestamp(trade.timestamp))
            trade_close.append(trade.basis)

    fig, ax = plt.subplots(nrows=3, ncols=1, sharex=True, figsize=(12, 10))

    basis_ax: plt.Axes = ax[0]
    position_ax: plt.Axes = ax[1]
    profit_ax: plt.Axes = ax[2]

    basis_ax.plot(index, basis, alpha=0.8, lw=1)
    offset = lambda p: transforms.ScaledTranslation(0, p/72., fig.dpi_scale_trans)
    trans = basis_ax.transData
    basis_ax.scatter(trade_open_index, trade_open, c='r', marker='v', s=9, transform=trans + offset(5))
    basis_ax.scatter(trade_close_index, trade_close, c='g', marker='^', s=9, transform=trans + offset(-5))
    basis_ax.set_ylabel('basis')

    position_ax.plot(index, position)
    position_ax.set_ylabel('position')

    profit_ax.plot(index, profit)
    profit_ax.set_ylabel('profit')

    if to_show:
        plt.show()

    fig.savefig(save_path)
    print(f"Save plot to {save_path}")
    plt.close(fig)


def plot_combined_model_logs(logs: List[LogState], hedge_trades: List[CombinedModelHedgeTrade], save_path: str, to_show: bool = True):
    path = pathlib.Path(save_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    index = []
    basis = []
    position = []
    net_deposit = []
    profit = []
    for log in logs:
        index.append(datetime.fromtimestamp(log.timestamp))
        basis.append(log.basis)
        position.append(log.spot_position)
        net_deposit.append(log.net_deposit)
        profit.append(log.profit)

    both_open_index = []
    both_open = []
    m1_open_index = []
    m1_open = []
    m2_open_index = []
    m2_open = []
    both_close_index = []
    both_close = []
    m1_close_index = []
    m1_close = []
    m2_close_index = []
    m2_close = []
    one_open_the_other_close_index = []
    one_open_the_other_close = []

    for trade in hedge_trades:
        if trade.hedge_type == CombinedModelHedgeType.BOTH_OPEN:
            both_open_index.append(datetime.fromtimestamp(trade.timestamp))
            both_open.append(trade.basis)
        elif trade.hedge_type == CombinedModelHedgeType.MODEL1_OPEN:
            m1_open_index.append(datetime.fromtimestamp(trade.timestamp))
            m1_open.append(trade.basis)
        elif trade.hedge_type == CombinedModelHedgeType.MODEL2_OPEN:
            m2_open_index.append(datetime.fromtimestamp(trade.timestamp))
            m2_open.append(trade.basis)
        elif trade.hedge_type == CombinedModelHedgeType.BOTH_CLOSE:
            both_close_index.append(datetime.fromtimestamp(trade.timestamp))
            both_close.append(trade.basis)
        elif trade.hedge_type == CombinedModelHedgeType.MODEL1_CLOSE:
            m1_close_index.append(datetime.fromtimestamp(trade.timestamp))
            m1_close.append(trade.basis)
        elif trade.hedge_type == CombinedModelHedgeType.MODEL2_CLOSE:
            m2_close_index.append(datetime.fromtimestamp(trade.timestamp))
            m2_close.append(trade.basis)
        elif trade.hedge_type == CombinedModelHedgeType.ONE_OPEN_THE_OTHER_CLOSE:
            one_open_the_other_close_index.append(datetime.fromtimestamp(trade.timestamp))
            one_open_the_other_close.append(trade.basis)

    fig, ax = plt.subplots(nrows=3, ncols=1, sharex=True, figsize=(12, 10))

    basis_ax: plt.Axes = ax[0]
    position_ax: plt.Axes = ax[1]
    profit_ax: plt.Axes = ax[2]

    basis_ax.plot(index, basis, alpha=0.8, lw=1)
    offset = lambda p: transforms.ScaledTranslation(0, p/72., fig.dpi_scale_trans)
    trans = basis_ax.transData
    basis_ax.scatter(both_open_index, both_open, c='darkgreen', marker='v', s=9, transform=trans + offset(5), label='both open')
    basis_ax.scatter(m1_open_index, m1_open, c='cyan', marker='v', s=9, transform=trans + offset(5), label='m1 open')
    basis_ax.scatter(m2_open_index, m2_open, c='dodgerblue', marker='v', s=9, transform=trans + offset(5), label='m2 open')
    basis_ax.scatter(both_close_index, both_close, c='b', marker='^', s=9, transform=trans + offset(-5), label='both close')
    basis_ax.scatter(m1_close_index, m1_close, c='orangered', marker='^', s=9, transform=trans + offset(-5), label='m1 close')
    basis_ax.scatter(m2_close_index, m2_close, c='magenta', marker='^', s=9, transform=trans + offset(-5), label='m2 close')
    basis_ax.scatter(one_open_the_other_close_index, one_open_the_other_close, c='b', marker='X', s=18, label='bad')
    basis_ax.set_ylabel('basis')
    basis_ax.legend()

    position_ax.plot(index, position)
    position_ax.set_ylabel('position')

    profit_ax.plot(index, profit)
    profit_ax.set_ylabel('profit')

    if to_show:
        plt.show()

    fig.savefig(save_path)
    print(f"Save plot to {save_path}")
    plt.close(fig)
