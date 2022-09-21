import json
from decimal import Decimal
from statistics import quantiles
from typing import TypedDict


class BacktestResult(TypedDict):
    avg_deposit: float
    profit: float
    roi: float
    apr: float
    index: int
    params: dict


def _read_from_file(file_path) -> list[BacktestResult]:
    with open(file_path) as json_file:
        data = json.load(json_file)
        return data


def evaluate(backtest_result: BacktestResult) -> float:
    if backtest_result["apr"] < 0.08:
        return 0
    return backtest_result["avg_deposit"]


def smart_choose(
    backtest_results: list[BacktestResult], sorted_key="apr"
) -> BacktestResult:
    average_deposit_list = []
    profit_list = []
    apr_list = []
    n = 6
    m = 3
    min_apr = 0.08
    for backtest_result in backtest_results:
        average_deposit_list.append(backtest_result["avg_deposit"])
        profit_list.append(backtest_result["profit"])
        apr_list.append(backtest_result["apr"])
    average_deposit_quantiles = [
        round(q, 1) for q in quantiles(average_deposit_list, n=n + 1)
    ]
    profit_quantiles = [round(q, 1) for q in quantiles(profit_list, n=n + 1)]

    filtered_results = []
    for backtest_result in backtest_results:
        if backtest_result["apr"] < min_apr:
            continue
        if backtest_result["avg_deposit"] < average_deposit_quantiles[m - 1]:
            continue
        if backtest_result["profit"] < profit_quantiles[m - 1]:
            continue
        filtered_results.append(backtest_result)

    return sorted(filtered_results, key=lambda d: d[sorted_key], reverse=True)


def get_highest_score_result(
    file_path_list: list[str], sorted_key="apr"
) -> BacktestResult:
    params_results = []
    for file_path in file_path_list:
        filtered_results = smart_choose(
            _read_from_file(file_path)["results"], sorted_key=sorted_key
        )
        filtered_params = [
            filtered_result["params"] for filtered_result in filtered_results
        ]
        if not params_results:
            params_results = filtered_params
        else:
            params_results = [
                value for value in params_results if value in filtered_params
            ]

    if len(params_results) == 0:
        raise ValueError("Filter condition too strict.")

    final_param = params_results[0]
    for file_path in file_path_list:
        data = _read_from_file(file_path)
        for result in data["results"]:
            if result["params"] == final_param:
                print(
                    f'{data["future"]} from {data["from_date"]} to {data["to_date"]}, index={result["index"]}'
                )
    return final_param


def _sum_positions(position_dict):
    return_amount = Decimal("0")
    for position in position_dict["logs"]:
        return_amount += Decimal(position["positions"])
    return return_amount


def ensemble(file_path_list: list[str]):
    """
    currently support 2 indicator ensemble only
    TODO: add filter low apr position records
    """
    assert len(file_path_list) == 2
    indicator_positions_list = []
    for file_path in file_path_list:
        summary_results = _read_from_file(f"{file_path}/summary.json")["results"]
        positions = []
        for summary_result in summary_results:
            index = summary_result["index"]
            position_file_path = f"{file_path}/positions/position_{str(index)}.json"
            positions.append(_read_from_file(position_file_path))
        indicator_positions_list.append(positions)

    max_positions = 0
    max_position_combanation = ()
    for positions_1 in indicator_positions_list[0]:
        for positions_2 in indicator_positions_list[1]:
            assert len(positions_1) == len(positions_2)
            sum_positions = Decimal("0")
            for i, _ in enumerate(positions_1["logs"]):
                sum_positions += max(
                    Decimal(positions_1["logs"][i]["positions"]),
                    Decimal(positions_2["logs"][i]["positions"]),
                )
            if sum_positions > max_positions:
                max_position_combanation = (
                    positions_1,
                    positions_2,
                )
                max_positions = sum_positions
    print(
        f'max_position_combanation: {max_position_combanation[0]["params"]}, {max_position_combanation[1]["params"]}'
    )
    max_one_positions = max(
        _sum_positions(max_position_combanation[0]),
        _sum_positions(max_position_combanation[1]),
    )
    improve_percent = (max_positions - max_one_positions) / max_one_positions * 100
    print(f"improve_percent: {improve_percent}%")


def main():
    # file_path = "local/backtest/bollinger_BTC_20220326_20220624/summary.json"
    # file_path = "local/backtest/macd_BTC_20220326_20220624/summary.json"
    # final_result = get_highest_score_result(file_path)
    # print(f"final result: {final_result}")
    # ensemble(file_path_list)

    # [bollinger, macd, macd_boll, rsi]
    strategy_name = "macd_boll"
    coin = "BTC"

    file_path_list = [
        "local/backtest/"
        + strategy_name
        + "_"
        + coin
        + "-0325_20220101_20220325/summary.json",
        "local/backtest/"
        + strategy_name
        + "_"
        + coin
        + "-0624_20220326_20220624/summary.json",
        "local/backtest/"
        + strategy_name
        + "_"
        + coin
        + "-0930_20220625_20220905/summary.json",
    ]

    far_future_file_path_list = [
        "local/backtest/"
        + strategy_name
        + "_"
        + coin
        + "-0930_20220326_20220624/summary.json",
        "local/backtest/"
        + strategy_name
        + "_"
        + coin
        + "-1230_20220625_20220905/summary.json",
    ]

    # sorted_key: 'avg_deposit' or 'apr'
    print(get_highest_score_result(file_path_list, sorted_key="avg_deposit"))
    print(get_highest_score_result(far_future_file_path_list, sorted_key="avg_deposit"))


if __name__ == "__main__":
    main()
