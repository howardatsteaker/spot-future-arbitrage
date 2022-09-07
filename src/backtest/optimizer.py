import json
from decimal import Decimal
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
    if backtest_result["apr"] < 0.13:
        return 0
    return backtest_result["avg_deposit"]


def get_highest_score_result(file_path: str) -> BacktestResult:
    results = _read_from_file(file_path)["results"]
    max_value = 0
    final_result = None
    for result in results:
        evaluate_value = evaluate(result)
        if evaluate_value > max_value:
            max_value = evaluate_value
            final_result = result
    return final_result


def _sum_positions(position_dict):
    return_amount = Decimal("0")
    for position in position_dict["logs"]:
        return_amount += Decimal(position["positions"])
    return return_amount


def ensemble(file_path_list: list[str]):
    """
    currently support 2 indicator ensemble only
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

    file_path_list = [
        "local/backtest/bollinger_BTC_20220326_20220624",
        "local/backtest/macd_BTC_20220326_20220624",
    ]
    ensemble(file_path_list)


if __name__ == "__main__":
    main()
