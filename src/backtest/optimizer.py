import json
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
        return data["results"]


def evaluate(backtest_result: BacktestResult) -> float:
    if backtest_result["apr"] < 0.13:
        return 0
    return backtest_result["avg_deposit"]


def main():
    file_path = "local/backtest/bollinger_BTC_20220326_20220624/summary.json"
    results = _read_from_file(file_path)
    max_value = 0
    final_result = None
    for result in results:
        print(result)
        evaluate_value = evaluate(result)
        if evaluate_value > max_value:
            max_value = evaluate_value
            final_result = result
    print(f"final result: {final_result}")


if __name__ == "__main__":
    main()
