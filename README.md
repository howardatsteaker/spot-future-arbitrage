# spot-future-arbitrage
A multi-indicator arbitrage strategy of the difference between spot and future price

# Prerequisite
python3.9+

# Create virtual environment
```python
python3 -m venv venv
```

## Activate virtual environment
```
. venv/bin/activate
```

# Install modules
```
pip install -r requirement.txt
```

# Configurations
For more information, see [template.yaml](template.yaml)

# Runtime
python3 main.py -c `your_config_file.yaml`

# Backtest

## 1. Download trades data from FTX
See [src/backtest/ftx_trades_downloader.py](src/backtest/ftx_trades_downloader.py)

## 2. Prepare data for backtesting

- Prepare kline data for a symbol
- Prepare merged trades that merge the future and spot trades within 1 second and caculate the basis between these two symbols
- Base on the merged trades, one can further formulate merged kline into a specified resolution. For example: 1 hour resolution.
- For more information, see [src/backtest/ftx_prepare_backtest_data.py](src/backtest/ftx_prepare_backtest_data.py)

## 3. Run backtesting

Once the data for backtesting is prepared, one can backtest with different indicator. For instance, the [Bollinger Band](src/backtest/backtest_bollinger.py), [MACD](src/backtest/backtest_macd.py) or others.

```
python3 -m src.backtest.backtest
```
