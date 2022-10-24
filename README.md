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

```
pip install git+https://github.com/steakr/funding-service-client.git@0.0.11
```

# Configurations
For more information, see [template.yaml](template.yaml)

# Runtime
python3 main.py -c `your_config_file.yaml`

# Backtest

```
python3 -m src.backtest.main
```
