from datetime import timedelta
from decimal import Decimal
import pathlib
import numpy as np
import pandas as pd
from src.backtest.ftx_data_types import HedgeType, MarketOrder, Side, BaseState
from src.backtest import backtest_util

trades_path = "local/merged_trades/BTC_0624/1640995200_1656028800.parquet"
spot_klines_path = "local/kline/BTC_USD/1640995200_1656028800_1H.parquet"
future_klines_path = "local/kline/BTC_0624/1640995200_1656028800_1H.parquet"

trades = pd.read_parquet(trades_path)
spot_klines = pd.read_parquet(spot_klines_path)
future_klines = pd.read_parquet(future_klines_path)

std_mult = Decimal('0.9')
while std_mult < Decimal('3.0'):
    std_mult += Decimal('0.1')
    save_path = pathlib.Path(f"local/backtest/macd_{std_mult}")
    summary_path = save_path / "summary.json"
    plot_path = save_path / "plot.jpg"
    if summary_path.exists():
        continue
    # config
    config = {
        'fee_rate': Decimal('0.000228'),
        'collateral_weight': Decimal('0.975'),
        'ts_to_stop_open': 1655953200,  # 2022-6-23 03:00:00 UTC
        'ts_to_expiry': 1656039600,  # 2022-6-24 03:00:00 UTC
        'expiration_price': Decimal('21141.1'),
        'leverage': Decimal('3'),
        'macd_fast_length': 12,
        'macd_slow_length': 26,
        'macd_signal_length': 9,
        'macd_std_length': 20,
        'macd_std_mult': std_mult,
    }

    # merge klines
    spot_close = spot_klines['close'].rename('s_close')
    future_close = future_klines['close'].rename('f_close')
    concat_close = pd.concat([spot_close, future_close], axis=1)
    concat_close['basis'] = concat_close['f_close'] - concat_close['s_close']
    concat_close['fast_ema'] = concat_close['basis'].ewm(span=config['macd_fast_length']).mean()
    concat_close['slow_ema'] = concat_close['basis'].ewm(span=config['macd_slow_length']).mean()
    concat_close['dif'] = concat_close['fast_ema']  - concat_close['slow_ema']
    concat_close['macd'] = concat_close['dif'].ewm(span=config['macd_signal_length']).mean()
    concat_close['dif_sub_macd'] = concat_close['dif'] - concat_close['macd']
    concat_close['std'] = concat_close['dif_sub_macd'].rolling(config['macd_std_length']).std()


    def get_current_dif_sub_macd(basis: Decimal, fast_ema: Decimal, slow_ema: Decimal, macd: Decimal) -> Decimal:
        fast_alpha = Decimal(str(2 / (config['macd_fast_length'] + 1)))
        slow_alpha = Decimal(str(2 / (config['macd_slow_length'] + 1)))
        macd_alpha = Decimal(str(2 / (config['macd_signal_length'] + 1)))
        new_fast_ema = (1 - fast_alpha) * fast_ema + fast_alpha * basis
        new_slow_ewm = (1 - slow_alpha) * slow_ema + slow_alpha * basis
        new_dif = new_fast_ema - new_slow_ewm
        new_macd = (1 - macd_alpha) * macd + macd_alpha * new_dif
        return new_dif - new_macd


    # run backtest
    state = BaseState()
    logs = []

    trades_iter = trades.itertuples()
    while True:
        try:
            trade = next(trades_iter)
        except StopIteration:
            break
        dt: pd.Timestamp = trade.Index
        ts: float = dt.timestamp()
        basis = Decimal(str(trade.basis))
        future_side = trade.f_side
        spot_price = Decimal(str(trade.s_price))
        spot_size = Decimal(str(trade.s_size))
        future_price = Decimal(str(trade.f_price))
        future_size = Decimal(str(trade.f_size))
        max_available_size = min(spot_size, future_size)
        state.basis = basis

        # expiry liquidation
        if ts >= config['ts_to_expiry']:
            break

        # indicator ready
        dt_truncate = dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        if dt_truncate not in concat_close.index:
            continue
        fast_ema = Decimal(str(concat_close.loc[dt_truncate]['fast_ema']))
        slow_ema = Decimal(str(concat_close.loc[dt_truncate]['slow_ema']))
        macd = Decimal(str(concat_close.loc[dt_truncate]['macd']))
        dif_sub_macd = get_current_dif_sub_macd(basis, fast_ema, slow_ema, macd)
        std = Decimal(str(concat_close.loc[dt_truncate]['std']))
        if std.is_nan():
            continue

        # open position
        is_macd_open = dif_sub_macd > config['macd_std_mult'] * std
        if future_side == 'SELL' and is_macd_open and ts < config['ts_to_stop_open']:
            spot_market_order = MarketOrder(
                symbol='spot', side=Side.BUY, price=spot_price,
                size=max_available_size, create_timestamp=ts, fee_rate=config['fee_rate']
            )
            future_market_order = MarketOrder(
                symbol='future', side=Side.SELL, price=future_price,
                size=max_available_size, create_timestamp=ts, fee_rate=config['fee_rate']
            )
            fee = future_market_order.fee + spot_market_order.fee
            expected_return = future_market_order.order_value - spot_market_order.order_value - 2 * fee
            if expected_return > 0:
                state.open_position(spot_market_order, future_market_order, config['collateral_weight'], config['leverage'])
                state.append_hedge_trade(ts, HedgeType.OPEN, basis)

        # close position
        if state.spot_position > 0:
            entry_basis = state.future_entry_price - state.spot_entry_price
            is_macd_close = dif_sub_macd < -config['macd_std_mult'] * std and entry_basis > basis
            if future_side == 'BUY' and (basis <= 0 or is_macd_close):
                close_size = min(state.spot_position, max_available_size)
                spot_market_order = MarketOrder(
                    symbol='spot', side=Side.SELL, price=spot_price,
                    size=close_size, create_timestamp=ts, fee_rate=config['fee_rate']
                )
                future_market_order = MarketOrder(
                    symbol='future', side=Side.BUY, price=future_price,
                    size=close_size, create_timestamp=ts, fee_rate=config['fee_rate']
                )
                state.close_position(spot_market_order, future_market_order, config['collateral_weight'], config['leverage'])
                state.append_hedge_trade(ts, HedgeType.CLOSE, basis)
        
        # log
        logs.append(state.to_log_state(timestamp=ts))

    # liquidation after expiry
    if state.spot_position > 0:
        liquidation_size = state.spot_position
        spot_market_order = MarketOrder(
            symbol='spot', side=Side.SELL, price=config['expiration_price'],
            size=liquidation_size, create_timestamp=config['ts_to_expiry'], fee_rate=config['fee_rate']
        )
        future_market_order = MarketOrder(
            symbol='future', side=Side.BUY, price=config['expiration_price'],
            size=liquidation_size, create_timestamp=config['ts_to_expiry'], fee_rate=config['fee_rate']
        )
        state.close_position(spot_market_order, future_market_order, config['collateral_weight'], config['leverage'])
        state.append_hedge_trade(config['ts_to_expiry'], HedgeType.CLOSE, Decimal(0))
        logs.append(state.to_log_state(config['ts_to_expiry']))

    # save summary
    backtest_util.save_summary(logs, summary_path)

    # plot
    save_path = "local/backtest/macd/plot.jpg"
    backtest_util.plot_logs(logs, state.hedge_trades, plot_path, to_show=False)
