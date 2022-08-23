import os
import pathlib
import pandas as pd
from src.backtest.ftx_data_loader import DataLoader


def save_kline_from_trades(data_dir: str, symbol: str, start_ts: int, end_ts: int, resolution: str = '1H', save_dir: str = 'local/kline'):
    symbol_path_name = symbol.replace('-', '_').replace('/', '_')
    data_loader = DataLoader(data_dir)
    trades_df = data_loader.get_trades_df(start_ts, end_ts)
    klines_df = data_loader.trades_df_2_klines(trades_df, resolution=resolution)
    filename = os.path.join(save_dir, symbol_path_name, f"{start_ts}_{end_ts}_{resolution}.parquet")
    if not os.path.exists(filename):
        pathlib.Path(filename).parent.mkdir(parents=True, exist_ok=True)
        klines_df.to_parquet(filename)


def save_merged_trades(future: str, future_dir: str, spot_dir: str, start_ts: int, end_ts: int, save_path: str = 'local/merged_trades'):
    future_path_name = future.replace('-', '_')
    save_path = os.path.join(save_path, future_path_name)
    filename = os.path.join(save_path, f"{start_ts}_{end_ts}.parquet")
    if not os.path.exists(save_path):
        pathlib.Path(save_path).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(filename):
        spot_data_loader = DataLoader(spot_dir)
        future_data_loader = DataLoader(future_dir)
        spot_trades_df = spot_data_loader.get_trades_df(start_ts, end_ts)
        future_trades_df = future_data_loader.get_trades_df(start_ts, end_ts)
        spot_trades_df.rename(columns={'id': 's_id', 'price': 's_price', 'size': 's_size', 'side': 's_side'}, inplace=True)
        future_trades_df.rename(columns={'id': 'f_id', 'price': 'f_price', 'size': 'f_size', 'side': 'f_side'}, inplace=True)
        resample_1s_spot_df = spot_trades_df.resample('1s').last()
        resample_1s_future_df = future_trades_df.resample('1s').last()
        concat_df = pd.concat([resample_1s_spot_df, resample_1s_future_df], axis=1)
        concat_df.dropna(inplace=True)
        concat_df = concat_df[concat_df['s_side'] != concat_df['f_side']]
        concat_df['basis'] = concat_df['f_price'] - concat_df['s_price']
        concat_df.to_parquet(filename)


def save_merged_kline(future: str, future_dir: str, spot_dir: str, start_ts: int, end_ts: int, resolution: str = '1H', save_path: str = 'local/merged_kline'):
    future_path_name = future.replace('-', '_')
    filename = os.path.join(save_path, future_path_name, f"{start_ts}_{end_ts}.parquet")
    path = pathlib.Path(filename)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        spot_data_loader = DataLoader(spot_dir)
        future_data_loader = DataLoader(future_dir)
        spot_trades_df = spot_data_loader.get_trades_df(start_ts, end_ts)
        future_trades_df = future_data_loader.get_trades_df(start_ts, end_ts)
        spot_trades_df.rename(columns={'id': 's_id', 'price': 's_price', 'size': 's_size', 'side': 's_side'}, inplace=True)
        future_trades_df.rename(columns={'id': 'f_id', 'price': 'f_price', 'size': 'f_size', 'side': 'f_side'}, inplace=True)
        resample_1s_spot_df = spot_trades_df.resample('1s').last()
        resample_1s_future_df = future_trades_df.resample('1s').last()
        concat_df = pd.concat([resample_1s_spot_df, resample_1s_future_df], axis=1)
        concat_df.dropna(inplace=True)
        concat_df = concat_df[concat_df['s_side'] != concat_df['f_side']]
        concat_df['basis'] = concat_df['f_price'] - concat_df['s_price']
        resample: pd. DataFrame = concat_df.resample(resolution).agg({'basis': 'ohlc'})
        resample = resample.droplevel(0, axis=1)
        resample.to_parquet(path)


if __name__ == '__main__':
    spot = 'BTC/USD'
    future = 'BTC-0624'
    spot_data_dir = 'local/trades/BTC_USD'
    future_data_dir = 'local/trades/BTC_0624'
    start = 1640995200  # 2022-1-1 00:00:00 UTC
    end = 1656028800  # 2022-6-24 00:00:00 UTC
    save_kline_from_trades(spot_data_dir, spot, start, end, resolution='1H', save_dir='local/kline')
    save_kline_from_trades(future_data_dir, future, start, end, resolution='1H', save_dir='local/kline')
    save_merged_trades(future, future_data_dir, spot_data_dir, start, end, save_path='local/merged_trades')
    save_merged_kline(future, future_data_dir, spot_data_dir, start, end, resolution='1H', save_path='local/merged_kline')
