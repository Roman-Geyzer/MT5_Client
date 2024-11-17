# load_history.py

"""
This script loads historical data for 28 major currency pairs from MetaTrader 5.
The data is saved in CSV files in the 'historical_data' directory.
The script fetches data before the earliest existing data and after the latest existing data.
Additionally, it calculates technical indicators and appends them as new columns to the CSV files.
"""

import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
import ta  # Updated import for the 'ta' library

# Account details
account_number = 10004657677
server_name = "MetaQuotes-Demo"
account_password = "*fJrJ0Ma"

drive = "x:"
folder = "historical_data"

# List of 28 major currency pairs
currency_pairs = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD', 'AUDUSD', 'NZDUSD',
    'EURGBP', 'EURJPY', 'EURCHF', 'EURCAD', 'EURAUD', 'EURNZD',
    'GBPJPY', 'GBPCHF', 'GBPCAD', 'GBPAUD', 'GBPNZD',
    'AUDJPY', 'AUDCHF', 'AUDCAD', 'AUDNZD',
    'NZDJPY', 'NZDCHF', 'NZDCAD',
    'CADJPY', 'CADCHF',
    'CHFJPY'
]

# Timeframes
timeframes = {
    'M1': mt5.TIMEFRAME_M1,
    'M5': mt5.TIMEFRAME_M5,
    'M15': mt5.TIMEFRAME_M15,
    'M30': mt5.TIMEFRAME_M30,
    'H1': mt5.TIMEFRAME_H1,
    'H4': mt5.TIMEFRAME_H4,
    'D1': mt5.TIMEFRAME_D1,
    'W1': mt5.TIMEFRAME_W1,
}

def calculate_indicators(df):
    """
    Calculate technical indicators and add them as new columns to the DataFrame.

    Parameters:
    - df: pandas DataFrame with columns ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']

    Returns:
    - df: pandas DataFrame with new indicator columns
    """

    # Ensure the DataFrame is sorted by time in ascending order
    df = df.sort_values(by='time').reset_index(drop=True)

    # Check if necessary columns exist
    required_columns = ['open', 'high', 'low', 'close']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # 1. Calculate RSI (Period 14)
    rsi_indicator = ta.momentum.RSIIndicator(close=df['close'], window=14)
    df['RSI'] = rsi_indicator.rsi()

    # 2. Calculate ATR (Period 14)
    atr_indicator = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14)
    df['ATR'] = atr_indicator.average_true_range()

    # 3. Calculate Bollinger Bands with different deviations
    bollinger_settings = [
        (20, 1.5, 'BB15'),
        (20, 2.0, 'BB20'),
        (20, 2.5, 'BB25')  # Updated label from BB22 to BB25
    ]

    for period, deviation, label in bollinger_settings:
        bollinger = ta.volatility.BollingerBands(close=df['close'], window=period, window_dev=deviation)
        df[f'{label}_Upper'] = bollinger.bollinger_hband()
        df[f'{label}_Middle'] = bollinger.bollinger_mavg()
        df[f'{label}_Lower'] = bollinger.bollinger_lband()
        # Boolean flags for Close above Upper Band and below Lower Band
        df[f'{label}_Bool_Above'] = df['close'] > bollinger.bollinger_hband()
        df[f'{label}_Bool_Below'] = df['close'] < bollinger.bollinger_lband()

    # 4. Calculate Moving Averages
    moving_averages = {
        'MA_7': 7,      # Short-term
        'MA_21': 21,    # Medium-term
        'MA_50': 50,    # Long-term
        'MA_200': 200   # Very Long-term
    }

    for ma_label, period in moving_averages.items():
        ma_indicator = ta.trend.SMAIndicator(close=df['close'], window=period)
        df[ma_label] = ma_indicator.sma_indicator()

    # 5. Calculate Green-Red Candle Ratios
    green_red_settings = [
        (50, 'GA_50'),
        (100, 'GA_100'),
        (200, 'GA_200'),
        (500, 'GA_500')
    ]

    # A green candle is when Close > Open
    df['Is_Green'] = df['close'] > df['open']
    df['Is_Red'] = df['close'] < df['open']

    for window, column_name in green_red_settings:
        df[column_name] = df['Is_Green'].rolling(window=window).sum() / window

    # Drop intermediate columns if not needed
    df.drop(['Is_Green', 'Is_Red'], axis=1, inplace=True)

    # 6. Compare Price with Moving Averages
    for ma_label in ['MA_7', 'MA_21', 'MA_50']:
        df[f'{ma_label}_comp'] = np.where(
            df['close'] > df[ma_label],
            'above',
            np.where(df['close'] < df[ma_label], 'below', 'equal')
        )

    return df

def load_history():
    # Initialize MetaTrader 5
    print("Initializing MetaTrader 5...")
    if not mt5.initialize():
        print(f"initialize() failed, error code = {mt5.last_error()}")
        mt5.shutdown()
        quit()
    print("MetaTrader 5 initialized successfully.")

    # Log in to the account
    print("Logging into the account...")
    if not mt5.login(account_number, account_password, server=server_name):
        print(f"Login failed, error code = {mt5.last_error()}")
        mt5.shutdown()
        quit()
    print(f"Connected to the trade account {account_number} successfully.")

    # Create output directory if not exists
    output_dir = os.path.join(drive, folder)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Loop over currency pairs and timeframes
    for symbol in currency_pairs:
        # Ensure the symbol is available in MT5
        selected = mt5.symbol_select(symbol, True)
        if not selected:
            print(f"Failed to select symbol {symbol}")
            continue

        print(f"\nProcessing symbol: {symbol}")
        print(f"Current time is: {datetime.now()}")
        for tf_name, tf in timeframes.items():
            print(f"  Timeframe: {tf_name}")
            # Define file path
            filename = f"{symbol}_{tf_name}.csv"
            filepath = os.path.join(output_dir, filename)

            df_existing = pd.DataFrame()
            earliest_time = None
            latest_time = None

            if os.path.exists(filepath):
                try:
                    # Load existing data
                    df_existing = pd.read_csv(filepath, parse_dates=['time'])
                    df_existing['time'] = pd.to_datetime(df_existing['time'])
                    earliest_time = df_existing['time'].min()
                    latest_time = df_existing['time'].max()
                    print(f"    Existing data from {earliest_time} to {latest_time}")
                except Exception as e:
                    print(f"    Error loading existing data: {e}")
                    # If error in loading, reset existing data
                    df_existing = pd.DataFrame()
                    earliest_time = None
                    latest_time = None
            else:
                print("    No existing data found.")

            # Initialize list to hold new data DataFrames
            data_frames = []

            # Fetch data before earliest_time (if any)
            if earliest_time is None or earliest_time > datetime(2000, 1, 1):
                # Fetch data from desired start date to one second before the earliest existing time
                fetch_end_date = earliest_time - timedelta(seconds=1) if earliest_time else datetime.now()
                fetch_start_date = datetime(2000, 1, 1)
                print(f"    Fetching data from {fetch_start_date} to {fetch_end_date}")
                rates = mt5.copy_rates_range(symbol, tf, fetch_start_date, fetch_end_date)
                if rates is not None and len(rates) > 0:
                    df_before = pd.DataFrame(rates)
                    df_before['time'] = pd.to_datetime(df_before['time'], unit='s')
                    data_frames.append(df_before)
                else:
                    print(f"    No data fetched for period {fetch_start_date} to {fetch_end_date}")

            # Fetch data after latest_time (if any)
            if latest_time is None or latest_time < datetime.now():
                # Fetch data from one second after the latest existing time to now
                fetch_start_date = latest_time + timedelta(seconds=1) if latest_time else datetime(2000, 1, 1)
                fetch_end_date = datetime.now()
                print(f"    Fetching data from {fetch_start_date} to {fetch_end_date}")
                rates = mt5.copy_rates_range(symbol, tf, fetch_start_date, fetch_end_date)
                if rates is not None and len(rates) > 0:
                    df_after = pd.DataFrame(rates)
                    df_after['time'] = pd.to_datetime(df_after['time'], unit='s')
                    data_frames.append(df_after)
                else:
                    print(f"    No data fetched for period {fetch_start_date} to {fetch_end_date}")

            # Combine all data
            if not df_existing.empty or data_frames:
                if not df_existing.empty:
                    data_frames.append(df_existing)
                df_combined = pd.concat(data_frames, ignore_index=True)
                # Remove duplicates
                df_combined.drop_duplicates(subset=['time'], inplace=True)
                # Sort by time
                df_combined.sort_values(by='time', inplace=True)
                # Reset index
                df_combined.reset_index(drop=True, inplace=True)

                # Calculate technical indicators
                try:
                    df_combined = calculate_indicators(df_combined)
                except Exception as e:
                    print(f"    Error calculating indicators: {e}")
                    # Optionally, you can choose to skip saving or handle the error differently
                    continue

                # Save combined data with indicators
                try:
                    df_combined.to_csv(filepath, index=False)
                    print(f"    Updated data with indicators saved to {filepath}")
                except Exception as e:
                    print(f"    Error saving data to CSV: {e}")
            else:
                print(f"    No data available for {symbol} on timeframe {tf_name}")

            # Sleep to prevent rate limiting
            time.sleep(0.5)

    # Shutdown MT5 connection
    mt5.shutdown()
    print("\nMetaTrader 5 connection closed.")

# Run the function
if __name__ == "__main__":
    load_history()