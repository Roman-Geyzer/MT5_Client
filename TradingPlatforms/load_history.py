# load_history.py
"""
This script loads historical data for 28 major currency pairs from MetaTrader 5.
The data is saved in Parquet files in the 'historical_data' directory.
The script fetches data before the earliest existing data and after the latest existing data.
It only handles price data; indicators are calculated in a separate script.
"""

import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
import os
import time

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
            # Define file path with Parquet extension
            filename = f"{symbol}_{tf_name}.parquet"
            filepath = os.path.join(output_dir, filename)

            df_existing = pd.DataFrame()
            earliest_time = None
            latest_time = None

            if os.path.exists(filepath):
                try:
                    # Load existing data from Parquet
                    df_existing = pd.read_parquet(filepath)
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

                # Set efficient data types
                df_combined['open'] = df_combined['open'].astype('float32')
                df_combined['high'] = df_combined['high'].astype('float32')
                df_combined['low'] = df_combined['low'].astype('float32')
                df_combined['close'] = df_combined['close'].astype('float32')
                df_combined['spread'] = df_combined['spread'].astype('int32')

                # Save combined data to Parquet
                try:
                    df_combined.to_parquet(filepath, index=False)
                    print(f"    Updated data saved to {filepath}")
                except Exception as e:
                    print(f"    Error saving data to Parquet: {e}")
            else:
                print(f"    No data available for {symbol} on timeframe {tf_name}")

            # Sleep to prevent rate limiting
            time.sleep(0.5)

    # Shutdown MT5 connection
    mt5.shutdown()
    print("\nMetaTrader 5 connection closed.")

if __name__ == "__main__":
    load_history()