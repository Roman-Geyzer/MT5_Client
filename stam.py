"""
This script loads historical data for 28 major currency pairs from MetaTrader 5.
The data is saved in CSV files in the 'historical_data' directory.
The script fetches data before the earliest existing data and after the latest existing data.
Additionally, it calculates technical indicators and appends them as new columns to the CSV files,
including Support and Resistance (SR) indicators: upper_sr, lower_sr, prev_upper_sr_level, prev_lower_sr_level.
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

# SR Parameters
SR_PARAMS = {
    'period_for_sr': 100,                # Lookback period for SR levels
    'touches_for_sr': 3,                 # Number of touches for SR levels
    'slack_for_sr_atr_div': 10.0,        # Slack for SR levels based on ATR
    'atr_rejection_multiplier': 1.0,     # ATR rejection multiplier for SR levels
    'min_height_of_sr_distance': 3.0,    # Min height of SR distance - used in calculating SR levels
    'max_height_of_sr_distance': 30.0,  # Max height of SR distance - used in calculating SR levels
}

def calculate_indicators(df, pip):
    """
    Calculate technical indicators and add them as new columns to the DataFrame.

    Parameters:
    - df: pandas DataFrame with columns ['time', 'open', 'high', 'low', 'close', 'spread']
    - pip: float, pip value based on the currency pair

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

    # 7. Calculate Spread and Practical Spread, populate bid and ask

    # calculate bid and ask
    df['bid'] = df['open'] - df['spread'] * pip / 2
    df['ask'] = df['open'] + df['spread'] * pip / 2

    return df

def calculate_sr_levels(df, sr_params):
    """
    Calculate Support and Resistance (SR) levels and add them as new columns to the DataFrame.

    Parameters:
    - df: pandas DataFrame sorted by 'time' in ascending order with necessary price data and ATR
    - sr_params: dict containing SR calculation parameters

    Returns:
    - df: pandas DataFrame with SR indicator columns added
    """

    # Initialize SR columns with default values
    df['upper_sr'] = 0.0
    df['lower_sr'] = 0.0
    df['prev_upper_sr_level'] = 0.0
    df['prev_lower_sr_level'] = 0.0

    # Extract SR parameters
    period_for_sr = sr_params['period_for_sr']
    touches_for_sr = sr_params['touches_for_sr']
    slack_for_sr_atr_div = sr_params['slack_for_sr_atr_div']
    atr_rejection_multiplier = sr_params['atr_rejection_multiplier']
    min_height_of_sr_distance = sr_params['min_height_of_sr_distance']
    max_height_of_sr_distance = sr_params['max_height_of_sr_distance']

    # Iterate over the DataFrame to calculate SR levels
    for i in range(len(df)):
        if i < period_for_sr:
            # Not enough data to calculate SR levels
            continue

        # Define the window for SR calculation
        window_start = i - period_for_sr
        window_end = i  # Exclusive

        recent_rates = df.iloc[window_start:window_end]

        atr = df.at[i, 'ATR']
        if pd.isna(atr) or atr == 0:
            # Skip if ATR is not available
            continue

        uSlackForSR = atr / slack_for_sr_atr_div
        uRejectionFromSR = atr * atr_rejection_multiplier

        current_open = df.at[i, 'open']

        # Initialize HighSR and LowSR
        HighSR = current_open + min_height_of_sr_distance * uSlackForSR
        LowSR = current_open - min_height_of_sr_distance * uSlackForSR

        # LocalMax and LocalMin
        LocalMax = recent_rates['high'].max()
        LocalMin = recent_rates['low'].min()

        # Initialize LoopCounter
        LoopCounter = 0

        # Upper SR Level
        upper_sr_level = 0.0
        upper_limit = 0.0
        while LoopCounter < max_height_of_sr_distance:
            UpperSR = HighSR
            num_touches = count_touches(UpperSR, recent_rates, uRejectionFromSR, upper=True)
            if num_touches >= touches_for_sr:
                upper_sr_level = UpperSR
                break
            else:
                HighSR += uSlackForSR
                LoopCounter += 1
                if HighSR > LocalMax:
                    upper_sr_level = 0
                    break

        # Reset LoopCounter for LowerSR
        LoopCounter = 0

        # Lower SR Level
        lower_sr_level = 0.0
        lower_limit = 0.0
        while LoopCounter < max_height_of_sr_distance:
            LowerSR = LowSR
            num_touches = count_touches(LowerSR, recent_rates, uRejectionFromSR, upper=False)
            if num_touches >= touches_for_sr:
                lower_sr_level = LowerSR
                break
            else:
                LowSR -= uSlackForSR
                LoopCounter += 1
                if LowSR < LocalMin:
                    lower_sr_level = 0
                    break

        # Store SR levels in the DataFrame
        df.at[i, 'upper_sr'] = upper_sr_level
        df.at[i, 'lower_sr'] = lower_sr_level
        print(f"i is {i}, upper_sr is {upper_sr_level}, lower_sr is {lower_sr_level}")


    return df

def count_touches(current_hline, recent_rates, uRejectionFromSR, upper=True):
    """
    Count the number of touches to the given SR level.

    Parameters:
        current_hline (float): The SR level to check.
        recent_rates (DataFrame): The recent rates to check.
        uRejectionFromSR (float): The rejection slack based on ATR.
        upper (bool): True if checking for upper SR, False for lower SR.

    Returns:
        int: Number of touches.
    """
    counter = 0
    for idx in range(len(recent_rates) - 1):
        open_price = recent_rates['open'].iloc[idx]
        close_price = recent_rates['close'].iloc[idx]
        high_price = recent_rates['high'].iloc[idx]
        low_price = recent_rates['low'].iloc[idx]
        candle_size = abs(high_price - low_price)

        if upper:
            # Upper SR check
            if open_price < current_hline and close_price < current_hline:
                if high_price > current_hline or (
                    candle_size > uRejectionFromSR and (current_hline - high_price) < uRejectionFromSR / 2
                ):
                    counter += 1
        else:
            # Lower SR check
            if open_price > current_hline and close_price > current_hline:
                if low_price < current_hline or (
                    candle_size > uRejectionFromSR and (low_price - current_hline) < uRejectionFromSR / 2
                ):
                    counter += 1
    return counter


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
        if 'JPY' in symbol:
            pip_digits = 2
        else:
            pip_digits = 4
        pip = 10 ** -pip_digits

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

                # Calculate technical indicators
                try:
                    df_combined = calculate_indicators(df_combined, pip)
                except Exception as e:
                    print(f"    Error calculating indicators: {e}")
                    # Optionally, you can choose to skip saving or handle the error differently
                    continue

                # Calculate SR levels
                try:
                    df_combined = calculate_sr_levels(df_combined, SR_PARAMS)
                except Exception as e:
                    print(f"    Error calculating SR levels: {e}")
                    # Optionally, handle the error or skip SR calculation
                    continue

                # Handle potential NaN values resulting from SR calculation
                df_combined[['upper_sr', 'lower_sr']] = df_combined[['upper_sr', 'lower_sr']].fillna(0)

                # Save combined data with indicators to Parquet
                try:
                    df_combined.to_parquet(filepath, index=False)
                    print(f"    Updated data with indicators saved to {filepath}")
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