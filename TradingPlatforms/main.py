# main.py
"""
This module runs the MT5 server and exposes its functionalities via Pyro5.
Consolidated into a single MT5Server class.
"""

import Pyro5.api
import Pyro5.server
import MetaTrader5 as mt5
import numpy as np
import datetime
import inspect

from .utils import load_account_details





def print_with_time(msg):
    """
    Function to print a message with the current timestamp.
    Function will print the name of the method that called it (using call stack inspection).
    """
    frame = inspect.currentframe().f_back  # get the caller's frame
    function_name = frame.f_code.co_name  # get the function name
    #lineno = frame.f_lineno  # get the line number
    print("-" * 100)
    print(f"[{datetime.datetime.now()}] - function: {function_name}, prints: *** {msg} ***")
    print("-" * 100)


def print_with_info(*args, levels_up=2, **kwargs):
    """
    Prints information about the call stack up to 'levels_up' levels.

    Parameters:
        *args: Variable length argument list to be printed after the stack info.
        levels_up (int): Number of levels up the stack to print info for. Default is 1.
        **kwargs: Arbitrary keyword arguments to be printed after the stack info.
    """
    frame = inspect.currentframe().f_back  # Start with the caller's frame
    collected_info = []
    
    for level in range(1, levels_up + 1):
        if frame:
            module = frame.f_globals.get('__name__', '<unknown module>')
            lineno = frame.f_lineno
            collected_info.append(f"Level {level}: {module} : Line {lineno}")
            frame = frame.f_back  # Move up the stack
        else:
            collected_info.append(f"Level {level}: <No further stack frames>")
    
    border = "*" * 100
    separator = "-" * 100
    print(border)
    print(f"[{datetime.datetime.now()}] - print with info:")
    for info in collected_info:
        print(info)
        print(separator)
    if args or kwargs:
        print(*args, **kwargs)
    print(border)



@Pyro5.server.expose
class MT5Server:
    """
    MT5 Server Class to interact with MetaTrader 5 terminal and expose functionalities via Pyro5.
    Implements a singleton pattern to ensure only one instance interacts with MT5.
    """

    _instance = None

    def __new__(cls, account_number, password, server_name):
        if cls._instance is None:
            cls._instance = super(MT5Server, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, account_number, password, server_name, server_delta_time=0):
        if not self._initialized:
            self._initialized = True
            self._account_number = account_number
            self._password = password
            self._server_name = server_name
            self._server_delta_time = server_delta_time
            self.initialize_mt5()
            self.login_mt5(self._account_number, self._password, self._server_name)
            print_with_time("MT5Server initialized.")

    def initialize_mt5(self):
        print_with_time("Initializing MetaTrader 5...")
        if not mt5.initialize():
            error_code, description = mt5.last_error()
            raise Exception(f"initialize() failed, error code = {error_code}, description = {description}")
        print_with_time("MetaTrader 5 initialized successfully.")

    def login_mt5(self, account_number, password, server):
        if not mt5.login(account_number, password, server):
            error_code, description = mt5.last_error()
            raise Exception(f"login() failed, error code = {error_code}, description = {description}")
        print_with_time(f"Connected to the trade account {account_number} successfully.")


    def shutdown_mt5(self):
        mt5.shutdown()
        self._initialized = False
        print_with_time("MetaTrader 5 shutdown.")

    # Expose methods
    def account_info(self):
        info = mt5.account_info()
        if info is None:
            return None
        return self._convert_numpy_types(info._asdict())

    def copy_rates(self, symbol, timeframe, count):
        data = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
        if data is None:
            return None
        # Convert structured array to list of dictionaries
        data_list = [dict(zip(data.dtype.names, row)) for row in data]
        # Convert NumPy types to native Python types
        return self._convert_numpy_types(data_list)

    def order_send(self, request):
        print_with_time(f"request is: {request}")
        result = mt5.order_send(request)
        if result is None:
            return None
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print_with_time(f"order_send failed, retcode={result.retcode}, deal={result.deal}, order={result.order}")
        else:
            print_with_time(f"order_send successful (retcode is DONE), deal={result.deal}, order={result.order}")
        return self._convert_numpy_types(result._asdict())

    def positions_get(self, ticket=None):
        positions = mt5.positions_get(ticket=ticket) if ticket else mt5.positions_get()
        if not positions:
            return None
        positions_list = [self._convert_numpy_types(pos._asdict()) for pos in positions]
        return positions_list

    def symbol_info_tick(self, symbol):
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return None
        return self._convert_numpy_types(tick._asdict())

    def symbol_select(self, symbol, select=True):
        return mt5.symbol_select(symbol, select)

    def symbol_info(self, symbol):
        info = mt5.symbol_info(symbol)
        if info is None:
            return None
        return self._convert_numpy_types(info._asdict())
    
    def symbol_info_tick(self, symbol):
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return None
        return self._convert_numpy_types(tick._asdict())

    def history_deals_get(self, ticket):
        if ticket:
            print_with_time(f"ticket : {ticket}")
            return mt5.history_deals_get(ticket=ticket)
        return None

    def copy_rates_from(self, symbol, timeframe, datetime_from, num_bars):
        data = mt5.copy_rates_from(symbol, timeframe, datetime_from, num_bars)
        if data is None:
            return None
        data_list = [dict(zip(data.dtype.names, row)) for row in data]
        return self._convert_numpy_types(data_list)

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        data = mt5.copy_rates_from_pos(symbol, timeframe, start_pos, count)
        if data is None:
            return None
        data_list = [dict(zip(data.dtype.names, row)) for row in data]
        return self._convert_numpy_types(data_list)

    def last_error(self):
        error = mt5.last_error()
        print_with_info(f"error : {error}")
        return self._convert_numpy_types(error)
    


    def shutdown(self):
        self.shutdown_mt5()

    # Helper function to get the time delta between the server and local machine
    def get_time_delta(self):
        return self._server_delta_time
    # Helper function to convert NumPy scalars to native Python types
    def _convert_numpy_types(self, obj):
        if isinstance(obj, dict):
            return {k: self._convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(v) for v in obj]
        elif isinstance(obj, tuple):
            return tuple(self._convert_numpy_types(v) for v in obj)
        elif isinstance(obj, np.generic):
            return obj.item()
        else:
            return obj

    # Expose constants
    def get_constants(self):
        constants = {
            'TIMEFRAMES': {
                'M1': mt5.TIMEFRAME_M1,
                'M5': mt5.TIMEFRAME_M5,
                'M15': mt5.TIMEFRAME_M15,
                'M30': mt5.TIMEFRAME_M30,
                'H1': mt5.TIMEFRAME_H1,
                'H4': mt5.TIMEFRAME_H4,
                'D1': mt5.TIMEFRAME_D1,
                'W1': mt5.TIMEFRAME_W1,
            },
            'ORDER_TYPES': {
                'BUY': mt5.ORDER_TYPE_BUY,
                'BUY_LIMIT': mt5.ORDER_TYPE_BUY_LIMIT,
                'BUY_STOP': mt5.ORDER_TYPE_BUY_STOP,
                'BUY_STOP_LIMIT': mt5.ORDER_TYPE_BUY_STOP_LIMIT,
                'SELL': mt5.ORDER_TYPE_SELL,
                'SELL_LIMIT': mt5.ORDER_TYPE_SELL_LIMIT,
                'SELL_STOP': mt5.ORDER_TYPE_SELL_STOP,
                'SELL_STOP_LIMIT': mt5.ORDER_TYPE_SELL_STOP_LIMIT,
            },
            'TRADE_ACTIONS': {
                'DEAL': mt5.TRADE_ACTION_DEAL,
                'PENDING': mt5.TRADE_ACTION_PENDING,
                'MODIFY': mt5.TRADE_ACTION_MODIFY,
                'REMOVE': mt5.TRADE_ACTION_REMOVE,
                'CLOSE_BY': mt5.TRADE_ACTION_CLOSE_BY,
                'SLTP': mt5.TRADE_ACTION_SLTP,
                'DONE': mt5.TRADE_RETCODE_DONE
            },
            'ORDER_TIME': {
                'GTC': mt5.ORDER_TIME_GTC,
                'SPECIFIED': mt5.ORDER_TIME_SPECIFIED
            },
            'ORDER_FILLING': {
                'FOK': mt5.ORDER_FILLING_FOK
            },
            'TRADE_RETCODES' : {
                'REJECT' : mt5.TRADE_RETCODE_REJECT,
                'CANCEL' : mt5.TRADE_RETCODE_CANCEL,
                'PLACED' : mt5.TRADE_RETCODE_PLACED,
                'DONE' : mt5.TRADE_RETCODE_DONE,
                'DONE_PARTIAL' : mt5.TRADE_RETCODE_DONE_PARTIAL,
                'ERROR' : mt5.TRADE_RETCODE_ERROR,
                'TIMEOUT' : mt5.TRADE_RETCODE_TIMEOUT,
                'INVALID' : mt5.TRADE_RETCODE_INVALID,
                'INVALID_VOLUME' : mt5.TRADE_RETCODE_INVALID_VOLUME,
                'INVALID_PRICE' : mt5.TRADE_RETCODE_INVALID_PRICE,
                'INVALID_STOPS' : mt5.TRADE_RETCODE_INVALID_STOPS,
                'TRADE_DISABLED' : mt5.TRADE_RETCODE_TRADE_DISABLED,
                'MARKET_CLOSED' : mt5.TRADE_RETCODE_MARKET_CLOSED,
                'NO_MONEY' : mt5.TRADE_RETCODE_NO_MONEY,
                'PRICE_CHANGED' : mt5.TRADE_RETCODE_PRICE_CHANGED,
                'PRICE_OFF' : mt5.TRADE_RETCODE_PRICE_OFF,
                'INVALID_EXPIRATION' : mt5.TRADE_RETCODE_INVALID_EXPIRATION,
                'ORDER_CHANGED' : mt5.TRADE_RETCODE_ORDER_CHANGED,
                'TOO_MANY_REQUESTS' : mt5.TRADE_RETCODE_TOO_MANY_REQUESTS,
                'NO_CHANGES' : mt5.TRADE_RETCODE_NO_CHANGES,
                'SERVER_DISABLES_AT' : mt5.TRADE_RETCODE_SERVER_DISABLES_AT,
                'CLIENT_DISABLES_AT' : mt5.TRADE_RETCODE_CLIENT_DISABLES_AT,
                'LOCKED' : mt5.TRADE_RETCODE_LOCKED,
                'FROZEN' : mt5.TRADE_RETCODE_FROZEN,
                'INVALID_FILL' : mt5.TRADE_RETCODE_INVALID_FILL,
                'CONNECTION' : mt5.TRADE_RETCODE_CONNECTION,
                'ONLY_REAL' : mt5.TRADE_RETCODE_ONLY_REAL,
                'LIMIT_ORDERS' : mt5.TRADE_RETCODE_LIMIT_ORDERS,
                'LIMIT_VOLUME' : mt5.TRADE_RETCODE_LIMIT_VOLUME,
                'INVALID_ORDER' : mt5.TRADE_RETCODE_INVALID_ORDER,
                'POSITION_CLOSED' : mt5.TRADE_RETCODE_POSITION_CLOSED,
                'INVALID_CLOSE_VOLUME' : mt5.TRADE_RETCODE_INVALID_CLOSE_VOLUME,
                'CLOSE_ORDER_EXIST' : mt5.TRADE_RETCODE_CLOSE_ORDER_EXIST,
                'LIMIT_POSITIONS' : mt5.TRADE_RETCODE_LIMIT_POSITIONS,
                'REJECT_CANCEL' : mt5.TRADE_RETCODE_REJECT_CANCEL,
                'LONG_ONLY' : mt5.TRADE_RETCODE_LONG_ONLY,
                'SHORT_ONLY' : mt5.TRADE_RETCODE_SHORT_ONLY,
                'CLOSE_ONLY' : mt5.TRADE_RETCODE_CLOSE_ONLY,
                'FIFO_CLOSE' : mt5.TRADE_RETCODE_FIFO_CLOSE
            },
        }
        # Convert NumPy types to native Python types
        return self._convert_numpy_types(constants)

def main():
    # Load account details
    print("Loading account details...")
    account_number, server_name, account_password, server_delta_time = load_account_details()
    print(f"Account details loaded successfully: {account_number}, {server_name}")
    # Initialize the MT5Server with account details
    mt5_server_instance = MT5Server(account_number, account_password, server_name, server_delta_time)

    # Create a Pyro5 daemon and register the MT5Server object
    daemon = Pyro5.server.Daemon(host="localhost", port=9090)
    uri = daemon.register(mt5_server_instance, objectId="trading.platform.MT5Server")
    print_with_time(f"MT5Server is running. URI: {uri}")

    try:
        print_with_time("MT5Server is ready.")
        daemon.requestLoop()
    except KeyboardInterrupt:
        print_with_time("Shutting down MT5Server...")
    finally:
        mt5_server_instance.shutdown()
        daemon.shutdown()

if __name__ == "__main__":
    main()