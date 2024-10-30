# mt5_server/mt5_connector.py

import MetaTrader5 as mt5
import time

class MT5Connector:
    _instance = None

    def __new__(cls):
        # Simple Singleton implementation without threading
        if cls._instance is None:
            cls._instance = super(MT5Connector, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def initialize_mt5(self):
        if not self._initialized:
            print("Initializing MetaTrader 5...")
            if not mt5.initialize():
                error_code, description = mt5.last_error()
                raise Exception(f"initialize() failed, error code = {error_code}, description = {description}")
            self._initialized = True
            print("MetaTrader 5 initialized successfully.")
        else:
            print("MetaTrader 5 already initialized.")
        return True

    def login_mt5(self, account_number, password, server):
        if not mt5.login(account_number, password, server):
            error_code, description = mt5.last_error()
            raise Exception(f"login() failed, error code = {error_code}, description = {description}")
        print(f"Connected to the trade account {account_number} successfully.")

    def shutdown_mt5(self):
        if self._initialized:
            mt5.shutdown()
            self._initialized = False
            print("MetaTrader 5 shutdown.")
        else:
            print("MetaTrader 5 was not initialized.")

    # Expose constants
    @staticmethod
    def get_constants():
        return {
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
                'DONE' : mt5.TRADE_RETCODE_DONE
            },
            'ORDER_TIME': {
                'GTC': mt5.ORDER_TIME_GTC,
                'SPECIFIED': mt5.ORDER_TIME_SPECIFIED
            },
            'ORDER_FILLING': {
                'FOK': mt5.ORDER_FILLING_FOK
            },
        }
    
    # Expose functions
    @staticmethod
    def account_info():
        return mt5.account_info()
    
    @staticmethod
    def copy_rates(symbol, timeframe, count):
        return mt5.copy_rates(symbol, timeframe, count)
    
    @staticmethod
    def order_send(request):
        return mt5.order_send(request)
    
    @staticmethod
    def positions_get(symbol):
        return mt5.positions_get(symbol)
    
    @staticmethod
    def symbol_info_tick(symbol):
        return mt5.symbol_info_tick(symbol)
    
    @staticmethod
    def symbol_select(symbol, select=True):
        return mt5.symbol_select(symbol, select)
    
    @staticmethod
    def symbol_info(symbol):
        return mt5.symbol_info(symbol)
    
    @staticmethod
    def history_deals_get(request):
        return mt5.history_deals_get(request)
    
    @staticmethod
    def copy_rates_from(symbol, timeframe, datetime_from, num_bars):
        return mt5.copy_rates_from_pos(symbol, timeframe, datetime_from, num_bars)
    
    @staticmethod
    def copy_rates_from_post(symbol, timeframe, start_pos, count):
        return mt5.copy_rates_from_pos(symbol, timeframe, start_pos, count)
    
    @staticmethod
    def last_error():
        return mt5.last_error()
    

#Backup:
"""

    
    # internal order send
    @staticmethod
    def _RawOrder(order_type, symbol, volume, price, comment=None, ticket=None):
        order = {
        "action":    mt5.TRADE_ACTION_DEAL,
        "symbol":    symbol,
        "volume":    volume,
        "type":      order_type,
        "price":     price,
        "deviation": 10,
        }
        if comment is not None:
            order["comment"] = comment
        if ticket is not None:
            order["position"] = ticket
        return mt5.order_send(order)

    # Wrapper methods for mt5 functions
    def account_info(self):
        info = mt5.account_info()
        if info is not None:
            return info._asdict()
        else:
            raise Exception("Failed to get account info.")

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        rates = mt5.copy_rates_from_pos(symbol, timeframe, start_pos, count)
        if rates is not None:
            return rates.tolist()
        else:
            raise Exception(f"Failed to get rates for {symbol}.")

    def order_send(self, request):
        result = mt5.order_send(request)
        if result is not None:
            return result._asdict()
        else:
            raise Exception("Failed to send order.")
        
    def positions_get(self, request):
        result = mt5.positions_get(request)
        if result is not None:
            return result._asdict()
        else:
            raise Exception("Failed to get positions.")
        
    # Close specific order
    def Close(self, symbol, *, comment=None, ticket=None):
        if ticket is not None:
            position = self.positions_get(ticket=ticket)
        else:
            return False
        for tries in range(3):
            for tries in range(3):
                info = self.symbol_info_tick(symbol)
                if info is None:
                    return False
                if position.type == mt5.ORDER_TYPE_BUY:
                    r = self._RawOrder(mt5.ORDER_TYPE_SELL, symbol, position.volume, info.bid, comment, position.ticket)
                else:
                    r = self._RawOrder(mt5.ORDER_TYPE_BUY, symbol, position.volume, info.ask, comment, position.ticket)
                # check results
                if r is None:
                    return False
                if r.retcode != mt5.TRADE_RETCODE_REQUOTE and r.retcode != mt5.TRADE_RETCODE_PRICE_OFF:
                    if r.retcode == mt5.TRADE_RETCODE_DONE:
                        return True
        time.sleep(0.1)*tries
        return False
    #
        


#
"""
        