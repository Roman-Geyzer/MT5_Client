# main.py
"""
main module to run the MT5 server.
"""
import Pyro5.server
import MetaTrader5 as mt5
from time import sleep
from .utlis import catch_i_times_with_s_seconds_delay





# mt5_server/mt5_server.py

import Pyro5.server
from .mt5_connector import MT5Connector
import Pyro5.api

account_number = 10004657677
server_name = "MetaQuotes-Demo"
account_password = "*fJrJ0Ma"
#MT5 Exe file path
#mt5_path = "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
@Pyro5.server.expose
class MT5Server:
    """
    MT5 Server Class to expose MT5 functionalities via Pyro5.
    """

    def __init__(self, account_number, password, server_name):
        self.mt5 = MT5Connector()
        self.mt5.initialize_mt5()
        self.mt5.login_mt5(account_number, password, server_name)
        print("MT5Server initialized.")

    def shutdown(self):
        self.mt5.shutdown_mt5()

    # Expose wrapper methods
    def account_info(self):
        return self.mt5.account_info()

    def copy_rates(self, symbol, timeframe, count):
        return self.mt5.copy_rates_from_pos(symbol, timeframe, 0, count)

    def order_send(self, request):
        return self.mt5.order_send(request)

    # Expose constants
    def get_constants(self):
        return self.mt5.get_constants()

def main():

    # Initialize the MT5Server with account details
    mt5_server_instance = MT5Server(account_number, account_password, server_name)

    # Create a Pyro5 daemon and register the MT5Server object
    daemon = Pyro5.server.Daemon(host="localhost", port=9090)
    uri = daemon.register(mt5_server_instance, objectId="trading.platform.MT5Server")
    print(f"MT5Server is running. URI: {uri}")

    try:
        print("MT5Server is ready.")
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("Shutting down MT5Server...")
    finally:
        mt5_server_instance.shutdown()
        daemon.shutdown()

if __name__ == "__main__":
    main()