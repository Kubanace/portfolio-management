import logging
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Thread, Event
from typing import Dict, Optional, List
from decimal import Decimal
from functools import wraps
import time
from datetime import datetime, timedelta

from pylib.library.market_data.market_data_manager import MarketDataManager
from pylib.library.market_data.historical_data import HistoricalBar

TICK_TYPES = {
    4: 'last',
    1: 'bid',
    2: 'ask'
}


def require_connection(f):                                             # f is the original function being decorated
    """
    Decorator to check connection status before executing methods
    """
    @wraps(f)                                                          # This preserves the original function's metadata
    def wrapper(self, *args, **kwargs):                                # This is the function that will wrap around f
        if not self.connected:                                         # Add our check before running f
            raise ConnectionError("Not connected to IBKR")
        return f(self, *args, **kwargs)                                # Call the original function with all args
    return wrapper                                                     # Return our wrapper function


class IBAPIClient(EWrapper, EClient):
    def __init__(self, host='127.0.0.1', port=7497, client_id=1):
        """
        Initialize the IBKR API client

        Args:
            host (str): IBKR TWS/Gateway host
            port (int): Connection port (7496 for live, 7497 for paper trading)
            client_id (int): Unique client identifier
        """
        EWrapper.__init__(self)  # lookup utility
        EClient.__init__(self, self)  # lookup utility

        # Logging setup
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')  # to do: lookup utility of this
        self.logger = logging.getLogger(__name__)

        # Connection parameters
        self.host = host
        self.port = port
        self.client_id = client_id

        # Data storage
        # self.market_data = {}
        # self.account_details = {}

        # Initialize tracking attributes
        self.connected = False
        self.next_req_id = 0
        self.portfolio_positions = {}
        self.market_data_manager = MarketDataManager()

        # Threading events for synchronization
        self.portfolio_update_complete = Event()
        self.market_data_complete = Event()
        self.req_id_to_symbol = {}  # dict to map request IDs to corresponding symbols ({reqId: symbol})

        self.historical_data = {}
        self.historical_data_complete = Event()

    def connect_and_run(self):
        """
        Connect to IBKR and start the client thread
        """
        self.connect(self.host, self.port, self.client_id)

        # Start the socket client in a thread
        api_thread = Thread(target=lambda: self.run(), daemon=True)
        api_thread.start()

        # Wait for connection to establish, uses timeout but can be configured to wait indefinitely
        timeout = 10  # 10 second timeout
        start_time = time.time()
        while not self.connected and time.time() - start_time < timeout:
            time.sleep(0.1)

        if not self.connected:
            raise ConnectionError("Failed to connect to IBKR within timeout period")

        self.logger.info("Successfully connected to IBKR API")

    def disconnect_and_stop(self):
        """
         Disconnect from IBKR.
         Uses EClient.disconnect() inherited method.
         """
        try:
            # Cancel all market data subscriptions
            for req_id in self.req_id_to_symbol.keys():
                self.cancelMktData(req_id)

            # Disconnect using inherited EClient method
            self.disconnect()
            self.connected = False
            self.logger.info("Disconnected from IBKR API")

        except Exception as e:
            self.logger.error(f"Error disconnecting from IBKR: {str(e)}")
            raise

    def nextValidId(self, orderId):
        """
        Callback when connection is established
        """
        self.connected = True
        self.logger.info(f"Connected. Next Valid Order ID: {orderId}")

    def _get_next_req_id(self) -> int:
        """Get next request ID"""
        self.next_req_id += 1
        return self.next_req_id

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=''):
        """
        Error handling method
        """
        self.logger.error(f"Error {errorCode} for request {reqId}: {errorString}")

    @staticmethod
    def create_contract(
            symbol: str,
            sec_type: str = "STK",
            exchange: str = "SMART",
            currency: str = "USD"
    ) -> Contract:
        """
        Create a stock contract for trading

        Args:
            symbol (str): Stock ticker symbol
            sec_type: Security type
            exchange (str): Trading exchange
            currency (str): Trading currency

        Returns:
            Contract: IBKR Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def create_market_order(
            action: str,
            quantity: float,
            contract: Contract
    ):
        """
        Create a market order

        Args:
            action (str): 'BUY' or 'SELL'
            quantity (int): Number of shares
            contract (Contract): Contract to trade - needed to validate quantity rules and to ensure order parameters
                                match contract specs

        Returns:
            Order: IBKR Order object
        """
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = "MKT"
        return order

    def position(
            self,
            account: str,
            contract: Contract,
            position: float,
            avgCost: float
    ):
        """
        Handle incoming position data.
        Inherited from EWrapper to be called by IBAPI for sending position data.
        """
        symbol = contract.symbol
        self.portfolio_positions[symbol] = {
            'position': position,
            'avgCost': avgCost,
            'account': account,
            'contract': contract
        }

    def positionEnd(self):
        """
        Signal completion of position updates.
        Inherited from EWrapper to be called by IBAPI for sending position end event.
        """
        self.portfolio_update_complete.set()

    def historicalData(self, reqId: int, bar) -> None:
        """
        Callback for historical data bars.
        Inherited from EWrapper, called by IBKR API for each historical data bar.
        """
        symbol = self.req_id_to_symbol.get(reqId)
        if symbol not in self.historical_data:
            self.historical_data[symbol] = []

        historical_bar = HistoricalBar(
            timestamp=datetime.strptime(bar.date, '%Y%m%d %H:%M:%S' if len(bar.date) > 8 else '%Y%m%d'),
            open_price=Decimal(str(bar.open)),
            high_price=Decimal(str(bar.high)),
            low_price=Decimal(str(bar.low)),
            close_price=Decimal(str(bar.close)),
            volume=bar.volume,
            weighted_avg_price=Decimal(str(bar.wap)),
            bar_count=bar.barCount
        )

        self.historical_data[symbol].append(historical_bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """
        Callback indicating end of historical data transmission
        """
        self.historical_data_complete.set()
        symbol = self.req_id_to_symbol.get(reqId)
        self.logger.info(f"Historical data complete for {symbol} from {start} to {end}")

    # Market data handling
    def tickPrice(
            self,
            reqId: int,
            tickType: int,
            price: float,
            attrib: dict
    ):
        """
        Handle incoming market data price updates
        """
        symbol = self.req_id_to_symbol.get(reqId)
        if symbol:
            tick_types = {
                4: 'last',
                1: 'bid',
                2: 'ask'
            }

            if tickType in TICK_TYPES:
                self.market_data_manager.store_market_data(
                    symbol, tick_types[tickType], price)
            self.market_data_complete.set()  # set the event of data

    @require_connection
    def request_market_data(
            self,
            symbols: List[str],
            timeout: int = 10,  # timeout feature, to consider
            print_requested_data: bool = False,
    ) -> None:
        """
        Request market data for specified symbols
        """

        # Reset event
        self.market_data_complete.clear()

        try:
            for symbol in symbols:
                contract = self.create_contract(symbol)
                req_id = self._get_next_req_id()
                self.req_id_to_symbol[req_id] = symbol

                # IBAPI method to subscribe to market data and receive a stream of price updates until subscription is
                # canceled by cancelMktData(req_id) (in disconnect_and_stop)
                self.reqMktData(req_id, contract, '', False, False, [])

                # Wait for completion
                if not self.market_data_complete.wait(timeout):
                    self.logger.warning("Timeout waiting for market data")

                if print_requested_data:
                    print({symbol: self.market_data_manager.get_stored_market_data(symbol) for symbol in symbols})

        except Exception as e:
            self.logger.error(f"Error fetching market data: {str(e)}")
            raise

    @require_connection  # probably won't require a connection since we're pulling from stored data in mkt_data_mgr
    def _get_market_data(
            self,
            symbols: List[str]
    ):
        # Return market data for requested symbols
        return {symbol: self.market_data_manager.get_stored_market_data(symbol) for symbol in symbols}

    @require_connection
    def get_portfolio_positions(self, timeout: int = 10) -> Dict[str, Dict]:
        """
        Fetch current portfolio positions

        Args:
            timeout (int): Maximum wait time in seconds

        Returns:
            Dict[str, Dict]: Portfolio positions by symbol
        """
        # Clear previous data and reset event
        self.portfolio_positions.clear()
        self.portfolio_update_complete.clear()

        try:
            # Request positions from IB
            self.reqPositions()

            # Wait for all positions to arrive (positionEnd to be called)
            if not self.portfolio_update_complete.wait(timeout):  # Wait for completion
                self.logger.warning("Timeout waiting for portfolio positions")

            return self.portfolio_positions

        except Exception as e:
            self.logger.error(f"Error fetching portfolio positions: {str(e)}")
            raise

    @require_connection
    def fetch_portfolio_market_data(self, timeout: int = 10) -> Dict[str, Dict]:
        """Fetch portfolio positions with market data"""
        # Get positions first
        positions = self.get_portfolio_positions(timeout)
        if not positions:
            return {}

        # Request market data for all positions
        self.market_data_complete.clear()
        self.request_market_data(list(positions.keys()))

        # Wait for market data
        if not self.market_data_complete.wait(timeout):
            self.logger.warning("Timeout waiting for market data")

        # Combine position and market data
        return self._enrich_portfolio_with_market_data(positions)

    def _enrich_portfolio_with_market_data(self, positions: Dict[str, Dict]) -> Dict[str, Dict]:
        """Combine portfolio positions with market data"""
        enriched_portfolio = {}
        for symbol, position in positions.items():
            market_data = self.market_data_manager.get_stored_market_data(symbol)
            if market_data:
                position['market_data'] = market_data
                last_price = Decimal(str(market_data.get('last', 0)))
                position_size = Decimal(str(position['position']))
                position['current_value'] = last_price * position_size
                enriched_portfolio[symbol] = position
        return enriched_portfolio

    @require_connection
    def request_historical_data(
            self,
            symbol: str,
            end_datetime: Optional[datetime] = None,

            # How far back to go from endDateTime ("1 D", "2 W", "6 M", "1 Y"),
            # Valid units: "S" (seconds), "D" (days), "W" (weeks), "M" (months), "Y" (years)
            # Max: 1 year for 1-second bars, 6 months for 1-minute bars, 1 year for 1-day bars
            duration: str = "1 D",

            # Time span of each bar:
            # 1, 5, 10, 15, 30 secs
            # 1, 2, 3, 5, 10, 15, 20, 30 mins
            # 1, 2, 3, 4, 8 hours
            # 1 day, 1 week, 1 month
            bar_size: str = "1 min",

            # Type of data to retrieve:
            # "TRADES" - Last price and volume
            # "MIDPOINT" - Midpoint between bid and ask
            # "BID" - Bid price
            # "ASK" - Ask price
            # "BID_ASK" - Both bid and ask prices
            # "ADJUSTED_LAST" - Historical prices adjusted for corporate actions
            # "HISTORICAL_VOLATILITY"
            # "OPTION_IMPLIED_VOLATILITY"
            what_to_show: str = "TRADES",

            use_rth: bool = True,
            format_date: bool = True,
            keep_up_to_date: bool = False,
            timeout: int = 60
    ) -> List[HistoricalBar]:
        """
        Request historical data for a symbol

        Args:
            symbol: Stock symbol
            end_datetime: End date/time for the request. None means current time
            duration: Time span of the data. Examples: "1 D", "1 M", "1 Y"
            bar_size: Size of data bars. Examples: "1 min", "1 hour", "1 day"
            what_to_show: Type of data to retrieve (TRADES, MIDPOINT, BID, ASK, etc.)
            use_rth: True for regular trading hours only
            format_date: Format dates as strings
            keep_up_to_date: Keep updating data in real-time after initial history
            timeout: Maximum wait time in seconds

        Returns:
            List of HistoricalBar objects
        """
        try:
            # Clear previous data and reset event
            if symbol in self.historical_data:
                self.historical_data[symbol].clear()
            self.historical_data_complete.clear()

            # Create contract
            contract = self.create_contract(symbol)

            # Generate request ID and store symbol mapping
            req_id = self._get_next_req_id()
            self.req_id_to_symbol[req_id] = symbol

            # Format end datetime
            end_datetime = end_datetime or datetime.now()
            formatted_end = end_datetime.strftime('%Y%m%d %H:%M:%S') + ' EST'

            # Request historical data
            self.reqHistoricalData(
                reqId=req_id,                           # Unique identifier for this request
                contract=contract,                      # Contract object specifying the instrument
                endDateTime=formatted_end,              # The last date/time for the data request
                durationStr=duration,                   # How far back to go from endDateTime
                barSizeSetting=bar_size,                # Size of each data bar
                whatToShow=what_to_show,                # Type of data to retrieve
                useRTH=use_rth,                         # Regular Trading Hours only (1) or all hours (0)
                formatDate=1 if format_date else 2,     # 1 for formatted strings, 2 for UNIX timestamps
                keepUpToDate=keep_up_to_date,           # Continue streaming real-time data after historical
                chartOptions=[]                         # Additional chart options (usually empty list)
            )

            # Wait for completion
            if not self.historical_data_complete.wait(timeout):
                self.logger.warning(f"Timeout waiting for historical data for {symbol}")

            return self.historical_data.get(symbol, [])

        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {str(e)}")
            raise

    def get_daily_historical_data(
            self,
            symbol: str,
            days: int = 30,
            use_rth: bool = True
    ) -> List[HistoricalBar]:
        """
        Convenience method to get daily historical data

        Args:
            symbol: Stock symbol
            days: Number of trading days to retrieve
            use_rth: Use regular trading hours only

        Returns:
            List of HistoricalBar objects
        """
        return self.request_historical_data(
            symbol=symbol,
            duration=f"{days} D",
            bar_size="1 day",
            use_rth=use_rth
        )

    def get_intraday_historical_data(
            self,
            symbol: str,
            minutes: int = 1,
            days_back: int = 1,
            use_rth: bool = True
    ) -> List[HistoricalBar]:
        """
        Convenience method to get intraday historical data

        Args:
            symbol: Stock symbol
            minutes: Bar size in minutes
            days_back: Number of days to look back
            use_rth: Use regular trading hours only

        Returns:
            List of HistoricalBar objects
        """
        return self.request_historical_data(
            symbol=symbol,
            duration=f"{days_back} D",
            bar_size=f"{minutes} min",
            use_rth=use_rth
        )


def main():
    # Example usage
    client = IBAPIClient()

    try:
        # Connect to IBKR
        client.connect_and_run()

        # Fetch portfolio with market data
        portfolio = client.fetch_portfolio_market_data()

        # Keep the script running
        client.logger.info("IBKR API client is running...")
        client.logger.info("Press Ctrl+C to exit")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        client.logger.info("Stopping IBKR API client...")

    except Exception as e:
        client.logger.error(f"Error: {e}")

    finally:
        client.disconnect_and_stop()


if __name__ == "__main__":
    main()
