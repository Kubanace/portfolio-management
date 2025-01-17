
from typing import Dict, Optional, List
from threading import Event, Thread


class MarketDataManager:
    """
    Manages market data retrieval and storage.
    *** Here market data refers to market price for a stock symbol, to be extended to broader definition ***

    Initiation will create an empty market data carrier to be filled with desired data.
    """
    def __init__(self):
        self.market_data: Dict[str, Dict[str, float]] = {}  # symbol: {tick_type: value}
        self.data_received_event = Event()

    def store_market_data(self, symbol: str, tick_type: str, value: float):
        """
        Store market data for a specific symbol

        Args:
            symbol (str): Stock symbol
            tick_type (str): Type of market data (last, bid, ask)
            value (float): Market data value
        """
        if symbol not in self.market_data:
            self.market_data[symbol] = {}

        self.market_data[symbol][tick_type] = value

    def get_stored_market_data(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        Retrieve market data for a specific symbol from the stored values in self.market_data

        Args:
            symbol (str): Stock symbol

        Returns:
            Optional dictionary of market data
        """
        return self.market_data.get(symbol)
