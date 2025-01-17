
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass, field

from pylib.library.instrument.instrument import Instrument


@dataclass
class SecurityMarketData:
    """Holds real-time and historical market information of securities"""
    instrument: Instrument
    last_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    volume: int
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def mid_price(self) -> Decimal:
        """Calculate mid-price"""
        return (self.bid_price + self.ask_price) / 2

