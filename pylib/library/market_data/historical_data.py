
from datetime import datetime
from decimal import Decimal

from dataclasses import dataclass


@dataclass
class HistoricalBar:
    """Represents a single historical data bar"""
    timestamp: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    weighted_avg_price: Decimal
    bar_count: int