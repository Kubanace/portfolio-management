
from enum import Enum, auto


class AssetClass(Enum):
    """Enumeration of different asset classes"""
    EQUITY = auto()
    OPTION = auto()
    FUTURE = auto()
    BOND = auto()
    FOREX = auto()
    CRYPTOCURRENCY = auto()


class OrderType(Enum):
    """Types of orders that can be placed"""
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()
    STOP_LIMIT = auto()
    TRAILING_STOP = auto()


class OrderStatus(Enum):
    """Current status of an order"""
    PENDING = auto()
    SUBMITTED = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    CANCELLED = auto()
    REJECTED = auto()


class DayCountConvention(Enum):
    """Enumeration of supported day count conventions"""
    THIRTY_360 = "30/360"
    ACTUAL_360 = "Actual/360"
    ACTUAL_365 = "Actual/365"
    ACTUAL_ACTUAL = "Actual/Actual"
