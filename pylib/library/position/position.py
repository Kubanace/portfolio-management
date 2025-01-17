
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pylib.library.contract.contract import Contract


@dataclass
class Position:
    """Represents a holding of a specific instrument"""
    contract: Contract
    quantity: Decimal
    average_cost: Decimal
    purchase_date: datetime = field(default_factory=datetime.now)

    @property
    def market_value(self, current_price: Optional[Decimal] = None) -> Decimal:
        """
        Calculate current market value of the position

        Args:
            current_price (Optional[Decimal]): Current market price.
            If not provided, assumes last known price.
        """
        if current_price is None:
            # In a real implementation, this would fetch current market price
            raise ValueError("Current price must be provided")
        return self.quantity * current_price

    @property
    def unrealized_pl(self, current_price: Optional[Decimal] = None) -> Decimal:
        """Calculate unrealized profit/loss"""
        return self.market_value(current_price) - (self.quantity * self.average_cost)
