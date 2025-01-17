
from datetime import date
from typing import Optional
from decimal import Decimal
from dataclasses import dataclass

from pylib.library.instrument.instrument import Instrument


@dataclass
class Contract(Instrument):
    """Represents a specific tradable contract"""
    multiplier: Optional[Decimal] = None
    expiration_date: Optional[date] = None
    strike_price: Optional[Decimal] = None

    def is_expired(self) -> bool:
        """Check if the contract has expired"""
        return self.expiration_date and self.expiration_date < date.today()

