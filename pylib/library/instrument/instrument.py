
from datetime import date
from abc import ABC, abstractmethod
import uuid
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union

from pylib.library.config.enumerations import AssetClass
from pylib.library.tsir.tsir import Tsir
from pylib.library.utils.schedule import Schedule


@dataclass
class Instrument:
    """Base class for financial instruments"""
    # symbol: str
    # asset_class: AssetClass
    # exchange: str
    # currency: str
    instrument_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Instrument metadata
    instrument_desc: Optional[str] = None
    # base_currency: Optional[str] = None

    def __hash__(self):
        return hash(self.instrument_id)

    def __eq__(self, other):
        if not isinstance(other, Instrument):
            return False
        return self.instrument_id == other.instrument_id
