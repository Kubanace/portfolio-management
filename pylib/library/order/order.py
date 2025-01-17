
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pylib.library.contract.contract import Instrument
from pylib.library.config.enumerations import OrderType, OrderStatus


@dataclass
class Order:
    """Represents a trading order"""
    instrument: Instrument
    order_type: OrderType
    side: str  # 'BUY' or 'SELL'
    quantity: Decimal

    # Pricing parameters
    limit_price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None

    # Tracking and status
    unique_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: OrderStatus = OrderStatus.PENDING
    creation_time: datetime = field(default_factory=datetime.now)
    execution_time: Optional[datetime] = None

    # Additional order parameters
    time_in_force: Optional[str] = None  # e.g., 'GTC', 'DAY'
    parent_order_id: Optional[str] = None

    def update_status(self, new_status: OrderStatus):
        """Update order status with timestamp"""
        self.status = new_status
        if new_status in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
            self.execution_time = datetime.now()
