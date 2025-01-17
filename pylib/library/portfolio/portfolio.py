

import uuid
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
from decimal import Decimal

from pylib.library.contract.contract import Contract
from pylib.library.position.position import Position
from pylib.library.order.order import Order
from pylib.library.market_data.security_market_data import SecurityMarketData


@dataclass
class Portfolio(object):
    """
    Manages a collection of positions and orders
    """
    name: str
    unique_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    positions: Dict[Contract, Position] = field(default_factory=dict)
    orders: List[Order] = field(default_factory=list)  # Orders history

    # if we want to have cash balance not as position in cash instrument
    # cash_balance: Decimal = Decimal('0')

    def add_position(self, position: Position):
        """Add or update a position"""
        existing_pos = self.positions.get(position.contract)
        if existing_pos:
            # Update average cost and quantity for existing position
            total_quantity = existing_pos.quantity + position.quantity
            weighted_avg_cost = (
                (existing_pos.quantity * existing_pos.average_cost) +
                (position.quantity * position.average_cost)
            ) / total_quantity

            existing_pos.quantity = total_quantity
            existing_pos.average_cost = weighted_avg_cost
        else:
            self.positions[position.contract] = position

    def remove_position(self, contract: Contract, quantity: Decimal):
        """Reduce or remove a position"""
        position = self.positions.get(contract)
        if not position:
            raise ValueError(f"No position found for {contract}")

        if quantity >= position.quantity:
            del self.positions[contract]
        else:
            position.quantity -= quantity

    @property
    def total_market_value(self, market_data: Dict[Contract, SecurityMarketData]) -> Decimal:
        """
        Calculate total portfolio market value

        Args:
            market_data (Dict[Contract, MarketData]): Current market prices
        """
        return sum(
            pos.market_value(market_data[contract].last_price)
            for contract, pos in self.positions.items()
        ) + self.cash_balance

    def add_order(self, order: Order):
        """Add a new order"""
        self.orders.append(order)

    def get_position_by_symbol(self, symbol: str) -> Optional[Position]:
        """Retrieve a position by instrument symbol"""
        return next(
            (pos for pos in self.positions.values()
             if pos.contract.symbol == symbol),
            None
        )

    def __init__(self, portfolio_id):
        self.portfolio = portfolio_id
        self.instruments = list()

    def market_value(self):
        return sum([inst.market_value() for inst in self.instruments])
