
from decimal import Decimal
from typing import List, Dict, Optional, Union

from pylib.library.instrument.instrument import Instrument


class Stock(Instrument):
    # def __init__(self,
    #              instrument_id: int):
    #     Instrument.__init__(self, instrument_id)
    #     self.ticker = ""
    #     self.exchange = ""
    #     self.reference_data = {}
    exchange: Optional[str] = None
    base_currency: Optional[str] = None
    ticker: Optional[str] = None

    # def load_reference_data(self, instrument_id):
    #     _ref_data = {}  # query reference data
    #     self.reference_data = _ref_data

    # def market_value(self, eval_dt):
    #     quantity = self.get_quantity(eval_dt)
    #     price = self.get_price(eval_dt)
    #     return quantity*price

    # def get_quantity(self, eval_dt):
    #     qty = 0  # "some function to get qty
    #     return qty

    def get_market_price(self, eval_dt, price_type="close"):
        price = 0  # some function to get price depending on price type
        return price
