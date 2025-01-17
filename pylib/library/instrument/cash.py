
from typing import List, Dict, Optional, Union
from pylib.library.instrument.instrument import Instrument


class Cash(Instrument):
    # def __init__(self,
    #              instrument_id: int):
    #     Instrument.__init__(self, instrument_id)
    #     self.reference_data = {}

    # def load_reference_data(self, instrument_id):
    #     _ref_data = {}  # query reference data
    #     self.reference_data = _ref_data

    # def market_value(self, eval_dt):
    #     quantity = self.get_quantity(eval_dt)
    #     price = self.get_price(eval_dt)
    #     return quantity * price
    base_currency: Optional[str] = None
