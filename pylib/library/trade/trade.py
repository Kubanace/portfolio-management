
from datetime import date
import sqlalchemy.orm as orm
import pylib.library.sql.sql as sql
from pylib.library.sql.querier_trade import QuerierTrade


class Trade(object):
    def __init__(self,
                 session: orm.session.Session,
                 trade_id: int = None,
                 # trade_date: date = None,
                 # instrument_id: int = None,
                 # settlement_date: date = None,
                 new_trade_dict: dict = None,
                 ):
        self.querier = QuerierTrade()
        if not trade_id:
            if not new_trade_dict:
                raise TypeError("In the absence of a trade ID, a new trade must be entered")
            # self.trade_id = self.generate_new_trade_id(session)
            # self.trade_date = trade_date
            # self.instrument_id = instrument_id
            # self.settlement_date = settlement_date
            self.trade_id = self.generate_new_trade_id(session)
            self.trade_data = new_trade_dict
        else:
            self.trade_id = trade_id
            self.trade_data = self.querier.get_trade_from_book(self.trade_id, session)

        # if trade_id:
        #
        # else:
        #     if trade_date and instrument_id and settlement_date:
        #         self.trade_id = self.generate_new_trade_id(session)
        #     else:
        #         raise TypeError("Either a trade ID or an instrument ID with trade and settlement dates must be provided")
        #
        # if not instrument_id:
        #     self.instrument_id = self.querier.get_trade_from_book(trade_id=trade_id)

    def generate_new_trade_id(
            self,
            session: orm.session.Session
    ):
        return self.querier.get_last_trade_id(session) + 1



