
import datetime

from sqlalchemy import create_engine, Column, Date, DateTime, String, Float, Integer
from urllib.parse import quote_plus
from pandas import DataFrame

import pylib.library.sql.database as db

# class TradingBook(Base):
#     __tablename__ = 'tb_trading_book'
#     __table_args__ = {'schema': 'dbo'}
#     trade_id = Column(Integer)
#     instrument_id = Column(Integer)
#     trade_date = Column(Date)
#     settlement_date = Column(Date)
#     transaction_type = Column(String(50))
#     asset_class = Column(String(50))
#     symbol = Column(String(50))
#     description = Column(String(255))
#     market = Column(String(3))
#     quantity = Column(Integer)
#     price = Column(Float)
#     price_currency = Column(String(3))
#     commission_paid = Column(Float)
#     settlement_amount = Column(Float)
#     account_currency = Column(String(3))
#     portfolio = Column(String(50))

# class InstrumentData(Base):
#     __tablename__ = 'tb_instrument_data'
#     __table_args__ = {'schema': 'dbo'}
#     instrument_id = Column(Integer)
#     cusip = Column(String(50))
#     instrument_class_1 = Column(String(50))
#     instrument_class_2 = Column(String(50))
#     instrument_class_3 = Column(String(50))
#     instrument_class_4 = Column(String(50))
#     instrument_class_5 = Column(String(50))
#     instrument_desc = Column(String(255))
#     ticker = Column(String(50))
#     issuer_country = Column(String(255))
#     issuer_sector = Column(String(255))
#     issuer_industry = Column(String(255))
#

