

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import (
    Column, Date, DateTime, String, Float, Integer, BigInteger, ForeignKey,
    Numeric, UniqueConstraint, Boolean
)
from urllib.parse import quote_plus
from typing import List
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER, BIT
from datetime import date, datetime
from typing import List, Optional

CONNECTION_PARAMETERS = quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=P14N4S\\SQLEXPRESS;"
    "DATABASE=DataMart;"
    "SCHEMA=dbo;"
    "Trusted_Connection=yes;"
    "Encrypt=no"
)

Base = declarative_base()


# define database models here
# class TradingBook(Base):
#     __tablename__ = 'TradingBook'
#     __table_args__ = {'schema': 'dbo'}
#     trade_id = Column(String(50))
#     instrument_id = Column(String(50))
#     trade_date = Column(DateTime)
#     settlement_date = Column(Date)
#     transaction_type = Column(String(50))
#     symbol = Column(String(50))
#     description = Column(String(None))
#     exchange = Column(String(255))
#     quantity = Column(Float)
#     price = Column(Float)
#     price_currency = Column(String(3))
#     commission_paid = Column(Float)
#     settlement_amount = Column(Float)
#     trade_base_currency = Column(String(3))
#     portfolio = Column(String(50))
#
#
# class InstrumentData(Base):
#     __tablename__ = 'InstrumentData'
#     __table_args__ = {'schema': 'dbo'}
#     instrument_id = Column(String(None), primary_key=True)
#     instrument_classification_id = Column(String(50))
#     instrument_description = Column(String(255))
#     cusip = Column(String(50))
#     ticker = Column(String(50))
#     issuer_country = Column(String(255))
#     issuer_sector = Column(String(255))
#     issuer_industry = Column(String(255))
#     base_currency = Column(String(50))


class DimInstrument(Base):
    """
    SQLAlchemy model for dim_instrument table
    """
    __tablename__ = 'dim_instrument'
    __table_args__ = (
        UniqueConstraint('instrument_id', name='UC_instrument_id'),
        {'schema': 'dbo'}
    )

    # Primary Key
    instrument_sk = Column(BigInteger, primary_key=True)

    # Natural Key and Core Attributes
    instrument_id = Column(UNIQUEIDENTIFIER, nullable=False)
    instrument_code = Column(String(20), nullable=False)
    instrument_name = Column(String(100), nullable=False)

    # Classification
    classification_id = Column(String(8), nullable=False)
    classification_level_1 = Column(String(50), nullable=False)
    classification_level_2 = Column(String(50), nullable=False)
    classification_level_3 = Column(String(50), nullable=False)

    # Additional Attributes
    exchange_code = Column(String(10))
    currency_code = Column(String(3))
    issuer_sk = Column(BigInteger, ForeignKey('dbo.dim_issuer.issuer_sk'), nullable=False)

    # SCD Type 2 Attributes
    is_active = Column(BIT, default=1)
    valid_from_date = Column(Date, nullable=False)
    valid_to_date = Column(Date)

    # Relationships
    issuer = relationship("DimIssuer", back_populates="instruments")
    positions = relationship("FactPosition", back_populates="instrument")
    trades = relationship("FactTrade", back_populates="instrument")


class DimInstrumentClassification(Base):
    """
    SQLAlchemy model for dim_instrument_classification table
    """
    __tablename__ = 'dim_instrument_classification'
    __table_args__ = {'schema': 'dbo'}

    classification_id = Column(String(8), primary_key=True)
    classification_level_1 = Column(String(50), nullable=False)
    classification_level_2 = Column(String(50), nullable=False)
    classification_level_3 = Column(String(50), nullable=False)
    is_active = Column(BIT, default=True)
    valid_from_date = Column(Date, nullable=False)
    valid_to_date = Column(Date)


class DimPortfolio(Base):
    """
    SQLAlchemy model for dim_portfolio table
    """
    __tablename__ = 'dim_portfolio'
    __table_args__ = (
        UniqueConstraint('portfolio_id', name='UC_portfolio_id'),
        {'schema': 'dbo'}
    )

    # Primary Key
    portfolio_sk = Column(BigInteger, primary_key=True)

    # Natural Key and Core Attributes
    portfolio_id = Column(UNIQUEIDENTIFIER, nullable=False)
    portfolio_code = Column(String(50), nullable=False)
    portfolio_name = Column(String(100), nullable=False)
    portfolio_type = Column(String(50), nullable=False)
    portfolio_status = Column(String(20), nullable=False)

    # Additional Attributes
    base_currency_code = Column(String(3), nullable=False)
    inception_date = Column(Date, nullable=False)
    account = Column(String(50))

    # SCD Type 2 Attributes
    is_active = Column(BIT, default=1)
    valid_from_date = Column(Date, nullable=False)
    valid_to_date = Column(Date)

    # Relationships
    positions = relationship("FactPosition", back_populates="portfolio")
    trades = relationship("FactTrade", back_populates="portfolio")


class DimIssuer(Base):
    """
    SQLAlchemy model for dim_issuer table
    """
    __tablename__ = 'dim_issuer'
    __table_args__ = (
        UniqueConstraint('issuer_id', name='UC_issuer_id'),
        {'schema': 'dbo'}
    )

    # Primary Key
    issuer_sk = Column(BigInteger, primary_key=True)

    # Natural Key and Core Attributes
    issuer_id = Column(UNIQUEIDENTIFIER, nullable=False)
    issuer_code = Column(String(20), nullable=False)
    issuer_name = Column(String(100), nullable=False)
    issuer_type = Column(String(50), nullable=False)

    # Additional Attributes
    country_code = Column(String(2), nullable=False)
    sector_code = Column(String(4))
    sector_name = Column(String(50))
    credit_rating = Column(String(10))

    # SCD Type 2 Attributes
    is_active = Column(BIT, default=1)
    valid_from_date = Column(Date, nullable=False)
    valid_to_date = Column(Date)

    # Relationships
    instruments = relationship("DimInstrument", back_populates="issuer")


class DimBroker(Base):
    """
    SQLAlchemy model for dim_broker table
    """
    __tablename__ = 'dim_broker'
    __table_args__ = (
        UniqueConstraint('broker_id', name='UC_broker_id'),
        {'schema': 'dbo'}
    )

    # Primary Key
    broker_sk = Column(BigInteger, primary_key=True)

    # Natural Key and Core Attributes
    broker_id = Column(UNIQUEIDENTIFIER, nullable=False)
    broker_code = Column(String(20), nullable=False)
    broker_name = Column(String(100), nullable=False)
    broker_type = Column(String(50), nullable=False)  # e.g., Full Service, Discount, Prime

    # Additional Attributes
    country_code = Column(String(2), nullable=False)
    regulatory_id = Column(String(50))  # e.g., SEC registration number
    contact_info = Column(String(500))
    fee_schedule_id = Column(String(50))

    # SCD Type 2 Attributes
    is_active = Column(BIT, default=1)
    valid_from_date = Column(Date, nullable=False)
    valid_to_date = Column(Date)

    # Relationships
    trades = relationship("FactTrade", back_populates="broker")


class FactPosition(Base):
    """
    SQLAlchemy model for fact_position table
    """
    __tablename__ = 'fact_position'
    __table_args__ = {'schema': 'dbo'}

    # Composite Primary Key
    position_date = Column(Date, primary_key=True)
    portfolio_sk = Column(BigInteger, ForeignKey('dbo.dim_portfolio.portfolio_sk'), primary_key=True)
    instrument_sk = Column(BigInteger, ForeignKey('dbo.dim_instrument.instrument_sk'), primary_key=True)

    # Measures
    quantity = Column(Numeric(18, 8), nullable=False)
    market_price = Column(Numeric(18, 8), nullable=False)
    market_value = Column(Numeric(18, 2), nullable=False)
    average_cost = Column(Numeric(18, 8), nullable=False)
    book_cost = Column(Numeric(18, 2), nullable=False)
    unrealized_pl = Column(Numeric(18, 2), nullable=False)
    currency_code = Column(String(3), nullable=False)

    # Relationships
    portfolio = relationship("DimPortfolio", back_populates="positions")
    instrument = relationship("DimInstrument", back_populates="positions")


class FactTrade(Base):
    """
    SQLAlchemy model for fact_trade table
    """
    __tablename__ = 'fact_trade'
    __table_args__ = (
        UniqueConstraint('trade_id', name='UC_trade_id'),
        {'schema': 'dbo'}
    )

    # Primary Key
    trade_sk = Column(BigInteger, primary_key=True)

    # Natural Key and Core Attributes
    trade_id = Column(UNIQUEIDENTIFIER, nullable=False)
    trade_date = Column(Date, nullable=False)
    settlement_date = Column(Date, nullable=False)

    # Foreign Keys
    portfolio_sk = Column(BigInteger, ForeignKey('dbo.dim_portfolio.portfolio_sk'), nullable=False)
    instrument_sk = Column(BigInteger, ForeignKey('dbo.dim_instrument.instrument_sk'), nullable=False)
    broker_sk = Column(BigInteger,
                       ForeignKey('dbo.dim_broker.broker_sk'))  # Nullable as some trades might not have a broker

    # Transaction Details
    trade_type = Column(String(20), nullable=False)
    quantity = Column(Numeric(18, 8), nullable=False)
    price = Column(Numeric(18, 8), nullable=False)
    gross_amount = Column(Numeric(18, 2), nullable=False)
    net_amount = Column(Numeric(18, 2), nullable=False)
    commission = Column(Numeric(18, 2))
    fees = Column(Numeric(18, 2))
    currency_code = Column(String(3), nullable=False)
    trade_status = Column(String(20), nullable=False)

    # Relationships
    portfolio = relationship("DimPortfolio", back_populates="trades")
    instrument = relationship("DimInstrument", back_populates="trades")
    broker = relationship("DimBroker", back_populates="trades")
