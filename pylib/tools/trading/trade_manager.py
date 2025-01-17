
import pylib.library.sql.database as db
import pylib.library.sql.declarative_base as dec_b
import pylib.library.sql.sql as sql


def main():
    declarative_base = dec_b.DeclarativeBase()
    session = declarative_base.make_session()
    querier = sql.TradingBookQuerier

    operation = "TRADE_QUERY"

    if operation == "TRADE_QUERY":
        trade_data = querier.get_trade_from_book(trade_id=1, _session=session)
        return trade_data

    return None
