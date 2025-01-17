
from pylib.library.instrument.instrument import Instrument
from pylib.library.sql.querier_instrument import QuerierInstrument
from pylib.library.config.enumerations import AssetClass
import pylib.library.sql.declarative_base as dec_b

# instrument = Instrument(
#     # symbol = 'AAPL',
#     # asset_class = AssetClass.EQUITY,
#     # exchange = 'NASDAQ',
#     # currency = 'USD'
# )


# Create a session
declarative_base = dec_b.DeclarativeBase()
session = declarative_base.make_session()

# Initialize querier
querier = QuerierInstrument()
new_instrument = Instrument()

# Add a new instrument
new_instrument_dict = {
    "instrument_id": str(new_instrument.instrument_id),
    "instrument_classification_id": "01-01-01",
    "instrument_description": "TC Energy Corporation",
    "cusip": None,
    "ticker": "TRP-C",
    "issuer_country": "Canada",
    "issuer_sector": "Energy",
    "issuer_industry": "Oil & Gas Midstream",
    "base_currency": "CAD"
}
querier.add_instrument(session, new_instrument_dict)

# Update an instrument
# updates = {"instrument_description": "Updated description"}
# querier.update_instrument(session, "AAPL123", updates)

# Search for instruments
# results = querier.search_instruments(session, base_currency="USD", issuer_sector="Technology")

# Delete an instrument
# querier.delete_instrument(session, "AAPL123")