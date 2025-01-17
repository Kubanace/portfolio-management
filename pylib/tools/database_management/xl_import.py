
import xlwings as xw
from pylib.library.config.paths import PROJECT_TOOLS_DIR
from sqlalchemy.orm.session import Session

from pylib.library.excel.xl_instrument_handler import ExcelInstrumentHandler
from pylib.library.excel.xl_issuer_handler import ExcelIssuerHandler

WORKBOOK_PATH = PROJECT_TOOLS_DIR + r"\data_loader.xlsx"
TABLES = {
    "Instrument": "instrument_table",
    "Issuer": "issuer_table",
    "Portfolio": "portfolio_table",
}

session = Session()


def main(session: Session):

    # Check which tables are non-empty

    # Process instrument data if available

    # Process issuer data if available

    # Process portfolio data if available

    instrument_handler = ExcelInstrumentHandler(session)


def _is_empty(table):
    data = table.data_body_range.value
    if data is None:
        return True