
from xlwings import Book
import uuid
from datetime import date
from typing import Dict, List, Optional
from sqlalchemy.orm.session import Session
from pandas import DataFrame

from pylib.library.sql.declarative_base import DeclarativeBase
from pylib.library.sql.querier_instrument import QuerierInstrument
from pylib.library.sql.querier_instrument_classification import QuerierInstrumentClassification
from pylib.library.excel.xl_issuer_handler import ExcelIssuerHandler
from pylib.library.config.paths import XL_IMPORT_TOOL_PATH


class ExcelInstrumentHandler:
    """Handles operations between Excel workbook and instrument database"""

    def __init__(self
                 # session: Session,
                 ):
        """
        Initialize the handler with Excel workbook details
        """
        self.workbook_path = XL_IMPORT_TOOL_PATH
        self.sheet_name = "Instrument"
        self.data_table_name = "instrument_table"
        self.db = DeclarativeBase()
        self.querier = QuerierInstrument()
        # self.session = session

        # Define Excel table structure
        self.required_columns = [
            "InstrumentCode", "InstrumentName", "ClassificationID",
            "Exchange", "Currency", "IssuerName"
        ]

    def _get_excel_data(self) -> List[Dict]:
        """Read instrument data from Excel worksheet"""
        try:
            wb = Book(self.workbook_path)
            sheet = wb.sheets[self.sheet_name]
            data_table = sheet.tables[self.data_table_name]

            # Get data range (assuming headers are in row 1)
            headers = data_table.header_row_range.value
            data = data_table.data_body_range.value

            # Validate headers
            missing_cols = set(self.required_columns) - set(headers)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")

            # Convert to list of dictionaries
            records = []
            for row in data:
                if any(cell is not None for cell in row):  # Skip completely empty rows
                    record = dict(zip(headers, row))
                    records.append(record)

            return records

        except Exception as e:
            raise Exception(f"Error reading Excel data: {str(e)}")

    def _prepare_instrument_data(self, excel_record: Dict, existing_record: Optional[Dict] = None) -> Dict:
        """
        Convert Excel record to database format

        Args:
            excel_record: Record from Excel
            existing_record: Existing database record if updating
        """

        # Map classification ID to levels
        # classification_mapping = {
        #     "01-01-01": ("Equity", "Common Stock", "Domestic Common Stock"),
        #     "01-01-02": ("Equity", "Common Stock", "Foreign Common Stock"),
        #     # Add more mappings as needed
        # }
        classification_mapping = self._load_classification_mapping()

        classification_id = excel_record["ClassificationID"]
        if classification_id not in classification_mapping:
            raise ValueError(f"Invalid classification ID: {classification_id}. ")
        instrument_id = existing_record['instrument_id'] if existing_record else str(uuid.uuid4())
        level1, level2, level3 = classification_mapping[classification_id]

        return {
            "instrument_sk": None,  # Will be assigned by database
            "instrument_id": existing_record['instrument_id'] if existing_record else str(uuid.uuid4()),
            "instrument_code": excel_record["InstrumentCode"],
            "instrument_name": excel_record["InstrumentName"],
            "classification_id": classification_id,
            "classification_level_1": level1,
            "classification_level_2": level2,
            "classification_level_3": level3,
            "exchange_code": excel_record["Exchange"],
            "currency_code": excel_record["Currency"],
            "issuer_sk": self._get_or_create_issuer_sk(excel_record["IssuerName"]),
            "is_active": True,
            "valid_from_date": date.today(),
            "valid_to_date": None
        }

    def _load_classification_mapping(self):
        """Load classification mappings from dim_instrument_classification table"""
        try:
            # Using hypothetical QuerierInstrumentClassification
            classification_querier = QuerierInstrumentClassification()
            classifications = classification_querier.get_all_classifications(self.session)

            # Convert to mapping dictionary
            mappings = {
                c['classification_id']: (
                    c['classification_level_1'],
                    c['classification_level_2'],
                    c['classification_level_3']
                ) for c in classifications
            }
            return mappings
        except Exception as e:
            raise Exception(f"Error loading classification mappings from database: {str(e)}")

    def _get_or_create_issuer_sk(self, issuer_name: str) -> int:
        """Get issuer surrogate key or create new issuer if doesn't exist

    Args:
        issuer_name: Name of the issuer

    Returns:
        int: Surrogate key of the issuer
    """
        try:
            issuer_handler = ExcelIssuerHandler()
            return issuer_handler.get_or_create_issuer_sk(issuer_name)
        except Exception as e:
            raise Exception(f"Error processing issuer {issuer_name}: {str(e)}")

    def sync_to_database(self) -> Dict[str, int]:
        """
        Synchronize Excel data with database

        Returns:
            Dict with counts of records added/updated
        """
        excel_records = self._get_excel_data()
        session = self.db.make_session()

        try:
            stats = {"added": 0, "updated": 0, "errors": 0}

            for record in excel_records:
                try:
                    # Check if instrument already exists
                    existing_instruments = self.querier.search_instruments(
                        session,
                        instrument_code=record["InstrumentCode"]
                    )
                    existing = existing_instruments[0] if existing_instruments else None

                    instrument_data = self._prepare_instrument_data(record, existing)

                    if existing:
                        # Update existing record
                        self.querier.update_instrument(
                            session,
                            existing["instrument_id"],
                            instrument_data
                        )
                        stats["updated"] += 1
                    else:
                        # Add new record
                        self.querier.add_instrument(session, instrument_data)
                        stats["added"] += 1

                except Exception as e:
                    print(f"Error processing record {record}: {str(e)}")
                    stats["errors"] += 1

            session.commit()
            return stats

        except Exception as e:
            session.rollback()
            raise Exception(f"Error syncing to database: {str(e)}")

        finally:
            session.close()

    def validate_excel_data(self) -> List[str]:
        """
        Validate Excel data before syncing

        Returns:
            List of validation error messages
        """
        errors = []
        excel_records = self._get_excel_data()

        for i, record in enumerate(excel_records, start=2):  # Start at 2 to account for header row - TO CORRECT
            # Check for required fields
            for field in self.required_columns:
                if not record.get(field):
                    errors.append(f"Row {i}: Missing {field}")

            # Validate classification ID format
            if record.get("ClassificationID"):
                if not record["ClassificationID"].match(r"^\d{2}-\d{2}-\d{2}$"):
                    errors.append(f"Row {i}: Invalid ClassificationID format")

            # Validate currency code
            if record.get("Currency"):
                if len(record["Currency"]) != 3:
                    errors.append(f"Row {i}: Currency must be 3 characters")

        return errors
