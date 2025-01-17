
import uuid
from datetime import date
from typing import Dict, List, Optional
from sqlalchemy.orm.session import Session
from sqlalchemy import and_
import xlwings as xw

from pylib.library.sql.declarative_base import DeclarativeBase
from pylib.library.sql.database import DimPortfolio
from pylib.library.config.paths import PROJECT_TOOLS_DIR

WORKBOOK_PATH = PROJECT_TOOLS_DIR + r"\data_loader.xlsx"


class ExcelPortfolioHandler:
    """Handles operations between Excel workbook and portfolio database"""

    def __init__(self, session: Session):
        """
        Initialize the handler with Excel workbook details and database session

        Args:
            session: SQLAlchemy session object
        """
        self.workbook_path = WORKBOOK_PATH
        self.sheet_name = "Portfolio"
        self.data_table_name = "portfolio_table"
        self.db = DeclarativeBase()
        self.session = session

        # Define Excel table structure
        self.required_columns = [
            "PortfolioCode",
            "PortfolioName",
            "PortfolioType",
            "PortfolioStatus",
            "BaseCurrency",
            "InceptionDate",
            "ManagerID"
        ]

    def _get_excel_data(self) -> List[Dict]:
        """
        Read portfolio data from Excel worksheet

        Returns:
            List of dictionaries containing portfolio data

        Raises:
            ValueError: If required columns are missing
            Exception: For other Excel-related errors
        """
        try:
            wb = xw.Book(self.workbook_path)
            sheet = wb.sheets[self.sheet_name]
            data_table = sheet.tables[self.data_table_name]

            # Get data range
            headers = data_table.header_row_range.value
            data = data_table.data_body_range.value

            # Validate headers
            missing_cols = set(self.required_columns) - set(headers)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")

            # Convert to list of dictionaries
            records = []
            for row in data:
                if any(cell is not None for cell in row):  # Skip empty rows
                    record = dict(zip(headers, row))
                    records.append(record)

            return records

        except Exception as e:
            raise Exception(f"Error reading Excel data: {str(e)}")

    def get_active_portfolio(self, portfolio_code: str) -> Optional[DimPortfolio]:
        """
        Retrieve active portfolio by code

        Args:
            portfolio_code: Code of the portfolio to look up

        Returns:
            Optional[DimPortfolio]: Active portfolio record if found
        """
        return self.session.query(DimPortfolio).filter(
            and_(
                DimPortfolio.portfolio_code == portfolio_code,
                DimPortfolio.is_active == 1
            )
        ).first()

    def _prepare_portfolio_data(self, excel_record: Dict, existing_record: Optional[Dict] = None) -> Dict:
        """
        Convert Excel record to database format

        Args:
            excel_record: Record from Excel
            existing_record: Existing database record if updating

        Returns:
            Dict containing formatted portfolio data
        """
        return {
            "portfolio_sk": None,  # Will be assigned by database
            "portfolio_id": existing_record['portfolio_id'] if existing_record else str(uuid.uuid4()),
            "portfolio_code": excel_record["PortfolioCode"],
            "portfolio_name": excel_record["PortfolioName"],
            "portfolio_type": excel_record["PortfolioType"],
            "portfolio_status": excel_record["PortfolioStatus"],
            "base_currency_code": excel_record["BaseCurrency"],
            "inception_date": excel_record["InceptionDate"],
            "manager_id": excel_record["ManagerID"],
            "is_active": True,
            "valid_from_date": date.today(),
            "valid_to_date": None
        }

    def add_new_portfolio(self, portfolio_data: Dict) -> DimPortfolio:
        """
        Create a new portfolio record

        Args:
            portfolio_data: Dictionary containing portfolio data

        Returns:
            DimPortfolio: Newly created portfolio record

        Raises:
            Exception: If creation fails
        """
        try:
            new_portfolio = DimPortfolio(**portfolio_data)
            self.session.add(new_portfolio)
            self.session.flush()
            return new_portfolio

        except Exception as e:
            self.session.rollback()
            raise Exception(f"Failed to create new portfolio: {str(e)}")

    def update_portfolio(self, portfolio_sk: int, update_data: Dict) -> bool:
        """
        Update an existing portfolio using SCD Type 2

        Args:
            portfolio_sk: Surrogate key of the portfolio
            update_data: Dictionary containing fields to update

        Returns:
            bool: Success status
        """
        try:
            # Get current active record
            current_portfolio = self.session.query(DimPortfolio).filter(
                and_(
                    DimPortfolio.portfolio_sk == portfolio_sk,
                    DimPortfolio.is_active == 1
                )
            ).first()

            if not current_portfolio:
                raise ValueError(f"No active portfolio found with SK: {portfolio_sk}")

            # Set end date for current record
            current_portfolio.is_active = 0
            current_portfolio.valid_to_date = date.today()

            # Create new active record
            new_portfolio = DimPortfolio(
                portfolio_id=current_portfolio.portfolio_id,  # Keep same natural key
                portfolio_code=current_portfolio.portfolio_code,
                portfolio_name=update_data.get('portfolio_name', current_portfolio.portfolio_name),
                portfolio_type=update_data.get('portfolio_type', current_portfolio.portfolio_type),
                portfolio_status=update_data.get('portfolio_status', current_portfolio.portfolio_status),
                base_currency_code=update_data.get('base_currency_code', current_portfolio.base_currency_code),
                inception_date=update_data.get('inception_date', current_portfolio.inception_date),
                manager_id=update_data.get('manager_id', current_portfolio.manager_id),
                is_active=1,
                valid_from_date=date.today(),
                valid_to_date=None
            )

            self.session.add(new_portfolio)
            return True

        except Exception as e:
            self.session.rollback()
            raise Exception(f"Failed to update portfolio: {str(e)}")

    def validate_excel_data(self) -> List[str]:
        """
        Validate Excel data before syncing

        Returns:
            List of validation error messages
        """
        errors = []
        excel_records = self._get_excel_data()

        for i, record in enumerate(excel_records, start=2):
            # Check for required fields
            for field in self.required_columns:
                if not record.get(field):
                    errors.append(f"Row {i}: Missing {field}")

            # Validate currency code
            if record.get("BaseCurrency"):
                if len(record["BaseCurrency"]) != 3:
                    errors.append(f"Row {i}: BaseCurrency must be 3 characters")

            # Validate portfolio type
            valid_types = ["Individual", "Institution", "Fund"]
            if record.get("PortfolioType") and record["PortfolioType"] not in valid_types:
                errors.append(f"Row {i}: Invalid PortfolioType. Must be one of {valid_types}")

            # Validate portfolio status
            valid_statuses = ["Active", "Closed", "Suspended"]
            if record.get("PortfolioStatus") and record["PortfolioStatus"] not in valid_statuses:
                errors.append(f"Row {i}: Invalid PortfolioStatus. Must be one of {valid_statuses}")

        return errors

    def sync_to_database(self) -> Dict[str, int]:
        """
        Synchronize Excel data with database

        Returns:
            Dict with counts of records added/updated
        """
        excel_records = self._get_excel_data()

        try:
            stats = {"added": 0, "updated": 0, "errors": 0}

            for record in excel_records:
                try:
                    # Check if portfolio already exists
                    existing_portfolio = self.get_active_portfolio(record["PortfolioCode"])

                    portfolio_data = self._prepare_portfolio_data(
                        record,
                        existing_portfolio.__dict__ if existing_portfolio else None
                    )

                    if existing_portfolio:
                        # Update existing record
                        self.update_portfolio(
                            existing_portfolio.portfolio_sk,
                            portfolio_data
                        )
                        stats["updated"] += 1
                    else:
                        # Add new record
                        self.add_new_portfolio(portfolio_data)
                        stats["added"] += 1

                except Exception as e:
                    print(f"Error processing record {record}: {str(e)}")
                    stats["errors"] += 1

            self.session.commit()
            return stats

        except Exception as e:
            self.session.rollback()
            raise Exception(f"Error syncing to database: {str(e)}")
