
from xlwings import Book
import uuid
from datetime import date
from typing import Dict, List, Optional
from sqlalchemy import and_
from sqlalchemy.orm import Session

from pylib.library.sql.database import DimIssuer
from pylib.library.sql.declarative_base import DeclarativeBase
from pylib.library.config.paths import XL_IMPORT_TOOL_PATH


class ExcelIssuerHandler:
    """Handles operations related to issuer data in the database"""

    def __init__(self):
        """Initialize the handler with Excel workbook details"""
        self.workbook_path = XL_IMPORT_TOOL_PATH
        self.sheet_name = "Issuer"
        self.data_table_name = "issuer_table"
        self.db = DeclarativeBase()

        # Define Excel table structure
        self.required_columns = [
            "IssuerName", "IssuerType", "Country", "Sector", "SectorName", "CreditRating", "RatingDate",
        ]

    def _get_excel_data(self) -> List[Dict]:
        """Read issuer data from Excel worksheet"""
        try:
            wb = Book(self.workbook_path)
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

    @staticmethod
    def _prepare_issuer_data(
            excel_record: Dict,
            existing_record: Optional[Dict] = None
    ) -> Dict:
        """
        Convert Excel record to database format

        Args:
            excel_record: Record from Excel
            existing_record: Existing database record if updating

        Returns:
            Dict: Prepared issuer data for database
        """
        return {
            "issuer_id": existing_record['issuer_id'] if existing_record else str(uuid.uuid4()),
            "issuer_code": excel_record["IssuerCode"],
            "issuer_name": excel_record["IssuerName"],
            "issuer_type": excel_record["IssuerType"],
            "country_code": excel_record["CountryCode"],
            "sector_code": excel_record.get("SectorCode"),
            "sector_name": excel_record.get("SectorName"),
            "credit_rating": excel_record.get("CreditRating"),
            "is_active": 1,
            "valid_from_date": date.today(),
            "valid_to_date": None
        }

    @staticmethod
    def get_active_issuer(
            session: Session,
            issuer_name: str,
    ) -> Optional[DimIssuer]:
        """
        Retrieve active issuer data by name

        Args:
            session: SQLAlchemy ORM Session
            issuer_name: Name of the issuer to look up

        Returns:
            Optional[DimIssuer]: Active issuer record if found, None otherwise
        """
        return session.query(DimIssuer).filter(
            and_(
                DimIssuer.issuer_name == issuer_name,
                DimIssuer.is_active == 1
            )
        ).first()

    def add_new_issuer(self,
                       session: Session,
                       issuer_name: str,
                       issuer_type: str = "Corporation",
                       country_code: str = "US",
                       sector_code: Optional[str] = None,
                       sector_name: Optional[str] = None,
                       credit_rating: Optional[str] = None
                       ) -> DimIssuer:
        """
        Create a new issuer record

        Args:
            session: SQLAlchemy ORM session
            issuer_name: Name of the issuer
            issuer_type: Type of issuer (e.g., Corporation, Government)
            country_code: ISO country code
            sector_code: Industry sector code
            sector_name: Industry sector name
            credit_rating: Credit rating if applicable

        Returns:
            DimIssuer: Newly created issuer record

        Raises:
            Exception: If creation fails
        """
        new_issuer = DimIssuer(
            issuer_id=str(uuid.uuid4()),
            issuer_code=self._generate_issuer_code(session=session, issuer_name=issuer_name),
            issuer_name=issuer_name,
            issuer_type=issuer_type,
            country_code=country_code,
            sector_code=sector_code,
            sector_name=sector_name,
            credit_rating=credit_rating,
            is_active=1,
            valid_from_date=date.today(),
            valid_to_date=None
        )

        try:
            session.add(new_issuer)
            session.flush()  # Flush to get the generated issuer_sk
            return new_issuer

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to create new issuer: {str(e)}")

    def get_or_create_issuer_sk(self,
                                session: Session,
                                issuer_name: str,
                                issuer_type: Optional[str] = None,  # Default type
                                country_code: Optional[str] = None  # Default country
                                ) -> int:
        """
        Get issuer surrogate key or create new issuer if it doesn't exist

        Args:
            session: SQLAlchemy ORM session
            issuer_name: Name of the issuer
            issuer_type: Type of issuer (e.g., Corporation, Government)
            country_code: ISO country code

        Returns:
            issuer_sk: Surrogate key of the issuer
        """
        # First, try to find an active issuer with the same name
        existing_issuer = self.get_active_issuer(
            issuer_name=issuer_name,
            session=session
        )

        if existing_issuer:
            return existing_issuer.issuer_sk

        # If no existing issuer found, create a new one
        print("Issuer name not found in dim_issuer. Generating new issuer for {} ...".format(issuer_name))
        new_issuer = self.add_new_issuer(
            issuer_name=issuer_name,
            session=session,
            issuer_type=issuer_type,
            country_code=country_code
        )
        return new_issuer.issuer_sk

    # def _generate_issuer_code(
    #         self,
    #         session: Session,
    #         issuer_name: str) -> str:
    #     """
    #     Generate a unique issuer code based on the issuer name
    #
    #     Args:
    #         session: SQLAlchemy ORM session
    #         issuer_name: Name of the issuer
    #
    #     Returns:
    #         str: Generated issuer code
    #     """
    #     # Remove special characters and spaces, take first 10 characters
    #     base_code = ''.join(c for c in issuer_name if c.isalnum()).upper()[:10]
    #
    #     # Check if code exists and append number if needed
    #     suffix = 1
    #     final_code = base_code
    #
    #     while session.query(DimIssuer).filter(
    #             DimIssuer.issuer_code == final_code
    #     ).first():
    #         final_code = f"{base_code}{suffix}"
    #         suffix += 1
    #
    #     return final_code

    @staticmethod
    def update_issuer(
            session: Session,
            issuer_sk: int,
            update_data: dict
    ) -> bool:
        """
        Update an existing issuer using SCD Type 2

        Args:
            session: SQLAlchemy ORM session
            issuer_sk: Surrogate key of the issuer
            update_data: Dictionary containing fields to update

        Returns:
            bool: Success status
        """
        try:
            # Get current active record
            current_issuer = session.query(DimIssuer).filter(
                and_(
                    DimIssuer.issuer_sk == issuer_sk,
                    DimIssuer.is_active == 1
                )
            ).first()

            if not current_issuer:
                raise ValueError(f"No active issuer found with SK: {issuer_sk}")

            # Set end date for current record
            current_issuer.is_active = 0
            current_issuer.valid_to_date = date.today()

            # Create new active record
            new_issuer = DimIssuer(
                issuer_id=current_issuer.issuer_id,  # Keep same natural key
                issuer_code=current_issuer.issuer_code,
                issuer_name=update_data.get('issuer_name', current_issuer.issuer_name),
                issuer_type=update_data.get('issuer_type', current_issuer.issuer_type),
                country_code=update_data.get('country_code', current_issuer.country_code),
                sector_code=update_data.get('sector_code', current_issuer.sector_code),
                sector_name=update_data.get('sector_name', current_issuer.sector_name),
                credit_rating=update_data.get('credit_rating', current_issuer.credit_rating),
                is_active=1,
                valid_from_date=date.today(),
                valid_to_date=None
            )

            session.add(new_issuer)
            return True

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to update issuer: {str(e)}")

    def validate_excel_data(self) -> List[str]:
        """
        Validate Excel data before syncing

        Returns:
            List of validation error messages
        """
        errors = []
        excel_records = self._get_excel_data()

        for i, record in enumerate(excel_records, start=2):  # Start at 2 to account for header row
            # Check required fields
            for field in self.required_columns:
                if field not in ["SectorCode", "SectorName", "CreditRating"]:  # Optional fields
                    if not record.get(field):
                        errors.append(f"Row {i}: Missing required field {field}")

            # Validate country code
            if record.get("CountryCode"):
                if len(record["CountryCode"]) != 2:
                    errors.append(f"Row {i}: CountryCode must be 2 characters")

            # Validate issuer type
            valid_issuer_types = ["Corporation", "Government", "Municipality", "Other"]
            if record.get("IssuerType") and record["IssuerType"] not in valid_issuer_types:
                errors.append(f"Row {i}: Invalid IssuerType. Must be one of {valid_issuer_types}")

            # Validate issuer code format (assuming alphanumeric requirement)
            if record.get("IssuerCode"):
                if not record["IssuerCode"].isalnum():
                    errors.append(f"Row {i}: IssuerCode must be alphanumeric")

        return errors

    def sync_to_database(
            self,
            session: Session
    ) -> Dict[str, int]:
        """
        Synchronize Excel data with database

        Returns:
            Dict with counts of records added/updated/errors
        """
        excel_records = self._get_excel_data()

        try:
            stats = {"added": 0, "updated": 0, "errors": 0}

            for record in excel_records:
                try:
                    # Check if issuer already exists
                    existing_issuer = session.query(DimIssuer).filter(
                        and_(
                            DimIssuer.issuer_name == record["IssuerName"],
                            DimIssuer.is_active == 1
                        )
                    ).first()

                    issuer_data = self._prepare_issuer_data(
                        record,
                        existing_record={"issuer_id": existing_issuer.issuer_id} if existing_issuer else None
                    )

                    if existing_issuer:
                        # Update existing record using SCD Type 2
                        self.update_issuer(session, existing_issuer.issuer_sk, issuer_data)
                        stats["updated"] += 1
                    else:
                        # Add new record
                        new_issuer = DimIssuer(**issuer_data)
                        session.add(new_issuer)
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
