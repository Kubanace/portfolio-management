
import sqlalchemy.orm as orm
from sqlalchemy import and_
from typing import Optional, Dict, List
from datetime import date
from sqlalchemy.orm.session import Session

from pylib.library.sql.database import DimIssuer


class QuerierIssuer:
    """
    Class to handle all issuer database operations
    """

    @staticmethod
    def get_issuer_by_sk(
            session: Session,
            issuer_sk: int
    ) -> Optional[Dict]:
        """
        Retrieve issuer data by surrogate key

        Args:
            session: SQLAlchemy session object
            issuer_sk: Surrogate key of the issuer

        Returns:
            Dictionary containing issuer data or None if not found
        """
        query = session.query(DimIssuer).filter(DimIssuer.issuer_sk == issuer_sk).first()
        if not query:
            return None

        return {column.name: getattr(query, column.name) for column in DimIssuer.__table__.columns}

    # @staticmethod
    # def get_active_issuer_by_code(
    #         session: orm.session.Session,
    #         issuer_code: str
    # ) -> Optional[Dict]:
    #     """
    #     Retrieve active issuer data by code
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         issuer_code: Business code of the issuer
    #
    #     Returns:
    #         Dictionary containing issuer data or None if not found
    #     """
    #     query = session.query(DimIssuer).filter(
    #         and_(
    #             DimIssuer.issuer_code == issuer_code,
    #             DimIssuer.is_active == 1
    #         )
    #     ).first()
    #
    #     if not query:
    #         return None
    #
    #     return {column.name: getattr(query, column.name) for column in DimIssuer.__table__.columns}

    @staticmethod
    def get_active_issuer_by_name(
            session: Session,
            issuer_name: str
    ) -> Optional[Dict]:
        """
        Retrieve active issuer data by name

        Args:
            session: SQLAlchemy session object
            issuer_name: Name of the issuer

        Returns:
            Dictionary containing issuer data or None if not found
        """
        query = session.query(DimIssuer).filter(
            and_(
                DimIssuer.issuer_name == issuer_name,
                DimIssuer.is_active == 1
            )
        ).first()

        if not query:
            return None

        return {column.name: getattr(query, column.name) for column in DimIssuer.__table__.columns}

    @staticmethod
    def get_all_active_issuers(
            session: Session
    ) -> List[Dict]:
        """
        Retrieve all active issuers from the database

        Args:
            session: SQLAlchemy session object

        Returns:
            List of dictionaries containing issuer data
        """
        query = session.query(DimIssuer).filter(DimIssuer.is_active == 1).all()
        return [
            {column.name: getattr(record, column.name)
             for column in DimIssuer.__table__.columns}
            for record in query
        ]

    @staticmethod
    def add_issuer(
            session: Session,
            issuer_data: Dict
    ) -> bool:
        """
        Add a new issuer to the database

        Args:
            session: SQLAlchemy session object
            issuer_data: Dictionary containing issuer data

        Returns:
            Boolean indicating success

        Raises:
            Exception: If creation fails
        """
        try:
            new_issuer = DimIssuer(**issuer_data)
            session.add(new_issuer)
            session.flush()  # Flush to get the generated issuer_sk
            return True
        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to add issuer: {str(e)}")

    @staticmethod
    def update_issuer(
            session: Session,
            issuer_sk: int,
            update_data: Dict
    ) -> bool:
        """
        Update an existing issuer using SCD Type 2

        Args:
            session: SQLAlchemy session object
            issuer_sk: Surrogate key of the issuer
            update_data: Dictionary containing fields to update

        Returns:
            Boolean indicating success

        Raises:
            Exception: If update fails
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
                issuer_id=update_data.get('issuer_id', current_issuer.issuer_id),
                issuer_code=update_data.get('issuer_code', current_issuer.issuer_code),
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

    @staticmethod
    def deactivate_issuer(
            session: Session,
            issuer_sk: int,
            deactivation_date: Optional[date] = None
    ) -> bool:
        """
        Deactivate an issuer (soft delete)

        Args:
            session: SQLAlchemy session object
            issuer_sk: Surrogate key of the issuer
            deactivation_date: Date when the issuer becomes inactive

        Returns:
            Boolean indicating success
        """
        try:
            current_issuer = session.query(DimIssuer).filter(
                and_(
                    DimIssuer.issuer_sk == issuer_sk,
                    DimIssuer.is_active == 1
                )
            ).first()

            if not current_issuer:
                raise ValueError(f"No active issuer found with SK: {issuer_sk}")

            current_issuer.is_active = 0
            current_issuer.valid_to_date = deactivation_date or date.today()
            return True

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to deactivate issuer: {str(e)}")

    @staticmethod
    def search_issuers(
            session: Session,
            include_inactive: bool = False,
            **search_criteria
    ) -> List[Dict]:
        """
        Search for issuers based on various criteria

        Args:
            session: SQLAlchemy session object
            include_inactive: Whether to include inactive records
            **search_criteria: Keyword arguments for search filters

        Returns:
            List of matching issuers
        """
        query = session.query(DimIssuer)

        # Add active/inactive filter
        if not include_inactive:
            query = query.filter(DimIssuer.is_active == 1)

        # Add search criteria
        for field, value in search_criteria.items():
            if hasattr(DimIssuer, field):
                query = query.filter(getattr(DimIssuer, field) == value)

        results = query.all()
        return [
            {column.name: getattr(record, column.name)
             for column in DimIssuer.__table__.columns}
            for record in results
        ]

    @staticmethod
    def bulk_insert_issuers(
            session: Session,
            issuers_data: List[Dict]
    ) -> bool:
        """
        Insert multiple issuers at once

        Args:
            session: SQLAlchemy session object
            issuers_data: List of dictionaries containing issuer data

        Returns:
            Boolean indicating success
        """
        try:
            session.bulk_insert_mappings(DimIssuer, issuers_data)
            session.flush()
            return True
        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to bulk insert issuers: {str(e)}")