
import sqlalchemy.orm as orm
from sqlalchemy import and_
from typing import Optional, Dict, List
from datetime import date

from pylib.library.sql.database import DimInstrumentClassification


class QuerierInstrumentClassification:
    """
    Class to handle all instrument classification database operations
    """

    @staticmethod
    def get_classification_by_id(
            session: orm.session.Session,
            classification_id: str
    ) -> Optional[Dict]:
        """
        Retrieve a specific classification by ID

        Args:
            session: SQLAlchemy session object
            classification_id: Classification ID to lookup

        Returns:
            Dictionary containing classification data or None if not found
        """
        query = session.query(DimInstrumentClassification).filter(
            DimInstrumentClassification.classification_id == classification_id
        ).first()

        if not query:
            return None

        return {
            column.name: getattr(query, column.name)
            for column in DimInstrumentClassification.__table__.columns
        }

    @staticmethod
    def get_all_classifications(
            session: orm.session.Session,
            include_inactive: bool = False
    ) -> List[Dict]:
        """
        Retrieve all classifications from the database

        Args:
            session: SQLAlchemy session object
            include_inactive: Whether to include inactive classifications

        Returns:
            List of dictionaries containing classification data
        """
        query = session.query(DimInstrumentClassification)

        if not include_inactive:
            query = query.filter(DimInstrumentClassification.is_active == 1)

        return [
            {column.name: getattr(record, column.name)
             for column in DimInstrumentClassification.__table__.columns}
            for record in query.all()
        ]

    @staticmethod
    def get_classifications_by_level(
            session: orm.session.Session,
            level_1: Optional[str] = None,
            level_2: Optional[str] = None,
            level_3: Optional[str] = None
    ) -> List[Dict]:
        """
        Search for classifications by hierarchy levels

        Args:
            session: SQLAlchemy session object
            level_1: First level classification (e.g., "Equity")
            level_2: Second level classification (e.g., "Common Stock")
            level_3: Third level classification (e.g., "Domestic Common Stock")

        Returns:
            List of matching classifications
        """
        filters = []

        if level_1:
            filters.append(DimInstrumentClassification.classification_level_1 == level_1)
        if level_2:
            filters.append(DimInstrumentClassification.classification_level_2 == level_2)
        if level_3:
            filters.append(DimInstrumentClassification.classification_level_3 == level_3)

        query = session.query(DimInstrumentClassification)

        if filters:
            query = query.filter(and_(*filters))

        return [
            {column.name: getattr(record, column.name)
             for column in DimInstrumentClassification.__table__.columns}
            for record in query.all()
        ]

    @staticmethod
    def add_classification(
            session: orm.session.Session,
            classification_data: Dict
    ) -> bool:
        """
        Add a new classification to the database

        Args:
            session: SQLAlchemy session object
            classification_data: Dictionary containing classification data

        Returns:
            Boolean indicating success
        """
        try:
            new_classification = DimInstrumentClassification(**classification_data)
            session.add(new_classification)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to add classification: {str(e)}")

    @staticmethod
    def update_classification(
            session: orm.session.Session,
            classification_id: str,
            update_data: Dict
    ) -> bool:
        """
        Update an existing classification

        Args:
            session: SQLAlchemy session object
            classification_id: ID of the classification to update
            update_data: Dictionary containing fields to update

        Returns:
            Boolean indicating success
        """
        try:
            session.query(DimInstrumentClassification) \
                .filter(DimInstrumentClassification.classification_id == classification_id) \
                .update(update_data)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to update classification: {str(e)}")

    @staticmethod
    def deactivate_classification(
            session: orm.session.Session,
            classification_id: str,
            deactivation_date: Optional[date] = None
    ) -> bool:
        """
        Deactivate a classification (soft delete)

        Args:
            session: SQLAlchemy session object
            classification_id: ID of the classification to deactivate
            deactivation_date: Date when the classification becomes inactive

        Returns:
            Boolean indicating success
        """
        try:
            update_data = {
                "is_active": 0,
                "valid_to_date": deactivation_date or date.today()
            }

            session.query(DimInstrumentClassification) \
                .filter(DimInstrumentClassification.classification_id == classification_id) \
                .update(update_data)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to deactivate classification: {str(e)}")

    @staticmethod
    def validate_classification_hierarchy(
            session: orm.session.Session,
            level_1: str,
            level_2: str,
            level_3: str
    ) -> bool:
        """
        Validate if a classification hierarchy exists and is valid

        Args:
            session: SQLAlchemy session object
            level_1: First level classification
            level_2: Second level classification
            level_3: Third level classification

        Returns:
            Boolean indicating if the hierarchy is valid
        """
        query = session.query(DimInstrumentClassification).filter(
            and_(
                DimInstrumentClassification.classification_level_1 == level_1,
                DimInstrumentClassification.classification_level_2 == level_2,
                DimInstrumentClassification.classification_level_3 == level_3,
                DimInstrumentClassification.is_active == 1
            )
        ).first()

        return query is not None
