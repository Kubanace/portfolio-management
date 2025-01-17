
import sqlalchemy.orm as orm
from typing import Optional, Dict, List
from datetime import date
from uuid import UUID

from pylib.library.instrument.instrument import Instrument
# from pylib.library.sql.database import InstrumentData
from pylib.library.sql.database import DimInstrument, DimIssuer


class QuerierInstrument:
    """
    Class to handle all instrument database operations
    """
    # def __init__(self, instrument: Optional[Instrument] = None):
    #     self.instrument = instrument

    # @staticmethod
    # def get_instrument_data(session: orm.session.Session,
    #                         instrument_id: str
    #                         ) -> Optional[Dict]:
    #     """
    #     Retrieve instrument data from the database
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         instrument_id: ID of the instrument to lookup
    #
    #     Returns:
    #         Dictionary containing instrument data or None if not found
    #     """
    #     _query_filter = [
    #         InstrumentData.instrument_id == instrument_id
    #     ]
    #
    #     _query = session.query(InstrumentData).filter(*_query_filter).first()
    #     if not _query:
    #         return None
    #
    #     return {column.name: getattr(_query, column.name) for column in InstrumentData.__table__.columns}
    #
    # @staticmethod
    # def get_all_instruments(session: orm.session.Session) -> List[Dict]:
    #     """
    #     Retrieve all instruments from the database
    #
    #     Args:
    #         session: SQLAlchemy session object
    #
    #     Returns:
    #         List of dictionaries containing instrument data
    #     """
    #     query = session.query(InstrumentData).all()
    #     return [
    #         {column.name: getattr(record, column.name)
    #          for column in InstrumentData.__table__.columns}
    #         for record in query
    #     ]
    #
    # @staticmethod
    # def add_instrument(
    #         session: orm.session.Session,
    #         instrument_data: Dict
    # ) -> bool:
    #     """
    #     Add a new instrument to the database
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         instrument_data: Dictionary containing instrument data
    #
    #     Returns:
    #         Boolean indicating success
    #     """
    #     try:
    #         new_instrument = InstrumentData(**instrument_data)
    #         session.add(new_instrument)
    #         session.commit()
    #         return True
    #     except Exception as e:
    #         session.rollback()
    #         raise Exception(f"Failed to add instrument: {str(e)}")
    #
    # @staticmethod
    # def update_instrument(
    #         session: orm.session.Session,
    #         instrument_id: str,
    #         update_data: Dict
    # ) -> bool:
    #     """
    #     Update an existing instrument in the database
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         instrument_id: ID of the instrument to update
    #         update_data: Dictionary containing fields to update
    #
    #     Returns:
    #         Boolean indicating success
    #     """
    #     try:
    #         session.query(InstrumentData) \
    #             .filter(InstrumentData.instrument_id == instrument_id) \
    #             .update(update_data)
    #         session.commit()
    #         return True
    #     except Exception as e:
    #         session.rollback()
    #         raise Exception(f"Failed to update instrument: {str(e)}")
    #
    # @staticmethod
    # def delete_instrument(
    #         session: orm.session.Session,
    #         instrument_id: str
    # ) -> bool:
    #     """
    #     Delete an instrument from the database
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         instrument_id: ID of the instrument to delete
    #
    #     Returns:
    #         Boolean indicating success
    #     """
    #     try:
    #         session.query(InstrumentData) \
    #             .filter(InstrumentData.instrument_id == instrument_id) \
    #             .delete()
    #         session.commit()
    #         return True
    #     except Exception as e:
    #         session.rollback()
    #         raise Exception(f"Failed to delete instrument: {str(e)}")
    #
    # def search_instruments(self,
    #                        session: orm.session.Session,
    #                        **search_criteria) -> List[Dict]:
    #     """
    #     Search for instruments based on various criteria
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         **search_criteria: Keyword arguments for search filters
    #
    #     Returns:
    #         List of matching instruments
    #     """
    #     query = session.query(InstrumentData)
    #
    #     for field, value in search_criteria.items():
    #         if hasattr(InstrumentData, field):
    #             query = query.filter(getattr(InstrumentData, field) == value)
    #
    #     results = query.all()
    #     return [
    #         {column.name: getattr(record, column.name)
    #          for column in InstrumentData.__table__.columns}
    #         for record in results
    #     ]
    #
    # @staticmethod
    # def bulk_insert_instruments(
    #         session: orm.session.Session,
    #         instruments_data: List[Dict]
    # ) -> bool:
    #     """
    #     Insert multiple instruments at once
    #
    #     Args:
    #         session: SQLAlchemy session object
    #         instruments_data: List of dictionaries containing instrument data
    #
    #     Returns:
    #         Boolean indicating success
    #     """
    #     try:
    #         session.bulk_insert_mappings(InstrumentData, instruments_data)
    #         session.commit()
    #         return True
    #     except Exception as e:
    #         session.rollback()
    #         raise Exception(f"Failed to bulk insert instruments: {str(e)}")

    #############################
    @staticmethod
    def get_instrument_data(
            session: orm.session.Session,
            instrument_id: UUID
    ) -> Optional[Dict]:
        """
        Retrieve instrument data from the dimensional model

        Args:
            session: SQLAlchemy session object
            instrument_id: UUID of the instrument to lookup

        Returns:
            Dictionary containing instrument data or None if not found
        """
        query = session.query(DimInstrument).filter(
            DimInstrument.instrument_id == instrument_id,
            DimInstrument.is_active == 1
        ).first()

        if not query:
            return None

        result = {column.name: getattr(query, column.name)
                  for column in DimInstrument.__table__.columns}

        # Add issuer information
        result.update({
            'issuer_name': query.parent.issuer_name,
            'issuer_country': query.parent.country_code,
            'issuer_sector': query.parent.sector_name,
            'credit_rating': query.parent.credit_rating
        })

        return result

    @staticmethod
    def get_all_active_instruments(session: orm.session.Session) -> List[Dict]:
        """
        Retrieve all active instruments from the dimensional model

        Args:
            session: SQLAlchemy session object

        Returns:
            List of dictionaries containing instrument data
        """
        current_date = date.today()

        query = session.query(DimInstrument).join(DimIssuer) \
            .filter(DimInstrument.is_active == 1,
                    DimInstrument.valid_from_date <= current_date,
                    (DimInstrument.valid_to_date >= current_date) |
                    (DimInstrument.valid_to_date.is_(None))) \
            .all()

        return [{
            **{column.name: getattr(record, column.name)
               for column in DimInstrument.__table__.columns},
            'issuer_name': record.parent.issuer_name,
            'issuer_country': record.parent.country_code,
            'issuer_sector': record.parent.sector_name,
            'credit_rating': record.parent.credit_rating
        } for record in query]

    @staticmethod
    def add_instrument(session: orm.session.Session,
                       instrument_data: Dict) -> bool:
        """
        Add a new instrument to the dimensional model

        Args:
            session: SQLAlchemy session object
            instrument_data: Dictionary containing instrument data including issuer_sk

        Returns:
            Boolean indicating success
        """
        try:
            # Ensure required fields are present
            required_fields = ['instrument_id', 'instrument_code', 'instrument_name',
                               'classification_id', 'classification_level_1',
                               'classification_level_2', 'classification_level_3',
                               'issuer_sk']

            if not all(field in instrument_data for field in required_fields):
                raise ValueError("Missing required fields")

            # Set valid_from_date if not provided
            if 'valid_from_date' not in instrument_data:
                instrument_data['valid_from_date'] = date.today()

            # Set is_active if not provided
            if 'is_active' not in instrument_data:
                instrument_data['is_active'] = 1

            new_instrument = DimInstrument(**instrument_data)
            session.add(new_instrument)
            session.commit()
            return True

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to add instrument: {str(e)}")

    @staticmethod
    def update_instrument(
            session: orm.session.Session,
                          instrument_id: UUID,
                          update_data: Dict) -> bool:
        """
        Update an existing instrument in the dimensional model

        Args:
            session: SQLAlchemy session object
            instrument_id: UUID of the instrument to update
            update_data: Dictionary containing fields to update

        Returns:
            Boolean indicating success
        """
        try:
            # If deactivating the instrument, set valid_to_date
            if update_data.get('is_active') == 0 and 'valid_to_date' not in update_data:
                update_data['valid_to_date'] = date.today()

            session.query(DimInstrument) \
                .filter(DimInstrument.instrument_id == instrument_id) \
                .update(update_data)
            session.commit()
            return True

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to update instrument: {str(e)}")

    @staticmethod
    def deactivate_instrument(session: orm.session.Session,
                              instrument_id: UUID) -> bool:
        """
        Deactivate an instrument (soft delete)

        Args:
            session: SQLAlchemy session object
            instrument_id: UUID of the instrument to deactivate

        Returns:
            Boolean indicating success
        """
        try:
            current_date = date.today()

            session.query(DimInstrument) \
                .filter(DimInstrument.instrument_id == instrument_id) \
                .update({
                'is_active': 0,
                'valid_to_date': current_date
            })
            session.commit()
            return True

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to deactivate instrument: {str(e)}")

    def search_instruments(self,
                           session: orm.session.Session,
                           **search_criteria) -> List[Dict]:
        """
        Search for instruments based on various criteria

        Args:
            session: SQLAlchemy session object
            **search_criteria: Keyword arguments for search filters

        Returns:
            List of matching instruments
        """
        query = session.query(DimInstrument).join(DimIssuer)

        # Handle instrument fields
        for field, value in search_criteria.items():
            if hasattr(DimInstrument, field):
                query = query.filter(getattr(DimInstrument, field) == value)
            elif hasattr(DimIssuer, field):
                query = query.filter(getattr(DimIssuer, field) == value)

        # By default, only return active instruments
        if 'is_active' not in search_criteria:
            query = query.filter(DimInstrument.is_active == 1)

        results = query.all()
        return [{
            **{column.name: getattr(record, column.name)
               for column in DimInstrument.__table__.columns},
            'issuer_name': record.parent.issuer_name,
            'issuer_country': record.parent.country_code,
            'issuer_sector': record.parent.sector_name,
            'credit_rating': record.parent.credit_rating
        } for record in results]

    @staticmethod
    def bulk_insert_instruments(session: orm.session.Session,
                                instruments_data: List[Dict]) -> bool:
        """
        Insert multiple instruments at once

        Args:
            session: SQLAlchemy session object
            instruments_data: List of dictionaries containing instrument data

        Returns:
            Boolean indicating success
        """
        try:
            # Set default values for any missing fields
            current_date = date.today()
            for data in instruments_data:
                if 'valid_from_date' not in data:
                    data['valid_from_date'] = current_date
                if 'is_active' not in data:
                    data['is_active'] = 1

            session.bulk_insert_mappings(DimInstrument, instruments_data)
            session.commit()
            return True

        except Exception as e:
            session.rollback()
            raise Exception(f"Failed to bulk insert instruments: {str(e)}")
