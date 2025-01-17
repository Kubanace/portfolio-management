

import sqlalchemy.orm as orm
from sqlalchemy import create_engine, Column, Date, DateTime, String, Float, Integer

import pylib.library.sql.database as db


class DeclarativeBase:
    """
    A class to regroup all ORM database operations
    """
    def __init__(self):
        self.engine = self._create_engine_()
        self._create_database()

    @staticmethod
    def _create_engine_():
        return create_engine("mssql+pyodbc:///?odbc_connect={}".format(db.CONNECTION_PARAMETERS))

    def _create_database(self):
        db.Base.metadata.create_all(self.engine)

    def make_session(self):
        return orm.sessionmaker(bind=self.engine)()
