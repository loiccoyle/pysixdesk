from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from tables import Base
import tables


class DBHandler:
    def __init__(self, dialect, database, user=None, passwd=None, host=None,
                 port=None):
        """Helper class for handling the database.

        Args:
            dialect (str): Type of sql database, either "sqlite" or "mysql".
            database (str): Either the path to the databse, in the
            case of "sqlite" db, or db name in the case of "mysql".
            user (str, optional): Username, required for "mysql" db.
            passwd (str, optional): Password, required for "mysql" db.
            host (str, optional): Host, required for "mysql" db.
            port (str, optional): Port, required for "mysql" db.

        Raises:
            ValueError: If "dialect" is not "sqlite" or "mysql".
        """
        if dialect not in ['sqlite', 'mysql']:
            raise ValueError('"dialect" must be either "sqlite" or "mysql"')

        self.dialect = dialect
        self.database = database
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.engine = self._get_engine()

    def _get_engine(self):
        """Creates an sqlalchemy engine.

        Returns:
            sqlalchemy.engine.base.Engine: sql engine.
        """
        if self.dialect == 'sqlite':
            engine = create_engine(f'{self.dialect}:///{self.database}')
        elif self.dialect == 'mysql':
            # create database
            mysql_engine = create_engine(f'{self.dialect}://{self.user}:{self.passwd}@{self.host}:{self.port}')
            mysql_engine.execute(f"CREATE DATABASE IF NOT EXISTS {self.database} ")
            # create engine
            engine = create_engine(f'{self.dialect}://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.database}')
        return engine

    def init_tables(self, tables=None):
        """Initializes the tables. Will lock in the tables' columns.
        """
        Base.metadata.create_all(self.engine, tables=tables)

    def session(self, **kwargs):
        """Creates a session bound to the engine.

        Args:
            **kwargs: Forwarded to sqlalchemy.sessionmaker

        Returns:
            sqlalchemy.orm.session.Session: Bound session.
        """
        Session = sessionmaker(bind=self.engine, **kwargs)
        return Session()

    @contextmanager
    def session_scope(self, **kwargs):
        """Session context manager, rolls back if exception is raised and
        commits on leave.

        Args:
            **kwargs: Forwared to sqlalchemy.sessionmaker
        """
        session = self.session(**kwargs)
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def add(self, table_entry, **kwargs):
        """Helper function to quickly add a table entry.

        Args:
            table_entry (mapped classes): Table entry to add, see tables.py.
            file.
            **kwargs: Forwarded to self.session_scope()
        """
        with self.session_scope(**kwargs) as sess:
            sess.add(table_entry)


if __name__ == '__main__':
    db = DBHandler('sqlite', 'test.db')
    db.init_tables(tables=tables.PREPROCESSING_TABLES)
