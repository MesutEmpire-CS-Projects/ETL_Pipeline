from sqlalchemy import create_engine
import pandas as pd
from typing import Optional
from config import Config

# # from pipeline.config import Config



class Extractor:
    """
    Class to handle extraction from PostgreSQL and MySQL databases.
    """

    def _connect_postgres(self):
        """
        Establish a connection to the PostgreSQL database using configuration from Config.
        """
        postgres_uri = Config.get_postgres_source_uri()
        return create_engine(postgres_uri)

    def _connect_mysql(self):
        """
        Establish a connection to the MySQL database using configuration from Config.
        """
        mysql_uri = Config.get_mysql_source_uri()
        return create_engine(mysql_uri)

    def extract_postgres(self, query: str) -> Optional[pd.DataFrame]:
        """
        Extract data from PostgreSQL and return it as a pandas DataFrame.

        :param query: The SQL query to be executed.
        :return: pandas DataFrame containing the query result, or None in case of an error.
        """
        try:
            engine = self._connect_postgres()
            return pd.read_sql(query, engine)
        except Exception as e:
            print(f"Error extracting PostgreSQL data: {e}")
            return None

    def extract_mysql(self, query: str) -> Optional[pd.DataFrame]:
        """
        Extract data from MySQL and return it as a pandas DataFrame.

        :param query: The SQL query to be executed.
        :return: pandas DataFrame containing the query result, or None in case of an error.
        """
        try:
            engine = self._connect_mysql()
            return pd.read_sql(query, engine)
        except Exception as e:
            print(f"Error extracting MySQL data: {e}")
            return None


if __name__ == "__main__":
    extractor = Extractor()
    postgres_query = "SELECT * FROM landlord;"
    result_postgres = extractor.extract_postgres(postgres_query)
    print(result_postgres)