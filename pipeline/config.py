from dotenv import load_dotenv
from typing import Dict
import os

class Config:
    """Configuration class for managing and returning database connection URIs."""

    def __init__(self):
        load_dotenv()

    @staticmethod
    def load_env():
        """Load environment variables from .env file."""
        load_dotenv()

    @staticmethod
    def get_postgres_source() -> Dict[str, str]:
        """
        Returns a dictionary containing PostgreSQL source configuration.
        """
        Config.load_env()
        return {
            "host": os.getenv("POSTGRES_SOURCE_HOST"),
            "database": os.getenv("POSTGRES_SOURCE_DB"),
            "user": os.getenv("POSTGRES_SOURCE_USER"),
            "password": os.getenv("POSTGRES_SOURCE_PASSWORD"),
            "port": os.getenv("POSTGRES_SOURCE_PORT"),
        }

    @staticmethod
    def get_mysql_source() -> Dict[str, str]:
        """
        Returns a dictionary containing MySQL source configuration.
        """
        Config.load_env()
        return {
            "host": os.getenv("MYSQL_SOURCE_HOST"),
            "database": os.getenv("MYSQL_SOURCE_DB"),
            "user": os.getenv("MYSQL_SOURCE_USER"),
            "password": os.getenv("MYSQL_SOURCE_PASSWORD"),
            "port": os.getenv("MYSQL_SOURCE_PORT"),
        }

    @staticmethod
    def get_postgres_warehouse() -> Dict[str, str]:
        """
        Returns a dictionary containing PostgreSQL warehouse configuration.
        """
        Config.load_env()
        return {
            "host": os.getenv("POSTGRES_WAREHOUSE_HOST"),
            "database": os.getenv("POSTGRES_WAREHOUSE_DB"),
            "user": os.getenv("POSTGRES_WAREHOUSE_USER"),
            "password": os.getenv("POSTGRES_WAREHOUSE_PASSWORD"),
            "port": os.getenv("POSTGRES_WAREHOUSE_PORT"),
        }


    @staticmethod
    def get_postgres_source_uri() -> str:
        """Return PostgreSQL source connection URI."""
        Config.load_env()
        return (
            f"postgresql://{os.getenv('POSTGRES_SOURCE_USER')}:{os.getenv('POSTGRES_SOURCE_PASSWORD')}@"
            f"{os.getenv('POSTGRES_SOURCE_HOST')}:{os.getenv('POSTGRES_SOURCE_PORT')}/{os.getenv('POSTGRES_SOURCE_DB')}"
        )

    @staticmethod
    def get_mysql_source_uri() -> str:
        """Return MySQL source connection URI."""
        Config.load_env()
        return (
            f"mysql+mysqlconnector://{os.getenv('MYSQL_SOURCE_USER')}:{os.getenv('MYSQL_SOURCE_PASSWORD')}@"
            f"{os.getenv('MYSQL_SOURCE_HOST')}:{os.getenv('MYSQL_SOURCE_PORT')}/{os.getenv('MYSQL_SOURCE_DB')}"
        )

    @staticmethod
    def get_postgres_warehouse_uri() -> str:
        """Return PostgreSQL warehouse connection URI."""
        Config.load_env()
        return (
            f"postgresql://{os.getenv('POSTGRES_WAREHOUSE_USER')}:{os.getenv('POSTGRES_WAREHOUSE_PASSWORD')}@"
            f"{os.getenv('POSTGRES_WAREHOUSE_HOST')}:{os.getenv('POSTGRES_WAREHOUSE_PORT')}/{os.getenv('POSTGRES_WAREHOUSE_DB')}"
        )

    @staticmethod
    def get_redshift_warehouse_uri() -> str:
        """Return Redshift warehouse connection URI."""
        Config.load_env()
        return (
                f"redshift+psycopg2://{os.getenv('REDSHIFT_WAREHOUSE_USER')}:{os.getenv('REDSHIFT_WAREHOUSE_PASSWORD')}@"
                f"{os.getenv('REDSHIFT_WAREHOUSE_HOST')}:{os.getenv('REDSHIFT_WAREHOUSE_PORT')}/{os.getenv('REDSHIFT_WAREHOUSE_DB')}")



if __name__ == '__main__':
    print(Config.get_postgres_source_uri())

    print(Config.get_mysql_source_uri())

    print(Config.get_postgres_warehouse_uri())

    print(Config.get_postgres_source())