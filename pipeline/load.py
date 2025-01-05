from typing import Literal
from sqlalchemy import create_engine
import pandas as pd
from config import Config
from extract import Extractor
from transform import Transformer

class Loader:
    def __init__(self):
        """
        Initializes the Loader class by setting up the database connection URL.
        """
        self.engine_url = Config.get_redshift_warehouse_uri()

    def get_engine(self):
        """
        Creates a new SQLAlchemy engine for database operations.
        """
        return create_engine(self.engine_url)

    def load_to_postgres(self, df: pd.DataFrame, table_name: str, if_exists: Literal["fail", "replace", "append"] = "append"):
        """
        Loads a DataFrame into a PostgreSQL table.
        :param df: pandas DataFrame to load.
        :param table_name: Target PostgreSQL table name.
        :param if_exists: How to behave if the table already exists:
                          'fail', 'replace', or 'append' (default: 'append').
        """
        engine = self.get_engine()
        try:
            print(f"Loading data into table {table_name}...")
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            print(f"Data successfully loaded into table {table_name}.")
        except Exception as e:
            print(f"Error loading data into table {table_name}: {e}")
            raise
        finally:
            engine.dispose()

    def load_geospatial_data(self, df: pd.DataFrame, table_name: str, geom_column: str, srid: int = 4326, if_exists: str = "append"):
        """
        Loads geospatial data with a geometry column into a PostgreSQL/PostGIS table.
        :param df: pandas DataFrame containing geospatial data.
        :param table_name: Target PostgreSQL table name.
        :param geom_column: Name of the geometry column in the DataFrame.
        :param srid: Spatial Reference System Identifier for geometry data (default: 4326).
        :param if_exists: Behavior if the table already exists: 'fail', 'replace', or 'append'.
        """
        engine = self.get_engine()
        try:
            print(f"Loading geospatial data into table {table_name}...")
            df = df.copy()
            # Convert geometry column to WKT format with SRID prefix
            df[geom_column] = df[geom_column].apply(
                lambda geom: f"SRID={srid};{geom}" if geom else None
            )

            df.to_sql(table_name, engine, if_exists=if_exists, index=False)

            # Ensure geometry data is cast properly in PostGIS
            with engine.connect() as conn:
                conn.execute(f"""
                    ALTER TABLE {table_name}
                    ALTER COLUMN {geom_column} 
                    TYPE geometry USING ST_SetSRID(ST_GeomFromText({geom_column}), {srid});
                """)
            print(f"Geospatial data successfully loaded into table {table_name}.")
        except Exception as e:
            print(f"Error loading geospatial data into table {table_name}: {e}")
            raise
        finally:
            engine.dispose()


if __name__ == "__main__":
    extractor = Extractor()

    # Example DataFrames
    landlord_df = extractor.extract_postgres("SELECT * FROM landlord;")

    property_df = extractor.extract_postgres("SELECT * FROM property;")

    unit_df = extractor.extract_postgres("SELECT * FROM unit;")

    tenant_df = extractor.extract_postgres("SELECT * FROM tenant;")

    payment_df = extractor.extract_postgres("SELECT * FROM payment;")

    expense_df = extractor.extract_postgres("SELECT * FROM expense;")

    vacate_request_df = extractor.extract_postgres("SELECT * FROM vacate_request;")

    comment_df = extractor.extract_postgres("SELECT * FROM comment;")

    # Apply transformations
    transformer = Transformer()
    landlord_transformed = transformer.transform_landlord(landlord_df)
    property_transformed = transformer.transform_property(property_df)
    unit_transformed = transformer.transform_unit(unit_df)
    tenant_transformed = transformer.transform_tenant(tenant_df)
    payment_transformed = transformer.transform_payment(payment_df)
    expense_transformed = transformer.transform_expense(expense_df)
    vacate_request_transformed = transformer.transform_vacate_request(vacate_request_df)
    comment_transformed = transformer.transform_comment(comment_df)

# Loader
    loader = Loader()
    loader.load_to_postgres(landlord_transformed, "dim_landlord")
    loader.load_to_postgres(property_transformed, "dim_property")
    loader.load_to_postgres(unit_transformed, "dim_unit")
    loader.load_to_postgres(tenant_transformed, "dim_tenant")
    loader.load_to_postgres(payment_transformed, "dim_payment")
    loader.load_to_postgres(expense_transformed, "dim_expense_type")
    loader.load_to_postgres(vacate_request_transformed, "dim_vacate_request")
    loader.load_to_postgres(comment_transformed, "dim_comment")


