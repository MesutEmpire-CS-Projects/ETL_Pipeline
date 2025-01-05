from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text
from extract import Extractor
from transform import Transformer
from load import Loader
from calendar import month_name, day_name

class ETL:
    def __init__(self):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()

    def get_or_create_date_id(self, date: datetime, engine) -> int:
        """
        Gets or creates a date record in dim_date and returns its id
        """
        # Format date for comparison
        formatted_date = date.strftime('%Y-%m-%d')

        with engine.begin() as conn:
            # Check if it's a Redshift database
            print(engine.dialect.name)
            is_redshift = engine.dialect.name == 'redshift'

            # Try to get existing date
            query = text("SELECT date_id FROM dim_date WHERE date = :date")
            result = conn.execute(query, {'date': formatted_date}).fetchone()

            if result:
                return result[0]

            # Calculate all required fields
            date_data = {
                'date': date,
                'year': date.year,
                'quarter': (date.month - 1) // 3 + 1,
                'quarter_name': f'Q{(date.month - 1) // 3 + 1}',
                'month': date.month,
                'month_name': month_name[date.month],
                'day_of_month': date.day,
                'day_of_week': date.isoweekday(),  # 1 (Monday) to 7 (Sunday)
                'day_of_week_name': day_name[date.weekday()],
                'day_of_year': date.timetuple().tm_yday,
                'week_of_year': date.isocalendar()[1]
            }

            if is_redshift:
                # Insert the record
                insert_query = text("""
                        INSERT INTO dim_date (
                            date, 
                            year, 
                            quarter, 
                            quarter_name, 
                            month, 
                            month_name, 
                            day_of_month, 
                            day_of_week, 
                            day_of_week_name, 
                            day_of_year, 
                            week_of_year
                        )
                        VALUES (
                            :date,
                            :year,
                            :quarter,
                            :quarter_name,
                            :month,
                            :month_name,
                            :day_of_month,
                            :day_of_week,
                            :day_of_week_name,
                            :day_of_year,
                            :week_of_year
                        )
                    """)
                conn.execute(insert_query, date_data)

                # Fetch the last inserted ID using deterministic filtering
                result = conn.execute(query, {'date': formatted_date}).fetchone()
                return result[0]

            else:
                # Insert the new date record
                insert_query = text("""
                    INSERT INTO dim_date (
                        date, 
                        year, 
                        quarter, 
                        quarter_name, 
                        month, 
                        month_name, 
                        day_of_month, 
                        day_of_week, 
                        day_of_week_name, 
                        day_of_year, 
                        week_of_year
                    )
                    VALUES (
                        :date,
                        :year,
                        :quarter,
                        :quarter_name,
                        :month,
                        :month_name,
                        :day_of_month,
                        :day_of_week,
                        :day_of_week_name,
                        :day_of_year,
                        :week_of_year
                    )
                    RETURNING date_id
                """)

                result = conn.execute(insert_query, date_data).fetchone()
                return result[0]

    def _get_surrogate_key(self, table_name: str, entity_name: str, column_value: str, engine) -> Optional[int]:
        """
        Gets the surrogate key for a specific table by matching the value of a given column
        and filtering by current_flag (active row).

        Args:
        table_name (str): The dimension table to query (e.g., 'dim_property', 'dim_unit').
        entity_name (str): The column in the dimension table to match (e.g., 'property_id').
        column_value (str or int): The value to match for the column (e.g., 1 for property_id).
        engine: The SQLAlchemy engine to execute queries.

        Returns:
        int: The surrogate key for the matching row.
        """
        surrogate_key = entity_name+'_key'
        natural_key = entity_name+'_id'
        # Build a query to get the surrogate key based on column value and current_flag
        if table_name == 'dim_payment' or table_name == 'dim_expense':
            query = text(f"""
            SELECT {surrogate_key}
            FROM {table_name}
            WHERE {natural_key} = :column_value
            ORDER BY {surrogate_key} DESC
            LIMIT 1
        """)
        else :
            query = text(f"""
            SELECT {surrogate_key}
            FROM {table_name}
            WHERE {natural_key} = :column_value AND current_flag = true
            ORDER BY {surrogate_key} DESC
            LIMIT 1
             """)

        with engine.begin() as conn:
            result = conn.execute(query, {'column_value':column_value}).fetchone()
            if result:
                return result[0]  # The surrogate_key is assumed to be the first column
            else:
                return None

    def load_dimension_table(self, df: pd.DataFrame, table_name: str, primary_key: str):
        """
        Loads a DataFrame into a dimension table using SCD Type 2 methodology,
        properly handling date dimension references
        """
        engine = self.loader.get_engine()
        current_timestamp = datetime.now(timezone.utc)

        with engine.begin() as conn:
            for idx, row in df.iterrows():
                # Check if record exists
                query = text(f"SELECT * FROM {table_name} WHERE {primary_key} = :pk AND current_flag = true")
                existing = pd.read_sql(query, conn, params={'pk': row[primary_key]})
                print(f'is empty : {existing.empty} \n {existing}')

                if existing.empty:
                    # For new record
                    effective_start_date_id = self.get_or_create_date_id(current_timestamp, engine)
                    print(f"start date : {effective_start_date_id}")
                    # Prepare insert data
                    df.loc[idx, 'effective_start_date_id'] = effective_start_date_id
                    df.loc[idx, 'end_date_id'] = None
                    df.loc[idx, 'current_flag'] = True

                    updated_row = df.loc[idx].to_dict()
                    self.loader.load_to_postgres(pd.DataFrame([updated_row]), table_name)
                else:
                    # Check if the record has changed
                    comparison_cols = [col for col in row.index if col not in
                                       ['effective_start_date_id', 'effective_end_date_id', 'current_flag']]

                    if not row[comparison_cols].equals(existing.iloc[0][comparison_cols]):
                        # Get date IDs for the update
                        end_date_id = self.get_or_create_date_id(current_timestamp, engine)
                        new_start_date_id = self.get_or_create_date_id(current_timestamp, engine)

                        # Update existing record
                        update_query = text(f"""
                            UPDATE {table_name}
                            SET end_date_id = :end_date_id,
                                current_flag = false
                            WHERE {primary_key} = :pk
                            AND current_flag = true
                        """)

                        conn.execute(update_query, {
                            'end_date_id': end_date_id,
                            'pk': row[primary_key]
                        })

                        # Insert new record
                        df.loc[idx, 'effective_start_date_id'] = new_start_date_id
                        df.loc[idx, 'end_date_id'] = None
                        df.loc[idx, 'current_flag'] = True

                        updated_row = df.loc[idx].to_dict()
                        self.loader.load_to_postgres(pd.DataFrame([updated_row]), table_name)

            print(f"Processed dimension table: {table_name}")

    def load_unit_dimension(self, df: pd.DataFrame, table_name: str, primary_key: str):
        """
        Loads a DataFrame into the unit dimension table.
        Checks for the surrogate key of the property and performs SCD Type 2 handling.
        """
        engine = self.loader.get_engine()

        with engine.begin() as conn:
            rows_to_drop = []
            for idx, row in df.iterrows():
                # Check for Property Surrogate Key
                property_surrogate_key = self._get_surrogate_key('dim_property','property',row['property_id'], engine)
                if not property_surrogate_key:
                    print(f"Skipping unit {row['unit_id']} - Property not found.")
                    rows_to_drop.append(idx)
                else:
                    # Prepare row data with property surrogate key
                    df.loc[idx, 'property_key'] = property_surrogate_key
                    # row['property_key'] = property_surrogate_key

                # Drop the rows with missing property surrogate key
            df = df.drop(rows_to_drop)

        self.load_dimension_table(df.drop(columns=['price','is_vacant','property_id']), table_name, primary_key)

    # TODO : Check the use of move_in_date
    def load_tenant_dimension(self, df: pd.DataFrame, table_name: str, primary_key: str):
        """
        Loads a DataFrame into the tenant dimension table.
        Checks for the surrogate key of the property and performs SCD Type 2 handling.
        """
        engine = self.loader.get_engine()

        with engine.begin() as conn:
            rows_to_drop = []
            for idx, row in df.iterrows():
                # Check for move_in_date
                date_id = self.get_or_create_date_id(row['move_in_date'],engine)
                df.loc[idx, 'move_in_date'] = date_id

            df = df.drop(rows_to_drop)

        self.load_dimension_table(df, table_name, primary_key)

    def load_table(self, df: pd.DataFrame, table_name: str):
        """
        Loads fact data into the fact table.
        :param df: Extracted and transformed fact data.
        :param table_name: Target fact table name.
        """
        self.loader.load_to_postgres(df, table_name)

    def load_payment_fact(self, df: pd.DataFrame, table_name: str):
        """
        Loads a DataFrame into the payment fact table.
        """
        engine = self.loader.get_engine()

        with engine.begin() as conn:
            rows_to_drop = []
            for idx, row in df.iterrows():
                # Check for Property Surrogate Key
                property_surrogate_key = self._get_surrogate_key('dim_property','property',row['property_id'], engine)

                tenant_surrogate_key = self._get_surrogate_key('dim_tenant','tenant',row['tenant_id'], engine)

                unit_surrogate_key = self._get_surrogate_key('dim_unit','unit',row['unit_id'], engine)

                landlord_surrogate_key = self._get_surrogate_key('dim_landlord','landlord',row['landlord_id'], engine)

                payment_surrogate_key = self._get_surrogate_key('dim_payment','payment',row['payment_id'], engine)

                date_id = self.get_or_create_date_id(row['payment_date'],engine)

                if not property_surrogate_key or not tenant_surrogate_key or not unit_surrogate_key or not landlord_surrogate_key:
                    print(f"Removed payment {row['payment_id']} ")
                    rows_to_drop.append(idx)
                else:
                    # Prepare row data with surrogate keys
                    df.loc[idx, 'property_key'] = property_surrogate_key
                    df.loc[idx, 'tenant_key'] = tenant_surrogate_key
                    df.loc[idx, 'unit_key'] = unit_surrogate_key
                    df.loc[idx, 'landlord_key'] = landlord_surrogate_key
                    df.loc[idx, 'payment_key'] = payment_surrogate_key
                    df.loc[idx, 'date_id'] = date_id

                # Drop the rows with missing property surrogate key
            df = df.drop(rows_to_drop)

        self.load_table(df.drop(columns=['payment_id','payment_date','property_id','tenant_id','unit_id','landlord_id']),table_name)

    def load_expense_fact(self, df: pd.DataFrame, table_name: str):
        """
        Loads a DataFrame into the expense fact table.
        """
        engine = self.loader.get_engine()

        with engine.begin() as conn:
            rows_to_drop = []
            for idx, row in df.iterrows():
                # Check for Property Surrogate Key
                property_surrogate_key = self._get_surrogate_key('dim_property','property',row['property_id'], engine)

                expense_surrogate_key = self._get_surrogate_key('dim_expense','expense',row['expense_id'], engine)

                date_id = self.get_or_create_date_id(row['expense_date'],engine)

                #
                # if not property_surrogate_key or not tenant_surrogate_key or not unit_surrogate_key or not landlord_surrogate_key:

                if not property_surrogate_key:
                    print(f"Removed expense : {row['expense_id']} ")
                    rows_to_drop.append(idx)
                else:
                    # Prepare row data with property surrogate key
                    df.loc[idx, 'property_key'] = property_surrogate_key
                    df.loc[idx, 'expense_key'] = expense_surrogate_key
                    df.loc[idx, 'date_id'] = date_id

                # Drop the rows with missing property surrogate key
            df = df.drop(rows_to_drop)

        self.load_table(df.drop(columns=['expense_id','expense_date','property_id']),table_name)

    def load_tenancy_fact(self, df: pd.DataFrame, table_name: str):
        """
        Loads a DataFrame into the tenancy fact table.
        """
        engine = self.loader.get_engine()

        with engine.begin() as conn:
            rows_to_drop = []
            for idx, row in df.iterrows():
                # Check for Property Surrogate Key
                property_surrogate_key = self._get_surrogate_key('dim_property','property',row['property_id'], engine)

                tenant_surrogate_key = self._get_surrogate_key('dim_tenant','tenant',row['tenant_id'], engine)

                unit_surrogate_key = self._get_surrogate_key('dim_unit','unit',row['unit_id'], engine)

                landlord_surrogate_key = self._get_surrogate_key('dim_landlord','landlord',row['landlord_id'], engine)

                date_id = self.get_or_create_date_id(row['move_in_date'],engine)

                if not property_surrogate_key or not tenant_surrogate_key or not unit_surrogate_key or not landlord_surrogate_key:
                    print(f"Removed tenancy : {row['tenant_id']}")
                    rows_to_drop.append(idx)
                else:
                    # Prepare row data with property surrogate key
                    df.loc[idx, 'property_key'] = property_surrogate_key
                    df.loc[idx, 'tenant_key'] = tenant_surrogate_key
                    df.loc[idx, 'unit_key'] = unit_surrogate_key
                    df.loc[idx, 'landlord_key'] = landlord_surrogate_key
                    df.loc[idx, 'date_id'] = date_id

                # Drop the rows with missing property surrogate key
            df = df.drop(rows_to_drop)

        self.load_table(df.drop(columns=['move_in_date','property_id','tenant_id','unit_id','landlord_id']),table_name)

    def load_vacancy_fact(self, df: pd.DataFrame, table_name: str):
        """
        Loads a DataFrame into the vacancy fact table.
        """
        engine = self.loader.get_engine()

        with engine.begin() as conn:
            rows_to_drop = []
            for idx, row in df.iterrows():
                # Check for Property Surrogate Key
                property_surrogate_key = self._get_surrogate_key('dim_property','property',row['property_id'], engine)

                tenant_surrogate_key = self._get_surrogate_key('dim_tenant','tenant',row['tenant_id'], engine)

                unit_surrogate_key = self._get_surrogate_key('dim_unit','unit',row['unit_id'], engine)

                landlord_surrogate_key = self._get_surrogate_key('dim_landlord','landlord',row['landlord_id'], engine)

                date_id = self.get_or_create_date_id(row['payment_date'],engine)

                if not property_surrogate_key or not tenant_surrogate_key or not unit_surrogate_key or not landlord_surrogate_key:
                    print(f"Removed vacancy {row['unit_id']}")
                    rows_to_drop.append(idx)
                else:
                    # Prepare row data with property surrogate key
                    df.loc[idx, 'property_key'] = property_surrogate_key
                    df.loc[idx, 'tenant_key'] = tenant_surrogate_key
                    df.loc[idx, 'unit_key'] = unit_surrogate_key
                    df.loc[idx, 'landlord_key'] = landlord_surrogate_key
                    df.loc[idx, 'date_id'] = date_id

                # Drop the rows with missing property surrogate key
            df = df.drop(rows_to_drop)

        self.load_table(df.drop(columns=['payment_date','property_id','tenant_id','unit_id','landlord_id']),table_name)

    def execute(self):
            """ Main ETL Execution flow """
            # Extract step
            landlord_df = self.extractor.extract_postgres("SELECT * FROM landlord")
            property_df = self.extractor.extract_postgres("SELECT * FROM property")
            unit_df = self.extractor.extract_postgres("SELECT * FROM unit;")
            tenant_df = self.extractor.extract_postgres("SELECT * FROM tenant;")
            payment_df = self.extractor.extract_postgres("SELECT * FROM payment;")
            expense_df = self.extractor.extract_postgres("SELECT * FROM expense;")
            vacate_request_df = self.extractor.extract_postgres("SELECT * FROM vacate_request;")
            comment_df = self.extractor.extract_postgres("SELECT * FROM comment;")

            # Transform step
            landlord_transformed = self.transformer.transform_landlord(landlord_df)
            property_transformed = self.transformer.transform_property(property_df)
            unit_transformed = self.transformer.transform_unit(unit_df)
            tenant_transformed = self.transformer.transform_tenant(tenant_df)
            payment_transformed = self.transformer.transform_payment(payment_df)
            expense_transformed = self.transformer.transform_expense(expense_df)
            vacate_request_transformed = self.transformer.transform_vacate_request(vacate_request_df)
            comment_transformed = self.transformer.transform_comment(comment_df)

            merged_landlord = landlord_transformed.merge(property_transformed,on=["landlord_id"])
            merged_property = merged_landlord.merge(unit_transformed,on=["property_id"])
            merged_tenant = tenant_transformed.merge(merged_property,on=["unit_id", "property_id"])
            merged_payment = payment_transformed.merge(merged_tenant,on=["tenant_id"])
            merged_expense = expense_transformed.merge(property_transformed,how="left",on=["property_id"])
            merged_tenant['security_deposit_amount'] = merged_tenant['price']

            # Load dimension tables with proper SCD Type 2 date handling
            self.load_dimension_table(landlord_transformed, "dim_landlord", "landlord_id")
            self.load_dimension_table(property_transformed.drop(columns=['landlord_id']), "dim_property", "property_id")
            self.load_unit_dimension(unit_transformed, "dim_unit", "unit_id")
            self.load_dimension_table(tenant_transformed.drop(columns=['is_verified','unit_id','property_id','move_in_date']), "dim_tenant", "tenant_id")

            self.load_table(payment_transformed[['payment_id','transaction_id','status']],'dim_payment')
            self.load_table(expense_transformed[['expense_id','expense_category']],'dim_expense')
            self.load_payment_fact(merged_payment[['payment_id','property_id','tenant_id','unit_id','landlord_id','payment_date','payment_amount']],'fact_payment')
            self.load_expense_fact(merged_expense[['expense_id','property_id',"expense_amount", "expense_date"]],'fact_expense')
            self.load_tenancy_fact(merged_tenant.rename(columns={"price":"monthly_rent_amount"})[['property_id','tenant_id','unit_id','landlord_id','move_in_date','monthly_rent_amount','security_deposit_amount']],'fact_tenancy')
            # # # self.load_vacancy_fact(merged_tenant.rename(columns={"price":"monthly_rent_amount"})[['property_id','tenant_id','unit_id','landlord_id','move_in_date','monthly_rent_amount','security_deposit_amount']],'fact_tenancy')


if __name__ == "__main__":
    etl = ETL()
    etl.execute()