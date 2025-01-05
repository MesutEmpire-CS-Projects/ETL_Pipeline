import pandas as pd
from extract import Extractor

class Transformer:

    # Transform landlord schema - matches target columns directly
    def transform_landlord(self, landlord_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform landlord data to match target schema.
        The landlord_id and status columns are retained as is from source schema to target.
        """
        return landlord_df.rename(columns={
            "name": "landlord_name",
            "email": "email_address"
        })[["landlord_id", "landlord_name", "phone_number", "email_address"]]

    # Transform tenant schema - handles move_in_date, retains matching column names.
    def transform_tenant(self, tenant_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the tenant dataframe.
        Ensures 'moveindate' maps directly to 'move_in_date' in target schema.
        """
        tenant_df["move_in_date"] = pd.to_datetime(tenant_df["move_in_date"], format="%Y-%m-%d")
        return tenant_df.rename(columns={
            "name": "tenant_name",
            "email": "email_address"
        })[["tenant_id", "tenant_name", "email_address", "is_verified", "phone_number", "unit_id", "property_id","move_in_date"]]

    # Transform property schema - full address and type match the target schema directly.
    def transform_property(self, property_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the property dataframe.
        Combines street, city, and state into a single 'full_address' field.
        Retains target column names 'property_id' and 'type' exactly.
        """
        return property_df.rename(columns={
            "name": "property_name"
        })[["property_id", "property_name", "location","landlord_id"]]

    # Transform unit schema - unit_id and unit_identifier based on source schema.
    def transform_unit(self, unit_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the unit dataframe.
        Combines building and unit_number into 'unit_identifier', map unit_id to target unit_id.
        """
        return unit_df.rename(columns={
            "name": "unit_name",
            "type": "unit_type"
        })[["unit_id", "unit_name", "unit_type", "price", "property_id", "is_vacant"]]

    # Transform payment schema - ensure columns are mapped to target schema (e.g., amount, payment_date).
    def transform_payment(self, payment_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the payment dataframe.
        Changes 'date' to 'payment_date', and maps 'payment_amount' to 'amount'.
        """
        payment_df["payment_date"] = pd.to_datetime(payment_df["date"], format="%Y-%m-%d")
        return payment_df.rename(columns={
            "amount": "payment_amount"
        })[["payment_id", "payment_amount", "payment_date", "status", "transaction_id", "tenant_id"]]

    # Transform expense schema - similar transformation with date mapping to expense_date.
    def transform_expense(self, expense_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the expense dataframe.
        Ensures expense date is mapped properly and retains the category.
        """
        expense_df["expense_date"] = pd.to_datetime(expense_df["date"], format="%Y-%m-%d")
        return expense_df.rename(columns={
            "amount": "expense_amount",
            "category":"expense_category"
        })[["expense_id", "expense_amount", "expense_date", "expense_category", "property_id", "receipt"]]


    # Transform vacate request schema - ensure date handling is in sync with target schema.
    def transform_vacate_request(self, vacate_request_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the vacate request dataframe.
        Renames columns to align with target schema (request_date, move_out_date).
        """
        vacate_request_df["request_date"] = pd.to_datetime(vacate_request_df["request_date"], format="%Y-%m-%d")
        vacate_request_df["move_out_date"] = pd.to_datetime(vacate_request_df["move_out_date"], format="%Y-%m-%d")
        return vacate_request_df[["vacate_request_id", "request_date", "move_out_date", "reason", "tenant_id"]]

    # Transform comment schema - ensure comment date handling as required by target schema.
    def transform_comment(self, comment_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the comment dataframe.
        Renames 'date' column to 'comment_date' for consistency and keeps comment text.
        """
        comment_df["comment_date"] = pd.to_datetime(comment_df["date"], format="%Y-%m-%d")
        return comment_df.rename(columns={
            "message": "comment_message"
        })[["comment_id", "comment_date", "comment_message", "tenant_id"]]


if __name__ == "__main__":
    # Example Usage:

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

    # Display transformed DataFrames
    print("Landlord Data:")
    print(landlord_transformed)
    print("\nProperty Data:")
    print(property_transformed)
    print("\nUnit Data:")
    print(unit_transformed)
    print("\nTenant Data:")
    print(tenant_transformed)
    print("\nPayment Data:")
    print(payment_transformed)
    print("\nExpense Data:")
    print(expense_transformed)
    print("\nVacate Request Data:")
    print(vacate_request_transformed)
    print("\nComment Data:")
    print(comment_transformed)
