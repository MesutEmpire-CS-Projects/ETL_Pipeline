from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# from pipeline.etl import ETL  # Import your ETL class
import pipeline.etl as etl

# Initialize ETL instance
etl = etl.ETL()

(landlord_df,property_df,unit_df,tenant_df,payment_df,expense_df,vacate_request_df,comment_df,landlord_transformed,
    property_transformed,unit_transformed ,tenant_transformed,payment_transformed ,expense_transformed ,
 vacate_request_transformed , comment_transformed) = None

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
        dag_id='etl_workflow',
        default_args=default_args,
        description='ETL workflow orchestration',
        schedule_interval=None,  # Set to a cron expression if you want periodic runs
        start_date=datetime(2025, 1, 3),
        catchup=False,
        tags=['etl', 'example']
) as dag:

    # ETL functions as Airflow tasks
    def extract():
        print("Starting extraction...")
        landlord_df = etl.extractor.extract_postgres("SELECT * FROM landlord")
        property_df = etl.extractor.extract_postgres("SELECT * FROM property")
        unit_df = etl.extractor.extract_postgres("SELECT * FROM unit;")
        tenant_df = etl.extractor.extract_postgres("SELECT * FROM tenant;")
        payment_df = etl.extractor.extract_postgres("SELECT * FROM payment;")
        expense_df = etl.extractor.extract_postgres("SELECT * FROM expense;")
        vacate_request_df = etl.extractor.extract_postgres("SELECT * FROM vacate_request;")
        comment_df = etl.extractor.extract_postgres("SELECT * FROM comment;")
        print("Extraction complete.")

    def transform():
        print("Starting transformation...")
        landlord_transformed = etl.transformer.transform_landlord(landlord_df)
        property_transformed = etl.transformer.transform_property(property_df)
        unit_transformed = etl.transformer.transform_unit(unit_df)
        tenant_transformed = etl.transformer.transform_tenant(tenant_df)
        payment_transformed = etl.transformer.transform_payment(payment_df)
        expense_transformed = etl.transformer.transform_expense(expense_df)
        vacate_request_transformed = etl.transformer.transform_vacate_request(vacate_request_df)
        comment_transformed = etl.transformer.transform_comment(comment_df)
        print("Transformation complete.")

    def load():
        print("Starting load...")
        etl.load_dimension_table(landlord_transformed, "dim_landlord", "landlord_id")
        etl.load_dimension_table(property_transformed, "dim_property", "property_id")
        etl.load_unit_dimension(unit_transformed, "dim_unit", "unit_id")
        etl.load_dimension_table(tenant_transformed.drop(columns=['is_verified','unit_id','property_id']), "dim_tenant", "tenant_id")


        etl.load_table(payment_transformed[['payment_id','transaction_id','status']],'dim_payment')
        etl.load_payment_fact(payment_transformed,'fact_payment')
        print("Load complete.")

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
