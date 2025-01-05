from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.abspath('/home/mesutempire/Desktop/TMS_DW/pipeline'))
import etl

# Initialize the ETL class
etl_instance = etl.ETL()

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
        dag_id='etl_workflow',
        default_args=default_args,
        description='ETL workflow orchestration',
        schedule_interval=None,  # Run on demand
        start_date=datetime(2025, 1, 3),
        catchup=False,
        tags=['etl', 'example']
) as dag:

    # Define the extract function to be used by the extract task
    def extract(**kwargs):
        print("Starting extraction...")

        # Push extracted data to XCom
        landlord_df = etl_instance.extractor.extract_postgres("SELECT * FROM landlord")
        property_df = etl_instance.extractor.extract_postgres("SELECT * FROM property")
        unit_df = etl_instance.extractor.extract_postgres("SELECT * FROM unit;")
        tenant_df = etl_instance.extractor.extract_postgres("SELECT * FROM tenant;")
        payment_df = etl_instance.extractor.extract_postgres("SELECT * FROM payment;")
        expense_df = etl_instance.extractor.extract_postgres("SELECT * FROM expense;")
        vacate_request_df = etl_instance.extractor.extract_postgres("SELECT * FROM vacate_request;")
        comment_df = etl_instance.extractor.extract_postgres("SELECT * FROM comment;")

        print("Extraction complete.")

        # Push data to XCom
        kwargs['ti'].xcom_push(key='landlord_df', value=landlord_df)
        kwargs['ti'].xcom_push(key='property_df', value=property_df)
        kwargs['ti'].xcom_push(key='unit_df', value=unit_df)
        kwargs['ti'].xcom_push(key='tenant_df', value=tenant_df)
        kwargs['ti'].xcom_push(key='payment_df', value=payment_df)
        kwargs['ti'].xcom_push(key='expense_df', value=expense_df)
        kwargs['ti'].xcom_push(key='vacate_request_df', value=vacate_request_df)
        kwargs['ti'].xcom_push(key='comment_df', value=comment_df)

    # Define the transform function to be used by the transform task
    def transform(**kwargs):
        print("Starting transformation...")

        # Pull data from XCom
        ti = kwargs['ti']
        landlord_df = ti.xcom_pull(task_ids='extract_task', key='landlord_df')
        property_df = ti.xcom_pull(task_ids='extract_task', key='property_df')
        unit_df = ti.xcom_pull(task_ids='extract_task', key='unit_df')
        tenant_df = ti.xcom_pull(task_ids='extract_task', key='tenant_df')
        payment_df = ti.xcom_pull(task_ids='extract_task', key='payment_df')
        expense_df = ti.xcom_pull(task_ids='extract_task', key='expense_df')
        vacate_request_df = ti.xcom_pull(task_ids='extract_task', key='vacate_request_df')
        comment_df = ti.xcom_pull(task_ids='extract_task', key='comment_df')

        # Transform data
        landlord_transformed = etl_instance.transformer.transform_landlord(landlord_df)
        property_transformed = etl_instance.transformer.transform_property(property_df)
        unit_transformed = etl_instance.transformer.transform_unit(unit_df)
        tenant_transformed = etl_instance.transformer.transform_tenant(tenant_df)
        payment_transformed = etl_instance.transformer.transform_payment(payment_df)
        expense_transformed = etl_instance.transformer.transform_expense(expense_df)
        vacate_request_transformed = etl_instance.transformer.transform_vacate_request(vacate_request_df)
        comment_transformed = etl_instance.transformer.transform_comment(comment_df)

        merged_landlord = landlord_transformed.merge(property_transformed,on=["landlord_id"])
        merged_property = merged_landlord.merge(unit_transformed,on=["property_id"])
        merged_tenant = tenant_transformed.merge(merged_property,on=["unit_id", "property_id"])
        merged_payment = payment_transformed.merge(merged_tenant,on=["tenant_id"])
        merged_expense = expense_transformed.merge(property_transformed,how="left",on=["property_id"])
        merged_tenant['security_deposit_amount'] = merged_tenant['price']

        print("Transformation complete.")

        # Push transformed data to XCom
        kwargs['ti'].xcom_push(key='landlord_transformed', value=landlord_transformed)
        kwargs['ti'].xcom_push(key='property_transformed', value=property_transformed)
        kwargs['ti'].xcom_push(key='unit_transformed', value=unit_transformed)
        kwargs['ti'].xcom_push(key='tenant_transformed', value=tenant_transformed)
        kwargs['ti'].xcom_push(key='payment_transformed', value=payment_transformed)
        kwargs['ti'].xcom_push(key='expense_transformed', value=expense_transformed)
        kwargs['ti'].xcom_push(key='vacate_request_transformed', value=vacate_request_transformed)
        kwargs['ti'].xcom_push(key='comment_transformed', value=comment_transformed)
        kwargs['ti'].xcom_push(key='merged_landlord', value=merged_landlord)
        kwargs['ti'].xcom_push(key='merged_property', value=merged_property)
        kwargs['ti'].xcom_push(key='merged_tenant', value=merged_tenant)
        kwargs['ti'].xcom_push(key='merged_payment', value=merged_payment)
        kwargs['ti'].xcom_push(key='merged_expense', value=merged_expense)

    # Define the load function to be used by the load task
    def load(**kwargs):
        print("Starting load...")

        # Pull transformed data from XCom
        ti = kwargs['ti']
        landlord_transformed = ti.xcom_pull(task_ids='transform_task', key='landlord_transformed')
        property_transformed = ti.xcom_pull(task_ids='transform_task', key='property_transformed')
        unit_transformed = ti.xcom_pull(task_ids='transform_task', key='unit_transformed')
        tenant_transformed = ti.xcom_pull(task_ids='transform_task', key='tenant_transformed')
        payment_transformed = ti.xcom_pull(task_ids='transform_task', key='payment_transformed')
        expense_transformed = ti.xcom_pull(task_ids='transform_task', key='expense_transformed')
        vacate_request_transformed = ti.xcom_pull(task_ids='transform_task', key='vacate_request_transformed')
        comment_transformed = ti.xcom_pull(task_ids='transform_task', key='comment_transformed')
        merged_landlord = ti.xcom_pull(task_ids='transform_task', key='merged_landlord')
        merged_property = ti.xcom_pull(task_ids='transform_task', key='merged_property')
        merged_tenant = ti.xcom_pull(task_ids='transform_task', key='merged_tenant')
        merged_payment = ti.xcom_pull(task_ids='transform_task', key='merged_payment')
        merged_expense = ti.xcom_pull(task_ids='transform_task', key='merged_expense')

        # Load data into your destination tables
        etl_instance.load_dimension_table(landlord_transformed, "dim_landlord", "landlord_id")
        etl_instance.load_dimension_table(property_transformed.drop(columns=['landlord_id']), "dim_property", "property_id")
        etl_instance.load_unit_dimension(unit_transformed, "dim_unit", "unit_id")
        etl_instance.load_dimension_table(tenant_transformed.drop(columns=['is_verified','unit_id','property_id','move_in_date']), "dim_tenant", "tenant_id")
        etl_instance.load_table(payment_transformed[['payment_id','transaction_id','status']],'dim_payment')
        etl_instance.load_table(expense_transformed[['expense_id','expense_category']],'dim_expense')
        etl_instance.load_payment_fact(merged_payment[['payment_id','property_id','tenant_id','unit_id','landlord_id','payment_date','payment_amount']],'fact_payment')
        etl_instance.load_expense_fact(merged_expense[['expense_id','property_id',"expense_amount", "expense_date"]],'fact_expense')
        etl_instance.load_tenancy_fact(merged_tenant.rename(columns={"price":"monthly_rent_amount"})[['property_id','tenant_id','unit_id','landlord_id','move_in_date','monthly_rent_amount','security_deposit_amount']],'fact_tenancy')
        # # # etl_instance.load_vacancy_fact(merged_tenant.rename(columns={"price":"monthly_rent_amount"})[['property_id','tenant_id','unit_id','landlord_id','move_in_date','monthly_rent_amount','security_deposit_amount']],'fact_tenancy')


    print("Load complete.")

    # Define the tasks
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True,
    )

    # Set up task dependencies
    extract_task >> transform_task >> load_task
