# TMS Data Warehouse Pipeline

This project is designed to build and manage a data warehouse for the Tenant Management System (TMS) using Apache Airflow. It includes ETL processes to extract, transform, and load data into the data warehouse, with Redshift as the target database.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
    - [1. Clone Repository](#1-clone-repository)
    - [2. Set Up a Virtual Environment](#2-set-up-a-virtual-environment)
    - [3. Install Dependencies](#3-install-dependencies)
    - [4. Configure Environment Variables](#4-configure-environment-variables)
    - [5. Set Up Airflow](#5-set-up-airflow)
---

## Prerequisites
Ensure the following tools are installed on your system:
- Python 3.8 or later
- Apache Airflow (tested with 2.4 or later)
- PostgreSQL (for Airflow metadata database)
- AWS CLI (to manage Redshift cluster)
- Docker (optional for using Dockerized Airflow)
- pipenv or virtualenv (for Python package management)

---

## Setup Instructions

### 1. Clone Repository
Clone the project repository to your local system:
```bash
git clone https://github.com/username/tms-dw-pipeline.git
cd tms-dw-pipeline
```

### 2. Set Up a Virtual Environment

Create and activate a virtual environment:

#### Using venv
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

#### Using pipenv (optional)
```bash
pipenv shell
```

### 3. Install Dependencies

Install the Python dependencies:
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a .env file in the root directory of the project and configure the following variables:

AIRFLOW_HOME=/path/to/airflow  # Change this to your preferred Airflow home directory
REDIS_HOST=<your_redis_host>
REDSHIFT_HOST=<your_redshift_endpoint>
REDSHIFT_PORT=5439
REDSHIFT_DB=<your_redshift_db_name>
REDSHIFT_USER=<your_redshift_user>
REDSHIFT_PASSWORD=<your_redshift_password>
AWS_ACCESS_KEY_ID=<your_aws_access_key>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_key>

### 5. Set Up Airflow
Initialize the Airflow Database
```bash
export AIRFLOW_HOME=/path/to/airflow  # Ensure this matches the value in your .env file
airflow db init
```

Create an Admin User
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```
Start Airflow Services

Start the webserver and scheduler:
```bash
airflow webserver -p 8080
airflow scheduler
```

Access Airflow at http://localhost:8080.
Running the Pipeline

    Deploy the DAG Move the DAG file tms_etl_pipeline.py to the dags/ directory in your Airflow home:

```bash
    cp airflow_dags/tms_etl_pipeline.py $AIRFLOW_HOME/dags/
```
    Trigger the Pipeline
        Open the Airflow web interface.
        Locate the DAG tms_etl_pipeline.
        Turn the DAG "on".
        Trigger a run by clicking the play button in the DAG's row.

    Monitor the Pipeline Use the Airflow web interface to monitor task execution status, logs, and any pipeline dependencies.


