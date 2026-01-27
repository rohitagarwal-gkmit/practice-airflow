from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from datetime import datetime, timedelta

from practice_task_2.tasks import (
    extract_data,
    transform_data,
    load_data,
    on_failure_callback,
    check_or_create_target_schema,
)


with DAG(
    dag_id="practice_task_2_etl",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backo ff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=15),
        "on_failure_callback": on_failure_callback,
        "pool": "default_pool",
    },
    description="A simple ETL DAG for practice task 2",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["practice", "etl"],
) as dag:
    dag.doc_md = """A simple ETL DAG for practice task 2"""

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        pool="default_pool",
    )
    extract_task.__doc__ = """Extract data from source systems."""

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        pool="default_pool",
    )
    transform_task.__doc__ = """Transform extracted data."""

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        pool="default_pool",
    )

    check_or_create_target_schema_task = PythonOperator(
        task_id="check_or_create_target_schema",
        python_callable=check_or_create_target_schema,
        pool="default_pool",
    )
    check_or_create_target_schema_task.__doc__ = (
        """Check or create the target schema in the database."""
    )

    load_task.__doc__ = """Load transformed data into the target system."""

    check_or_create_target_schema_task >> extract_task >> transform_task >> load_task
