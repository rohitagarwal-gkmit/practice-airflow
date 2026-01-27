from airflow.providers.postgres.hooks.postgres import PostgresHook
from logging import getLogger
import pandas as pd

logger = getLogger(__name__)
source_postgres_conn_id = "source_postgres"


def extract_data() -> pd.DataFrame:
    """Extract data from source systems.

    Returns:
        pd.DataFrame: Extracted records.
    """

    logger.info("Extracting data...")
    hook = PostgresHook(postgres_conn_id=source_postgres_conn_id)
    records = hook.get_pandas_df("SELECT * FROM source.books;")

    logger.info(f"Extracted {len(records)} records.")

    return records
