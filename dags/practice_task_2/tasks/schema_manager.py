from airflow.providers.postgres.hooks.postgres import PostgresHook
from logging import getLogger
import os

logger = getLogger(__name__)

postgres_conn_id = "target_postgres"


def check_or_create_target_schema() -> bool:
    """Check if the target schema exists; if not, create it.

    Returns:
        bool: True if the schema exists or was created successfully.
    """

    logger.info("Checking or creating target schema...")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    current_dir = os.path.dirname(__file__)

    with open(
        os.path.join(current_dir, "..", "schemas", "target_schema.sql"), "r"
    ) as file:
        create_schema_sql = file.read()

    try:
        hook.run(create_schema_sql)
        logger.info("Target schema is ready.")
        return True

    except Exception as e:
        logger.error(f"Failed to create target schema: {e}")
        return False
