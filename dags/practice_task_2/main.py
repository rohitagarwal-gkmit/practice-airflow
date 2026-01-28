import os
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from logging import getLogger
import pandas as pd

logger = getLogger(__name__)
target_postgres_conn_id = "target_postgres"
source_postgres_conn_id = "source_postgres"


def on_failure_callback(context):
    print(f"Task {context['task_instance_key_str']} failed. Alerting...")


@dag(
    dag_id="practice_task_2_etl",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=15),
        "on_failure_callback": on_failure_callback,
        "pool": "default_pool",
    },
    description="A simple ETL DAG for practice task 2",
    # manual triggering
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["practice", "etl"],
)
def practice_task_2_etl():
    """A simple ETL DAG for practice task 2 using TaskFlow API."""

    @task(task_id="check_or_create_target_schema", pool="default_pool")
    def check_or_create_target_schema():
        """Check if the target schema exists; if not, create it.

        Returns:
            bool: True if the schema exists or was created successfully.
        """

        logger.info("Checking or creating target schema...")
        hook = PostgresHook(target_postgres_conn_id)

        current_dir = os.path.dirname(__file__)

        with open(
            os.path.join(current_dir, "schemas", "target_schema.sql"), "r"
        ) as file:
            create_schema_sql = file.read()

        try:
            hook.run(create_schema_sql)
            logger.info("Target schema is ready.")
            return True

        except Exception as e:
            logger.error(f"Failed to create target schema: {e}")
            return False

    @task(task_id="extract_data", pool="default_pool")
    def extract_data() -> pd.DataFrame:
        """Extract data from source systems.

        Returns:
            pd.DataFrame: Extracted records.
        """

        logger.info("Extracting data...")
        hook = PostgresHook(source_postgres_conn_id)
        records = hook.get_pandas_df("SELECT * FROM source.books;")

        logger.info(f"Extracted {len(records)} records.")

        return records

    @task(task_id="transform_data", pool="default_pool")
    def transform_data(data: pd.DataFrame) -> dict:
        """Transform the extracted data from denormalized to normalized structure.

        Args:
            data: The extracted DataFrame to transform.

        Returns:
            dict: Dictionary containing normalized DataFrames for each target table.
        """
        logger.info("Transforming data...")
        logger.info(f"Received {len(data)} records for transformation.")

        # 1. Extract unique categories
        categories_df = pd.DataFrame(
            {"category_name": data["category"].dropna().unique()}
        )
        logger.info(f"Extracted {len(categories_df)} unique categories.")

        # Create category mapping for foreign key relationships
        category_mapping = {
            row["category_name"]: idx + 1 for idx, row in categories_df.iterrows()
        }

        # 2. Transform products table
        products_df = data[
            ["title", "product_link", "upc", "product_type", "description", "category"]
        ].copy()
        products_df["category_id"] = products_df["category"].map(category_mapping)
        products_df = products_df.drop("category", axis=1)
        products_df = products_df.drop_duplicates(subset=["product_link"])
        logger.info(f"Transformed {len(products_df)} products.")

        # 3. Transform product images
        product_images_df = data[["product_link", "image_link"]].copy()
        product_images_df = product_images_df.dropna(subset=["image_link"])
        product_images_df = product_images_df.drop_duplicates()
        logger.info(f"Extracted {len(product_images_df)} product images.")

        # 4. Transform product prices
        product_prices_df = data[
            ["product_link", "price_excl_tax", "price_incl_tax", "tax"]
        ].copy()
        product_prices_df["currency"] = "GBP"
        product_prices_df = product_prices_df.drop_duplicates(subset=["product_link"])
        logger.info(f"Extracted {len(product_prices_df)} product prices.")

        # 5. Transform product inventory
        product_inventory_df = data[["product_link", "availability"]].copy()
        product_inventory_df = product_inventory_df.rename(
            columns={"availability": "availability_status"}
        )
        product_inventory_df = product_inventory_df.dropna(
            subset=["availability_status"]
        )
        product_inventory_df = product_inventory_df.drop_duplicates(
            subset=["product_link"]
        )
        logger.info(f"Extracted {len(product_inventory_df)} inventory records.")

        # 6. Transform ratings
        ratings_df = data[["product_link", "rating"]].copy()
        ratings_df = ratings_df.rename(columns={"rating": "rating_value"})
        ratings_df = ratings_df.dropna(subset=["rating_value"])
        ratings_df = ratings_df.drop_duplicates(subset=["product_link"])
        logger.info(f"Extracted {len(ratings_df)} ratings.")

        # 7. Transform reviews summary
        reviews_summary_df = data[["product_link", "number_of_reviews"]].copy()
        reviews_summary_df = reviews_summary_df.dropna(subset=["number_of_reviews"])
        reviews_summary_df = reviews_summary_df.drop_duplicates(subset=["product_link"])
        logger.info(f"Extracted {len(reviews_summary_df)} review summaries.")

        transformed_data = {
            "categories": categories_df,
            "products": products_df,
            "product_images": product_images_df,
            "product_prices": product_prices_df,
            "product_inventory": product_inventory_df,
            "ratings": ratings_df,
            "reviews_summary": reviews_summary_df,
        }

        logger.info("Data transformation complete.")
        return transformed_data

    @task(task_id="load_data", pool="default_pool")
    def load_data(transformed_data: dict) -> dict:
        """Load transformed data into the target system.

        Args:
            transformed_data: Dictionary containing normalized DataFrames for each target table.

        Returns:
            dict: Summary of the load operation.
        """
        logger.info("Loading data...")
        hook = PostgresHook(postgres_conn_id=target_postgres_conn_id)

        summary = {}

        # Load in proper order to respect foreign key constraints
        load_order = [
            ("categories", "target.categories"),
            ("products", "target.products"),
            ("product_images", "target.product_images"),
            ("product_prices", "target.product_prices"),
            ("product_inventory", "target.product_inventory"),
            ("ratings", "target.ratings"),
            ("reviews_summary", "target.reviews_summary"),
        ]

        for table_key, table_name in load_order:
            df = transformed_data.get(table_key)
            if df is not None and not df.empty:
                logger.info(f"Loading {len(df)} records to {table_name}...")

                # Insert data using PostgresHook
                hook.insert_rows(
                    table=table_name,
                    rows=df.values.tolist(),
                    target_fields=df.columns.tolist(),
                    replace=False,
                    replace_index=None,
                )

                summary[table_name] = len(df)
                logger.info(f"Successfully loaded {len(df)} records to {table_name}.")
            else:
                logger.warning(f"No data to load for {table_name}.")
                summary[table_name] = 0

        logger.info("Data load complete.")
        logger.info(f"Load summary: {summary}")
        return {"status": "success", "tables_loaded": summary}

    # Define task dependencies
    schema_check = check_or_create_target_schema()
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)

    schema_check >> extracted_data


# Instantiate the DAG
practice_task_2_etl()
