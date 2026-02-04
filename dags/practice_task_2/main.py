import os
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from logging import getLogger
import pandas as pd

logger = getLogger(__name__)
target_postgres_conn_id = "target_postgres"
source_postgres_conn_id = "source_postgres"


def on_failure_callback(context):
    """Callback function to be called on task failure.

    as well as raise email of failure.
    """

    logger.error(f"Task {context['task_instance'].task_id} failed.")


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
        records = hook.get_pandas_df("SELECT * FROM books;")

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
        product_prices_df["price_excl_tax"] = (
            product_prices_df["price_excl_tax"].str.replace("£", "").astype(float)
        )
        product_prices_df["price_incl_tax"] = (
            product_prices_df["price_incl_tax"].str.replace("£", "").astype(float)
        )
        product_prices_df["tax"] = (
            product_prices_df["tax"].str.replace("£", "").astype(float)
        )
        logger.info(f"Extracted {len(product_prices_df)} product prices.")

        # 5. Transform product inventory
        product_inventory_df = data[["product_link", "availability"]].copy()
        product_inventory_df["availability_count"] = (
            product_inventory_df["availability"]
            .str.replace("In stock (", "")
            .str.replace(" available)", "")
            .str.strip()
        )
        product_inventory_df = product_inventory_df.dropna(
            subset=["availability_count"]
        )
        product_inventory_df = product_inventory_df.drop_duplicates(
            subset=["product_link"]
        )
        product_inventory_df["availability_count"] = product_inventory_df[
            "availability_count"
        ].astype(float)
        product_inventory_df["availability_status"] = product_inventory_df[
            "availability_count"
        ].apply(lambda x: "In Stock" if x > 0 else "Out of Stock")
        product_inventory_df = product_inventory_df[
            ["product_link", "availability_status", "availability_count"]
        ]
        logger.info(f"Extracted {len(product_inventory_df)} inventory records.")

        # 6. Transform ratings
        ratings_df = data[["product_link", "rating"]].copy()
        ratings_df = ratings_df.rename(columns={"rating": "rating_value"})
        for index, row in ratings_df.iterrows():
            rating_str = row["rating_value"]
            if rating_str == "One":
                ratings_df.at[index, "rating_value"] = 1
            elif rating_str == "Two":
                ratings_df.at[index, "rating_value"] = 2
            elif rating_str == "Three":
                ratings_df.at[index, "rating_value"] = 3
            elif rating_str == "Four":
                ratings_df.at[index, "rating_value"] = 4
            elif rating_str == "Five":
                ratings_df.at[index, "rating_value"] = 5
            else:
                ratings_df.at[index, "rating_value"] = None
        ratings_df = ratings_df.dropna(subset=["rating_value"])
        ratings_df = ratings_df.drop_duplicates(subset=["product_link"])
        logger.info(f"Extracted {len(ratings_df)} ratings.")

        # 7. Transform reviews summary
        reviews_summary_df = data[["product_link", "number_of_reviews"]].copy()
        reviews_summary_df = reviews_summary_df.dropna(subset=["number_of_reviews"])
        reviews_summary_df = reviews_summary_df.drop_duplicates(subset=["product_link"])
        reviews_summary_df["number_of_reviews"] = reviews_summary_df[
            "number_of_reviews"
        ].astype(int)
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
        """Load transformed data into the target system with transaction support.

        Args:
            transformed_data: Dictionary containing normalized DataFrames for each target table.

        Returns:
            dict: Summary of the load operation.
        """
        hook = PostgresHook(postgres_conn_id=target_postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        summary = {}

        try:
            # Start transaction
            logger.info("Starting transaction for data load...")

            # 1. Load categories
            categories_df = transformed_data.get("categories")
            if categories_df is not None and not categories_df.empty:
                logger.info(f"Loading {len(categories_df)} categories...")
                for _, row in categories_df.iterrows():
                    cursor.execute(
                        "INSERT INTO target.categories (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING",
                        (row["category_name"],),
                    )
                summary["target.categories"] = len(categories_df)
                logger.info(f"Loaded {len(categories_df)} categories.")

            # 2. Load products and get product_id mapping
            products_df = transformed_data.get("products")
            product_link_to_id = {}

            if products_df is not None and not products_df.empty:
                logger.info(f"Loading {len(products_df)} products...")
                for _, row in products_df.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO target.products (title, product_link, upc, product_type, description, category_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_link) DO UPDATE 
                        SET title = EXCLUDED.title
                        RETURNING product_id, product_link
                        """,
                        (
                            row["title"],
                            row["product_link"],
                            row["upc"],
                            row["product_type"],
                            row["description"],
                            row["category_id"],
                        ),
                    )
                    result = cursor.fetchone()
                    product_link_to_id[result[1]] = result[0]

                summary["target.products"] = len(products_df)
                logger.info(f"Loaded {len(products_df)} products.")

            # 3. Load product_images (with product_id)
            product_images_df = transformed_data.get("product_images")
            if product_images_df is not None and not product_images_df.empty:
                logger.info(f"Loading {len(product_images_df)} product images...")
                loaded_count = 0
                for _, row in product_images_df.iterrows():
                    product_id = product_link_to_id.get(row["product_link"])
                    if product_id:
                        cursor.execute(
                            "INSERT INTO target.product_images (product_id, image_link) VALUES (%s, %s)",
                            (product_id, row["image_link"]),
                        )
                        loaded_count += 1
                summary["target.product_images"] = loaded_count
                logger.info(f"Loaded {loaded_count} product images.")

            # 4. Load product_prices (with product_id)
            product_prices_df = transformed_data.get("product_prices")
            if product_prices_df is not None and not product_prices_df.empty:
                logger.info(f"Loading {len(product_prices_df)} product prices...")
                loaded_count = 0
                for _, row in product_prices_df.iterrows():
                    product_id = product_link_to_id.get(row["product_link"])
                    if product_id:
                        cursor.execute(
                            """
                            INSERT INTO target.product_prices 
                            (product_id, price_excl_tax, price_incl_tax, tax, currency)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                product_id,
                                row["price_excl_tax"],
                                row["price_incl_tax"],
                                row["tax"],
                                row["currency"],
                            ),
                        )
                        loaded_count += 1
                summary["target.product_prices"] = loaded_count
                logger.info(f"Loaded {loaded_count} product prices.")

            # 5. Load product_inventory (with product_id)
            product_inventory_df = transformed_data.get("product_inventory")
            if product_inventory_df is not None and not product_inventory_df.empty:
                logger.info(f"Loading {len(product_inventory_df)} inventory records...")
                loaded_count = 0
                for _, row in product_inventory_df.iterrows():
                    product_id = product_link_to_id.get(row["product_link"])
                    if product_id:
                        cursor.execute(
                            """
                            INSERT INTO target.product_inventory (product_id, availability_status)
                            VALUES (%s, %s)
                            """,
                            (product_id, row["availability_status"]),
                        )
                        loaded_count += 1
                summary["target.product_inventory"] = loaded_count
                logger.info(f"Loaded {loaded_count} inventory records.")

            # 6. Load ratings (with product_id)
            ratings_df = transformed_data.get("ratings")
            if ratings_df is not None and not ratings_df.empty:
                logger.info(f"Loading {len(ratings_df)} ratings...")
                loaded_count = 0
                for _, row in ratings_df.iterrows():
                    product_id = product_link_to_id.get(row["product_link"])
                    if product_id:
                        cursor.execute(
                            "INSERT INTO target.ratings (product_id, rating_value) VALUES (%s, %s)",
                            (product_id, row["rating_value"]),
                        )
                        loaded_count += 1
                summary["target.ratings"] = loaded_count
                logger.info(f"Loaded {loaded_count} ratings.")

            # 7. Load reviews_summary (with product_id)
            reviews_summary_df = transformed_data.get("reviews_summary")
            if reviews_summary_df is not None and not reviews_summary_df.empty:
                logger.info(f"Loading {len(reviews_summary_df)} review summaries...")
                loaded_count = 0
                for _, row in reviews_summary_df.iterrows():
                    product_id = product_link_to_id.get(row["product_link"])
                    if product_id:
                        cursor.execute(
                            """
                            INSERT INTO target.reviews_summary (product_id, number_of_reviews)
                            VALUES (%s, %s)
                            """,
                            (product_id, row["number_of_reviews"]),
                        )
                        loaded_count += 1
                summary["target.reviews_summary"] = loaded_count
                logger.info(f"Loaded {loaded_count} review summaries.")

            # Commit transaction
            conn.commit()
            logger.info("Transaction committed successfully.")
            logger.info(f"Load summary: {summary}")

            return {"status": "success", "tables_loaded": summary}

        except Exception as e:
            # Rollback on failure
            conn.rollback()
            logger.error(f"Error during data load. Rolling back transaction: {e}")
            raise

        finally:
            cursor.close()
            conn.close()

    @task(task_id="get_email_data", pool="default_pool")
    def get_email_data(load_result: dict) -> dict:
        """Prepare email content based on load result."""
        try:
            tables_loaded = (
                load_result.get("tables_loaded", {})
                if isinstance(load_result, dict)
                else {}
            )

            subject = "Airflow: practice_task_2_etl succeeded"
            body = (
                """<strong>Congratulations!</strong>\n\n
                <div>The ETL DAG <code>practice_task_2_etl</code> has completed successfully.</div>\n\n
                <br>
                <div>Tables loaded:</div>
                <ul>"""
                + "".join(
                    f"<li>{table}: {count} records</li>"
                    for table, count in tables_loaded.items()
                )
                + """</ul>"""
            )

            return {
                "status": "success",
                "email_data": {"subject": subject, "body": body},
            }
        except Exception as e:
            logger.error(f"Failed to send email notification via connection: {e}")
            return {"status": "error"}

    # Define task dependencies
    schema_check = check_or_create_target_schema()
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_result = load_data(transformed_data)
    email_data = get_email_data(load_result)

    email_data >> EmailOperator(
        task_id="send_email_notification",
        to="rohitagarwal@gkmit.co",
        subject="{{ ti.xcom_pull(task_ids='get_email_data')['email_data']['subject'] }}",
        html_content="{{ ti.xcom_pull(task_ids='get_email_data')['email_data']['body'] }}",
        trigger_rule="all_success",
    )

    schema_check >> extracted_data


# Instantiate the DAG
practice_task_2_etl()
