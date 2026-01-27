import textwrap
from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule


# Add logging and callbacks for maintenance
def on_failure_callback(context):
    print(
        f"Task {context['task_instance_key_str']} failed. Alerting..."
    )  # Replace with actual alerting (e.g., Slack/email)


with DAG(
    dag_id="practice_task_1_tutorial",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 3,  # Increased for reliability
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,  # Optimization: Exponential backoff
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": on_failure_callback,  # Maintenance: Failure alerts
        "execution_timeout": timedelta(minutes=10),  # Prevent hanging tasks
    },
    description="Optimized tutorial DAG for production",
    schedule="@daily",  # Changed to scheduled for automation
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "production-ready"],
    sla_miss_callback=on_failure_callback,  # Alert on SLA misses
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
        pool="default_pool",  # Optimization: Limit concurrent runs
    )

    t2 = BashOperator(
        task_id="sleep",
        bash_command="sleep 5",
        retries=5,  # Override for this task
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Ensure upstream success
    )

    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7) }}"
    {% endfor %}
    """
    )
    t3 = BashOperator(
        task_id="templated",
        bash_command=templated_command,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Add documentation (kept similar)
    t1.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    Optimized for production: Includes timeouts, retries, and SLAs.
    ![img](https://imgs.xkcd.com/comics/fixing_problems.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )
    t2.doc_md = "Production-ready sleep task with enhanced retries."
    t3.doc_md = "Templated task with error handling."
    dag.doc_md = "Production DAG with optimizations for scalability and maintenance."

    t1 >> [t2, t3]
