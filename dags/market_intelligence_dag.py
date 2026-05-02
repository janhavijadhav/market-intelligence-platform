from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    "owner": "market_intelligence",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_snowflake_connection():
    """Verify Snowflake connection is healthy."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM market_intelligence.raw.trades")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"Snowflake connection healthy. Total trades: {count}")
    return count


def check_data_freshness():
    """Alert if no new data has arrived in the last 10 minutes."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM market_intelligence.raw.trades
        WHERE loaded_at >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
    """)
    recent_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if recent_count == 0:
        raise ValueError("No new trades in the last 10 minutes — pipeline may be stale!")
    print(f"Data freshness check passed. {recent_count} trades in last 10 minutes.")
    return recent_count


def validate_trade_metrics():
    """Check trade_metrics table has no nulls in key columns."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM market_intelligence.raw.trade_metrics
        WHERE vwap IS NULL OR symbol IS NULL OR window_start IS NULL
    """)
    null_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if null_count > 0:
        raise ValueError(f"Found {null_count} rows with null values in trade_metrics!")
    print("Data validation passed. No null values found.")
    return null_count


with DAG(
    dag_id="market_intelligence_pipeline",
    default_args=default_args,
    description="Market Intelligence Platform monitoring and validation",
    schedule_interval="*/15 * * * *",  # every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["market_intelligence", "monitoring"],
) as dag:

    check_connection = PythonOperator(
        task_id="check_snowflake_connection",
        python_callable=check_snowflake_connection,
    )

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    validate_metrics = PythonOperator(
        task_id="validate_trade_metrics",
        python_callable=validate_trade_metrics,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dags && echo 'dbt run would execute here'",
    )

    # Task dependencies
    check_connection >> check_freshness >> validate_metrics >> run_dbt
