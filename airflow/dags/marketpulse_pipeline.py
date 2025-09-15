from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

PROJECT_DIR = Path("/opt/project")
EXTRACT_DIR = PROJECT_DIR / "Data_Ingestion"
DBT_DIR     = PROJECT_DIR / "dbt" / "marketpulse_dbt"
ML_DIR      = PROJECT_DIR / "ml"

default_args = {
    "owner": "marketpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketpulse_pipeline",
    start_date=datetime(2025, 9, 1),
    schedule="0 22 * * *",  # daily 22:00 UTC
    catchup=False,
    default_args=default_args,
    tags=["stocks", "snowflake", "dbt", "ml"],
) as dag:

    drop_schemas = SQLExecuteQueryOperator(
    task_id="drop_schemas",
    conn_id="snowflake_default",
    sql="""
      USE ROLE {{ var.value.SNOWFLAKE_ROLE }};
      USE WAREHOUSE {{ var.value.SNOWFLAKE_WAREHOUSE }};
      USE DATABASE {{ var.value.SNOWFLAKE_DATABASE }};

      DROP SCHEMA IF EXISTS RAW     CASCADE;
      DROP SCHEMA IF EXISTS STAGING CASCADE;
      DROP SCHEMA IF EXISTS MART    CASCADE;
    """,
    split_statements=True,
    autocommit=True,
)


    create_schemas = SQLExecuteQueryOperator(
    task_id="create_schemas",
    conn_id="snowflake_default",
    sql="""
      CREATE SCHEMA IF NOT EXISTS RAW;
      CREATE SCHEMA IF NOT EXISTS STAGING;
      CREATE SCHEMA IF NOT EXISTS MART;
    """,
    split_statements=True,   # <-- key: runs each DDL separately
    autocommit=True,         # <-- safe for DDL
)

    create_raw_tables = SQLExecuteQueryOperator(
    task_id="create_raw_tables",
    conn_id="snowflake_default",
    sql=r"""
      CREATE TABLE IF NOT EXISTS RAW.RAW_PRICES (
        symbol STRING,
        ts NUMBER,
        open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT,
        raw_payload VARIANT,
        load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      );

      CREATE TABLE IF NOT EXISTS RAW.RAW_NEWS (
        symbol STRING,
        published_at TIMESTAMP_NTZ,
        headline STRING,
        sentiment FLOAT,
        raw_payload VARIANT,
        load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      );

      CREATE TABLE IF NOT EXISTS RAW.RAW_EARNINGS (
        symbol STRING,
        report_date DATE,
        actual_eps FLOAT,
        consensus_eps FLOAT,
        surprise_pct FLOAT,
        raw_payload VARIANT,
        load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      );
    """,
    split_statements=True,
    autocommit=True,
)


    BASE_ENV = {
    "SNOWFLAKE_ACCOUNT": "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER": "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD": "{{ var.value.SNOWFLAKE_PASSWORD }}",
    "SNOWFLAKE_WAREHOUSE": "{{ var.value.SNOWFLAKE_WAREHOUSE }}",
    "SNOWFLAKE_DATABASE": "{{ var.value.SNOWFLAKE_DATABASE }}",
    "SNOWFLAKE_ROLE": "{{ var.value.SNOWFLAKE_ROLE | default('') }}",
    "SNOWFLAKE_SCHEMA": "RAW",
    "FINNHUB_API_KEY": "{{ var.value.FINNHUB_API_KEY }}",
    "PYTHONPATH": "/opt/project",
    }

    ingest_prices = BashOperator(
        task_id="ingest_prices",
        bash_command=f'cd "{EXTRACT_DIR}" && python -u extract_prices.py',
        env=BASE_ENV,
    )
    ingest_news = BashOperator(
        task_id="ingest_news",
        bash_command=f'cd "{EXTRACT_DIR}" && python -u extract_news.py',
        env=BASE_ENV,
    )
    ingest_earnings = BashOperator(
        task_id="ingest_earnings",
        bash_command=f'cd "{EXTRACT_DIR}" && python -u extract_earnings.py',
        env=BASE_ENV,
    )

    dbt_env = {**BASE_ENV, "DBT_PROFILES_DIR": "/opt/project/dbt"}

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            'cd "/opt/project/dbt/marketpulse_dbt" && '
            # if you use packages.yml keep deps; otherwise you can drop this line
            'python -m dbt.cli.main deps && '
            'python -m dbt.cli.main run --select "staging+ marts+"'
        ),
        env=dbt_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f'cd "{DBT_DIR}" && '
            'python -m dbt.cli.main test --select "marts+"'
        ),
        env=dbt_env,
    )

    ensure_mart_ml_and_views = SQLExecuteQueryOperator(
    task_id="ensure_mart_ml_and_views",
    conn_id="snowflake_default",
    sql=r"""
      CREATE TABLE IF NOT EXISTS MART.ML_MODEL_METRICS (
        trained_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        symbol STRING,
        auc FLOAT,
        accuracy FLOAT,
        n_rows NUMBER,
        model_version STRING
      );

      CREATE TABLE IF NOT EXISTS MART.ML_PREDICTIONS_DAILY (
        date DATE,
        symbol STRING,
        p_up FLOAT,
        pred_label NUMBER(1),
        model_version STRING,
        inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      );

      CREATE OR REPLACE VIEW MART.LATEST_PREDICTIONS AS
      WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC, inserted_at DESC) rn
        FROM MART.ML_PREDICTIONS_DAILY
      )
      SELECT date, symbol, p_up, pred_label, model_version
      FROM ranked
      WHERE rn = 1;

      CREATE OR REPLACE VIEW MART.VW_PREDICTIONS_WITH_QC AS
      WITH last_metrics AS (
        SELECT symbol, MAX(trained_at) AS trained_at
        FROM MART.ML_MODEL_METRICS
        GROUP BY symbol
      )
      SELECT
        p.date,
        p.symbol,
        p.p_up,
        p.pred_label,
        m.auc,
        m.accuracy,
        m.n_rows,
        p.model_version
      FROM MART.LATEST_PREDICTIONS p
      LEFT JOIN last_metrics lm ON lm.symbol = p.symbol
      LEFT JOIN MART.ML_MODEL_METRICS m
        ON m.symbol = lm.symbol AND m.trained_at = lm.trained_at;
    """,
    split_statements=True,
    autocommit=True,
)

    ml_train_and_predict = BashOperator(
        task_id="ml_train_and_predict",
        bash_command=f'cd "{ML_DIR}" && python train_and_infer.py',
        env=BASE_ENV,
    )

    warm_streamlit = BashOperator(
    task_id="warm_streamlit",
    bash_command=(
        'bash -lc "for i in {1..90}; do '
        "curl -fsS http://streamlit:8501/_stcore/health | grep -Eiq '(ok|ready)' && exit 0; "
        '[ $(curl -fsS -o /dev/null -w %{http_code} http://streamlit:8501/) = 200 ] && exit 0; '
        'sleep 2; done; echo streamlit not ready >&2; exit 1"'
    ),
)
    
    drop_schemas >> create_schemas >> create_raw_tables >> [ingest_prices, ingest_news, ingest_earnings] \
    >> dbt_run >> dbt_test >> ensure_mart_ml_and_views >> ml_train_and_predict >> warm_streamlit

