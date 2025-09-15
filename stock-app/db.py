import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        client_session_keep_alive=True,
    )

def fetch_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    finally:
        cur.close()
        conn.close()
