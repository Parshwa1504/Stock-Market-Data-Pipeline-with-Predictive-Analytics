import os, requests, json
from dotenv import load_dotenv
from db_utils import get_snowflake_connection

load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")

def fetch_earnings(symbol: str):
    url = "https://finnhub.io/api/v1/stock/earnings"
    resp = requests.get(url, params={"symbol": symbol, "token": API_KEY})
    resp.raise_for_status()
    return resp.json()   # list of dicts

def load_to_snowflake(symbol: str, earnings: list[dict]):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO RAW.RAW_EARNINGS
            (symbol, report_date, actual_eps, consensus_eps, surprise_pct, raw_payload)
        SELECT %s, TO_DATE(%s), %s, %s, %s, PARSE_JSON(%s)
        """
        inserted = 0
        for e in earnings or []:
            cur.execute(sql, (
                symbol,
                e.get("period"),          # "YYYY-MM-DD"
                e.get("actual"),
                e.get("estimate"),
                e.get("surprisePercent"),
                json.dumps(e),
            ))
            inserted += 1
        conn.commit()
        print(f"Inserted {inserted} earnings rows for {symbol}")
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    for sym in ["AAPL","MSFT","GOOGL","AMZN"]:
        data = fetch_earnings(sym)
        load_to_snowflake(sym, data)
