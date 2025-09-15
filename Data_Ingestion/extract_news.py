import os, requests, json, datetime as dt
from dotenv import load_dotenv
from db_utils import get_snowflake_connection

load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")

def fetch_news(symbol: str, days_back=30):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back)
    url = "https://finnhub.io/api/v1/company-news"
    resp = requests.get(url, params={"symbol": symbol, "from": str(start), "to": str(today), "token": API_KEY})
    resp.raise_for_status()
    return resp.json()

def load_to_snowflake(symbol: str, articles: list[dict]):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO RAW.RAW_NEWS
            (symbol, published_at, headline, sentiment, raw_payload)
        SELECT %s, TO_TIMESTAMP_NTZ(%s), %s, %s, PARSE_JSON(%s)
        """
        inserted = 0
        for a in articles or []:
            cur.execute(sql, (symbol, a.get("datetime"), a.get("headline"), None, json.dumps(a)))
            inserted += 1
        conn.commit()
        print(f"Inserted {inserted} news rows for {symbol}")
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    for sym in ["AAPL","MSFT","GOOGL","AMZN"]:
        arts = fetch_news(sym, days_back=30)
        load_to_snowflake(sym, arts)
