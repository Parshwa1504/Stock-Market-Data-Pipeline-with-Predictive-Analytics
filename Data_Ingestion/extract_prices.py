import yfinance as yf
import json
from db_utils import get_snowflake_connection

def fetch_prices_yahoo(symbol: str, period="1mo", interval="1d"):
    tkr = yf.Ticker(symbol)
    hist = tkr.history(period=period, interval=interval)
    hist.reset_index(inplace=True)
    t = [int(row["Date"].timestamp()) for _, row in hist.iterrows()]
    return {
        "s": "ok", "t": t,
        "o": hist["Open"].tolist(),
        "h": hist["High"].tolist(),
        "l": hist["Low"].tolist(),
        "c": hist["Close"].tolist(),
        "v": hist["Volume"].tolist(),
        "_provider": "yahoo"
    }

def load_to_snowflake(symbol: str, data: dict):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO RAW.RAW_PRICES
          (symbol, ts, open, high, low, close, volume, raw_payload)
        SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
        """
        payload = json.dumps(data)
        inserted = 0
        for ts, o, h, l, c, v in zip(data["t"], data["o"], data["h"], data["l"], data["c"], data["v"]):
            cur.execute(sql, (symbol, ts, o, h, l, c, v, payload))
            inserted += 1
        conn.commit()
        print(f"Inserted {inserted} rows for {symbol} (provider: yahoo)")
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    for sym in ["AAPL","MSFT","GOOGL","AMZN"]:
        candles = fetch_prices_yahoo(sym, period="1mo", interval="1d")
        load_to_snowflake(sym, candles)
