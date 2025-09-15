# replace the whole file with this version

import os
import pandas as pd
from dotenv import load_dotenv
from snowflake import connector
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score

load_dotenv()

def snow_conn():
    return connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MART",
    )

FEATURES = ["ret_d1","ret_5d","vol_20d","articles_1d","articles_3d","surprise_pct"]

def load_features(conn, lookback_days=365*2):
    q = f"""
      select date, symbol,
             {", ".join(FEATURES)},
             label_up_next_day
      from MART.FEATURES_DAILY
      where date >= dateadd('day', -{lookback_days}, (select max(date) from MART.FEATURES_DAILY))
      order by symbol, date
    """
    return pd.read_sql(q, conn)

def _both_classes(y):
    s = pd.Series(y)
    return s.nunique() >= 2

def _fit_logreg(X, y):
    clf = LogisticRegression(max_iter=500, class_weight="balanced")
    clf.fit(X, y)
    return clf

def train_per_symbol(df: pd.DataFrame, min_rows=12):
    models, metrics = {}, {}
    for sym, g in df.groupby("SYMBOL"):
        g = g.dropna(subset=["LABEL_UP_NEXT_DAY"]).copy()
        if len(g) < min_rows:
            continue

        X_all = g[[c.upper() for c in FEATURES]].fillna(0.0)
        y_all = g["LABEL_UP_NEXT_DAY"].astype(int)

        if not _both_classes(y_all):
            # cannot train a classifier with one class in entire history
            continue

        # try to find a time-based split (60–90%) that has both classes in train
        cut = None
        for frac in [0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6]:
            c = max(1, int(len(g) * frac))
            if _both_classes(y_all.iloc[:c]):
                cut = c
                break

        if cut is None:
            # if we never found a split, fit on all data (ok for a daily batch demo)
            clf = _fit_logreg(X_all, y_all)
            auc = acc = None
        else:
            X_tr, X_te = X_all.iloc[:cut], X_all.iloc[cut:]
            y_tr, y_te = y_all.iloc[:cut], y_all.iloc[cut:]
            clf = _fit_logreg(X_tr, y_tr)
            if len(X_te) and _both_classes(y_te):
                proba = clf.predict_proba(X_te)[:, 1]
                auc = float(roc_auc_score(y_te, proba))
                acc = float(accuracy_score(y_te, (pd.Series(proba) >= 0.55).astype(int)))
            else:
                auc = acc = None

        models[sym] = clf
        metrics[sym] = {"auc": auc, "acc": acc, "n": len(g)}
    return models, metrics

def write_metrics(conn, metrics, model_version="v1"):
    cur = conn.cursor()
    try:
        for sym, m in metrics.items():
            cur.execute("""
              insert into MART.ML_MODEL_METRICS (symbol, auc, accuracy, n_rows, model_version)
              select %s, %s, %s, %s, %s
            """, (sym, m.get("auc"), m.get("acc"), m.get("n"), model_version))
        conn.commit()
        print(f"logged {len(metrics)} model metrics")
    finally:
        cur.close()

def write_predictions(conn, df_feats, per_sym_models, model_version="v1"):
    cur = conn.cursor()
    try:
        latest_date = df_feats["DATE"].max()
        today = df_feats[df_feats["DATE"] == latest_date].copy()
        rows = 0
        for sym, g in today.groupby("SYMBOL"):
            clf = per_sym_models.get(sym)
            if clf is None:
                continue
            X = g[[c.upper() for c in FEATURES]].fillna(0.0)
            proba = clf.predict_proba(X)[:, 1]
            pred = (proba >= 0.55).astype(int)
            for p_up, lbl in zip(proba, pred):
                cur.execute("""
                  insert into MART.ML_PREDICTIONS_DAILY (date, symbol, p_up, pred_label, model_version)
                  select %s, %s, %s, %s, %s
                """, (latest_date, sym, float(p_up), int(lbl), model_version))
                rows += 1
        conn.commit()
        print(f"wrote {rows} predictions for {latest_date}")
    finally:
        cur.close()

if __name__ == "__main__":
    conn = snow_conn()
    feats = load_features(conn)
    per_models, per_metrics = train_per_symbol(feats, min_rows=12)
    print("Per-symbol models trained:", len(per_models), "| sample metrics:", dict(list(per_metrics.items())[:3]))
    write_metrics(conn, per_metrics, model_version="v1")
    write_predictions(conn, feats, per_models, model_version="v1")
    conn.close()
