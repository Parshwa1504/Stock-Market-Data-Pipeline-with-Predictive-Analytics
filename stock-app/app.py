import os
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from db import fetch_df

# -----------------------------
# Page & global styles
# -----------------------------
st.set_page_config(
    page_title="Stock Signals & Model QC",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Subtle CSS polish (rounded cards, nicer tables)
st.markdown("""
<style>
/* tighten top spacing & make tables crisper */
.block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
.dataframe td {font-size: 0.92rem;}
hr {border: 0; height: 1px; background: linear-gradient(to right,#333, #777, #333);}
.small {font-size: 0.9rem; opacity: 0.85;}
.badge {display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 0.8rem; font-weight: 700;}
.badge.green {background:#1f5130; color:#b4f5c5;}
.badge.amber {background:#5c4b22; color:#ffe6a7;}
.badge.red {background:#5a2a2a; color:#ffb3b3;}
.card {background: var(--secondary-background-color); border-radius: 14px; padding: 14px 16px; border: 1px solid rgba(255,255,255,0.06);}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# Helpers (cached queries)
# -----------------------------
@st.cache_data(ttl=300)
def load_latest_predictions():
    sql = """
      select date, symbol, p_up, pred_label, auc, accuracy, n_rows, model_version
      from MART.VW_PREDICTIONS_WITH_QC
      order by symbol
    """
    df = fetch_df(sql)
    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"])
        df["P_UP"] = df["P_UP"].astype(float)
        # tidy
        for col in ["AUC", "ACCURACY"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

@st.cache_data(ttl=300)
def load_history(symbol, days=180):
    sql = f"""
      select date, symbol, p_up, pred_label, model_version
      from MART.ML_PREDICTIONS_DAILY
      where symbol = %(sym)s
        and date >= dateadd('day', -{int(days)}, current_date())
      order by date
    """
    df = fetch_df(sql, {"sym": symbol})
    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"])
        df["P_UP"] = df["P_UP"].astype(float)
    return df

@st.cache_data(ttl=300)
def load_metrics(symbol=None, limit_rows=300):
    where = "where 1=1"
    params = {}
    if symbol:
        where += " and symbol = %(sym)s"
        params["sym"] = symbol
    sql = f"""
      select trained_at, symbol, auc, accuracy, n_rows, model_version
      from MART.ML_MODEL_METRICS
      {where}
      order by trained_at desc
      limit {int(limit_rows)}
    """
    df = fetch_df(sql, params)
    if not df.empty:
        df["TRAINED_AT"] = pd.to_datetime(df["TRAINED_AT"])
        for c in ["AUC", "ACCURACY"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@st.cache_data(ttl=300)
def load_news(symbol=None, days=60):
    # Optional table: MART.FCT_NEWS (headlines, published_at, url, symbol)
    where, params = "where published_at >= dateadd('day', -%(d)s, current_date())", {"d": days}
    if symbol:
        where += " and symbol = %(sym)s"
        params["sym"] = symbol
    sql = f"""
      select published_at, symbol, source, headline, url
      from MART.FCT_NEWS
      {where}
      order by published_at desc
      limit 200
    """
    try:
        df = fetch_df(sql, params)
        if not df.empty:
            df["PUBLISHED_AT"] = pd.to_datetime(df["PUBLISHED_AT"])
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_earnings(symbol=None, lookback_quarters=8):
    # Optional table: MART.FCT_EARNINGS (symbol, report_date, surprise_pct, eps_actual, eps_estimate)
    where, params = "where 1=1", {}
    if symbol:
        where += " and symbol = %(sym)s"
        params["sym"] = symbol
    sql = f"""
      select report_date, symbol, surprise_pct, eps_actual, eps_estimate
      from MART.FCT_EARNINGS
      {where}
      order by report_date desc
      limit {int(lookback_quarters * 20)}
    """
    try:
        df = fetch_df(sql, params)
        if not df.empty:
            df["REPORT_DATE"] = pd.to_datetime(df["REPORT_DATE"])
            for c in ["SURPRISE_PCT","EPS_ACTUAL","EPS_ESTIMATE"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()

def confidence_badge(p):
    if pd.isna(p): return '<span class="badge">—</span>'
    if p >= 0.70: return '<span class="badge green">STRONG</span>'
    if p >= 0.60: return '<span class="badge amber">MODERATE</span>'
    return '<span class="badge red">WEAK</span>'

# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.title("⚙️ Controls")
latest_df = load_latest_predictions()

symbols = sorted(latest_df["SYMBOL"].unique()) if not latest_df.empty else []
sym = st.sidebar.selectbox("Symbol", symbols, index=0 if symbols else None)
days_hist = st.sidebar.slider("History window (days)", 30, 365, 180, step=30)
min_auc = st.sidebar.slider("Min AUC (quality filter)", 0.50, 0.95, 0.65, step=0.01)

st.sidebar.markdown("---")
st.sidebar.caption("Data sources: MART.VW_PREDICTIONS_WITH_QC, ML_PREDICTIONS_DAILY, ML_MODEL_METRICS")

# -----------------------------
# Header
# -----------------------------
st.title("📈 Stock ML Signals & Quality Control")
st.caption("Daily directional signal (P↑) with model quality metrics and supporting context.")

# -----------------------------
# Top KPIs
# -----------------------------
with st.container():
    if latest_df.empty:
        st.info("No data found. Ensure Day 2 populated MART.ML_PREDICTIONS_DAILY and MLS metrics, and created MART.VW_PREDICTIONS_WITH_QC.")
    else:
        # compute KPIs with quality filter
        dfq = latest_df.copy()
        if "AUC" in dfq.columns:
            dfq = dfq[dfq["AUC"].fillna(0) >= min_auc]

        total_syms = len(latest_df["SYMBOL"].unique())
        syms_q = len(dfq["SYMBOL"].unique())
        up_rate = (dfq["PRED_LABEL"] == 1).mean() if not dfq.empty else np.nan
        avg_auc = dfq["AUC"].replace([np.inf,-np.inf], np.nan).dropna().mean()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Symbols (all)", f"{total_syms}")
        c2.metric("Symbols (AUC ≥ filter)", f"{syms_q}")
        c3.metric("% labeled UP", f"{(100*up_rate):.1f}%" if not np.isnan(up_rate) else "—")
        c4.metric("Avg AUC (filtered)", f"{avg_auc:.3f}" if pd.notnull(avg_auc) else "—")

st.markdown("<hr/>", unsafe_allow_html=True)

# -----------------------------
# Tabs
# -----------------------------
tab_overview, tab_symbol, tab_qc, tab_news, tab_about = st.tabs(
    ["Overview", "Symbol Explorer", "Model QC", "News & Earnings", "About"]
)

# === Overview: latest table, sortable, with confidence badges ===
with tab_overview:
    st.subheader("Latest Signals (with QC)")
    if latest_df.empty:
        st.info("No latest rows.")
    else:
        show_df = latest_df.copy()

        # apply quality filter for display
        show_df = show_df[show_df["AUC"].fillna(0) >= min_auc]

        # add confidence label
        show_df["Confidence"] = show_df["P_UP"].apply(lambda x: confidence_badge(float(x)))
        # pretty cols
        show_df["P_UP_%"] = (show_df["P_UP"] * 100).round(2)
        show_df.rename(columns={
            "DATE": "Date",
            "SYMBOL": "Symbol",
            "P_UP_%": "P(up) %",
            "PRED_LABEL": "Label",
            "AUC": "AUC",
            "ACCURACY": "Accuracy",
            "N_ROWS": "#Train Rows",
            "MODEL_VERSION": "Model Ver"
        }, inplace=True)

        # order & select columns
        cols = ["Date","Symbol","P(up) %","Label","Confidence","AUC","Accuracy","#Train Rows","Model Ver"]
        show_df = show_df[cols].sort_values(["P(up) %"], ascending=False)

        # render with HTML for badges
        st.write(
            show_df.to_html(escape=False, index=False),
            unsafe_allow_html=True
        )

# === Symbol Explorer: history chart + latest snapshot for selected symbol ===
with tab_symbol:
    st.subheader("Symbol Explorer")
    if not sym:
        st.info("Select a symbol in the sidebar.")
    else:
        colA, colB = st.columns([2,1])

        # Latest row for the selected symbol
        latest_row = latest_df[latest_df["SYMBOL"] == sym]
        if latest_row.empty:
            st.warning(f"No latest prediction for {sym}.")
        else:
            r = latest_row.iloc[0]
            with colB:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.markdown(f"**{sym} — Latest**")
                st.metric("P(up)", f"{r['P_UP']:.3f}")
                st.metric("Label", int(r["PRED_LABEL"]))
                st.metric("AUC", f"{float(r['AUC']):.3f}" if pd.notnull(r["AUC"]) else "—")
                st.metric("Accuracy", f"{float(r['ACCURACY']):.3f}" if pd.notnull(r["ACCURACY"]) else "—")
                st.metric("Model", r["MODEL_VERSION"])
                st.markdown(confidence_badge(float(r["P_UP"])), unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)

        # History chart
        hist = load_history(sym, days=days_hist)
        with colA:
            if hist.empty:
                st.info(f"No historical predictions for {sym}.")
            else:
                fig = px.line(
                    hist, x="DATE", y="P_UP",
                    title=f"{sym} — P(up) over time",
                    markers=True
                )
                fig.update_layout(height=360, yaxis_tickformat=".0%")
                st.plotly_chart(fig, use_container_width=True)

        # History table
        if not hist.empty:
            tbl = hist.copy()
            tbl["P_UP_%"] = (tbl["P_UP"] * 100).round(2)
            tbl["LABEL"] = tbl["PRED_LABEL"]
            tbl = tbl[["DATE","SYMBOL","P_UP_%","LABEL","MODEL_VERSION"]].rename(columns={
                "DATE":"Date","SYMBOL":"Symbol","P_UP_%":"P(up) %","LABEL":"Label","MODEL_VERSION":"Model Ver"
            })
            st.dataframe(tbl, use_container_width=True, hide_index=True)

# === Model QC: AUC & Accuracy trends ===
with tab_qc:
    st.subheader("Model Training Metrics")
    if not sym:
        st.info("Select a symbol in the sidebar.")
    else:
        met = load_metrics(sym)
        if met.empty:
            st.info("No metrics yet. Make sure you called write_metrics() in Day 2.")
        else:
            c1, c2 = st.columns(2)
            fig1 = px.line(met.sort_values("TRAINED_AT"), x="TRAINED_AT", y="AUC", title=f"{sym}: AUC over time", markers=True)
            fig1.update_layout(height=320)
            c1.plotly_chart(fig1, use_container_width=True)

            fig2 = px.line(met.sort_values("TRAINED_AT"), x="TRAINED_AT", y="ACCURACY", title=f"{sym}: Accuracy over time", markers=True)
            fig2.update_layout(height=320)
            c2.plotly_chart(fig2, use_container_width=True)

            st.dataframe(
                met.rename(columns={
                    "TRAINED_AT":"Trained At","SYMBOL":"Symbol","AUC":"AUC","ACCURACY":"Accuracy","N_ROWS":"#Train Rows","MODEL_VERSION":"Model Ver"
                }),
                use_container_width=True, hide_index=True
            )

# === News & Earnings (optional tables) ===
with tab_news:
    st.subheader("Recent News & Earnings (Optional)")
    news_df = load_news(sym, days=60)
    earn_df = load_earnings(sym, lookback_quarters=8)

    if news_df.empty and earn_df.empty:
        st.info("Optional tables not found or empty: MART.FCT_NEWS / MART.FCT_EARNINGS.")
    else:
        if not news_df.empty:
            st.markdown("**News (last 60 days)**")
            ndf = news_df.copy()
            ndf = ndf.rename(columns={
                "PUBLISHED_AT":"Published","SYMBOL":"Symbol","SOURCE":"Source","HEADLINE":"Headline","URL":"URL"
            })
            # clickable headlines
            ndf["Headline"] = ndf.apply(lambda r: f'<a href="{r["URL"]}" target="_blank">{r["Headline"]}</a>', axis=1)
            ndf = ndf[["Published","Symbol","Source","Headline"]]
            st.write(ndf.to_html(escape=False, index=False), unsafe_allow_html=True)

        st.markdown("<br/>", unsafe_allow_html=True)

        if not earn_df.empty:
            st.markdown("**Earnings (last 8 quarters)**")
            edf = earn_df.copy()
            edf["SURPRISE_%"] = (edf["SURPRISE_PCT"]).round(2)
            edf = edf.rename(columns={
                "REPORT_DATE":"Report Date","SYMBOL":"Symbol","SURPRISE_%":"Surprise %","EPS_ACTUAL":"EPS Actual","EPS_ESTIMATE":"EPS Estimate"
            })
            edf = edf[["Report Date","Symbol","EPS Actual","EPS Estimate","Surprise %"]]
            st.dataframe(edf, use_container_width=True, hide_index=True)

# === About: quick explainer for recruiters ===
with tab_about:
    st.subheader("About this App")
    st.markdown("""
- **Purpose**: End-to-end stock signal pipeline — ingest ➜ transform (dbt) ➜ model (Sklearn) ➜ serve (Streamlit).
- **Signals**: Daily **P(up)** (probability the next day closes higher) + predicted label.
- **Quality Control**: AUC & Accuracy logged per training run; view trends under **Model QC**.
- **Data**: Snowflake schema `MART` (features, predictions, metrics). Optional: `FCT_NEWS`, `FCT_EARNINGS`.
- **How to use**: Adjust **AUC filter** (left) to only show high-quality signals; use **Symbol Explorer** for history.
""")
    st.markdown('<span class="small">Tip: Add more symbols and longer history to make this dashboard shine in demos.</span>', unsafe_allow_html=True)
