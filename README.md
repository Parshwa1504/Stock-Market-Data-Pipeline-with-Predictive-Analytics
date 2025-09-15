# 📈 Stock Market Data Pipeline with Predictive Analytics

**An end-to-end, containerized ELT + ML project** that:
- ingests daily **prices, news, and earnings**,
- models clean features with **dbt**,
- trains a lightweight **Logistic Regression** signal per symbol,
- writes **metrics & predictions** back to **Snowflake**,
- orchestrates everything with **Apache Airflow**, and
- serves an interactive **Streamlit** dashboard.

> **Stack:** Docker & Docker Compose · Airflow 2.9 · dbt Core 1.7 (+ `dbt_utils`) · Snowflake · scikit-learn · pandas · Streamlit

---

## ✨ Highlights (TL;DR)

- ⏱️ **Automated nightly run (22:00 UTC)** via Airflow
- 🧹 **Clean, testable models** (staging → marts) with dbt + `dbt_utils` tests
- 🤖 **Per-symbol model** (Logistic Regression) writing AUC/Accuracy + predictions to Snowflake
- 🔭 **Streamlit app**: Overview • Symbol Explorer • Model QC • News & Earnings context
- 🐳 **One-command spin-up** with Docker Compose; isolated Airflow/DBT/Streamlit services
- 🧪 **Dev mode reset** (optional): drop & recreate `RAW`, `STAGING`, `MART` to start fresh during iteration

---

## 🖼️ Screenshots

### Airflow — End-to-End Pipeline  
![](Images/Airflow%20Pipeline.png)

### Airflow — Snowflake Connection  
![](Images/Airflow%20Connections.png)

### Airflow — Variables  
![](Images/Airflow%20Variables.png)

### Snowflake — Database Objects  
![](Images/Snowflake%20DataBase.png)

### Streamlit — Overview  
![](Images/Streamlit%20App%20Overview%20Page.png)

### Streamlit — Symbol Explorer  
![](Images/Streamlit%20App%20System%20Explorer%20page.png)

### Streamlit — Model QC  
![](Images/Streamlit%20Model%20QC%20Page.png)

### Architecture (high level)  
![](Images/Airflow%20Architecture.png)

---

## 🧭 How the System Works

1. **Extract (Python)**  
   `Data_Ingestion/` scripts call external APIs (e.g., Finnhub) and land raw payloads into **Snowflake → RAW**:
   - `RAW.RAW_PRICES`
   - `RAW.RAW_NEWS`
   - `RAW.RAW_EARNINGS`

2. **Transform (dbt)**  
   dbt builds **staging → intermediate → marts**:
   - Clean staging views (`stg_prices`, `stg_news`, `stg_earnings`)
   - Enrichment (`int_*`)
   - Facts & features in **MART**, incl. `features_daily`
   - **Tests**: `not_null`, `dbt_utils.unique_combination_of_columns` (e.g., `(symbol, date)`)

3. **Model (scikit-learn)**  
   A simple **Logistic Regression** trains **per symbol** and writes:
   - `MART.ML_MODEL_METRICS` (AUC, Accuracy, #rows, model_version)
   - `MART.ML_PREDICTIONS_DAILY`
   - Views:
     - `MART.LATEST_PREDICTIONS`
     - `MART.VW_PREDICTIONS_WITH_QC` (joins latest preds with last training metrics)

4. **Serve (Streamlit)**  
   The app reads from Snowflake and provides:
   - **Overview** KPIs and latest signals
   - **Symbol Explorer** (P(up) over time)
   - **Model QC** (AUC/Accuracy history)
   - **News & Earnings** context

5. **Orchestrate (Airflow DAG)**  
   `marketpulse_pipeline` chains everything with retries, logging, and a final **Streamlit warmup** against `http://streamlit:8501/_stcore/health`.

---

## 🗂️ Repository Structure

├─ airflow/
│ 	├─ dags/
│ 	 	└─ marketpulse_pipeline.py # main DAG
│ 	├─ scripts/
     	└─ seed_connections.sh
├─ Data_Ingestion/
│ 		├─ db_utils.py
│ 		├─ extract_news.py
│ 		├─ extract_prices.py	
│ 		├─  extract_earnings.py
├─ dbt/
│ ├─ marketpulse_dbt/
│ 	   ├─ models/
│ 	   		├─ staging/
│      			└─ schema.yml
│      			└─ stg_earnings.sql
│      			└─ stg_news.sql
│      			└─ stg_prices.sql
│  	   		├─ intermediate
│      			└─ int_earnings_clean.sql
│      			└─ int_news_daily.sql
│      			└─ int_prices_enriched.sql
│ 	   		├─ marts/
│      			└─ fct_earnings.sql
│      			└─ fct_news_daily.sql
│      			└─ fct_prices_daily.sql
│      			└─ features_daily.sql
│      		├─ sources.yml
│      		├─ marts_schema.yml
│  	   ├─ dbt_project.yml
│ 	   ├─ profiles.yml 
│ 	   ├─ packages.yml 
│ ├─ .user.yml
│ ├─ .profiles.yml
├─ ml/
│ └─ train_and_infer.py # trains & writes metrics/predictions
├─ stock-app/ # Streamlit UI
│ ├─ app.py
│ ├─ db.py
│ ├─ requirements.txt
│ ├─ .env # app-only env
├─ docker-compose.yml
├─ .env # project env
├─ requirements-airflow.txt

## 🧪 The Airflow DAG (what runs & in what order)

**DAG id:** `marketpulse_pipeline` • **Schedule:** daily `0 22 * * *` (22:00 UTC) • `catchup=False`

1. `drop_schemas` *(dev convenience; destructive)*  
   Drops `RAW`, `STAGING`, `MART` with `CASCADE` to guarantee a clean run while iterating.
2. `create_schemas` → `create_raw_tables`
3. `ingest_prices` · `ingest_news` · `ingest_earnings`
4. `dbt_run` → `dbt_test`
5. `ensure_mart_ml_and_views` (creates ML tables & views)
6. `ml_train_and_predict` (scikit-learn per symbol)
7. `warm_streamlit` (health-checks the separate Streamlit container)

> **Production note:** Replace step 1 with **idempotent upserts** (e.g., MERGE) and incremental models.

---

## 🚀 Getting Started

### 1) Prerequisites
  - Docker & Docker Compose
  - Snowflake account (database used: `MARKETPULSE`)
  - A market data API key (e.g., **FINNHUB_API_KEY**)

### 2) Environment Variables

  - bash
  - Copy code
  - docker compose up -d
  - Airflow Webserver → http://localhost:8080

  - Streamlit → http://localhost:8501

  - If 8501 is occupied, adjust the port mapping in docker-compose.yml (e.g., 8502:8501).

### 4) Airflow setup (one-time)
  -Connection: snowflake_default

  - Fill Login/Password

  - Put this in Extra:

  - json
 - Copy code
{
  "account": "YOUR_ACCOUNT",
  "warehouse": "COMPUTE_WH",
  "database": "MARKETPULSE",
  "role": "ACCOUNTADMIN",
  "insecure_mode": false
}

####  Variables (used by the DAG & profiles.yml in dbt):

  - vbnet
  - Copy code
  - FINNHUB_API_KEY
  - SNOWFLAKE_ACCOUNT
  - SNOWFLAKE_DATABASE
  - SNOWFLAKE_PASSWORD
  - SNOWFLAKE_ROLE
  - SNOWFLAKE_SCHEMA   (optional; loaders default to RAW)
  - SNOWFLAKE_USER
  - SNOWFLAKE_WAREHOUSE
  - The DAG sets DBT_PROFILES_DIR=/opt/project/dbt so dbt uses the included profiles.yml that templates from Airflow Variables.

### 5) Run the pipeline
  - In Airflow, turn on the DAG or Trigger DAG.

  - Watch task logs; the final task succeeds once http://streamlit:8501/_stcore/health is ready.

### 📚 Snowflake Model (what gets created)
  - RAW
  - RAW_PRICES, RAW_NEWS, RAW_EARNINGS

  - STAGING
  - stg_prices, stg_news, stg_earnings + int_* helpers

  - MART

  - Facts: fct_prices_daily, fct_news_daily, fct_earnings

  - Features: features_daily (tested for (symbol, date) uniqueness with dbt_utils)

  - ML tables: ML_MODEL_METRICS, ML_PREDICTIONS_DAILY
  
  - Views: LATEST_PREDICTIONS, VW_PREDICTIONS_WITH_QC

### 🧠 Modeling & ML Details
  - dbt

  - Clear staging → marts layer separation

#### Tests:

  - not_null on key columns

  - dbt_utils.unique_combination_of_columns on (symbol, date) in features_daily

  - ML

  - Logistic Regression per symbol (baseline)

  - Stores AUC, Accuracy, # training rows, model_version

  - Produces daily p_up probabilities and labeled predictions

  - A LATEST_PREDICTIONS view always exposes the most recent signal per symbol

### 🖥️ Streamlit App (what you can explore)
  - Overview: KPIs and latest signals table

  - Symbol Explorer: trend of P(up) with quick filters

  - Model QC: AUC/Accuracy over time per symbol

  - News & Earnings: contextual tables to explain moves

### 🛠️ Troubleshooting
  #### Streamlit warmup fails

  - Ensure the service name is streamlit in docker-compose.yml

  - Check logs: docker compose logs -f streamlit

  - If you see “port already allocated”, change the host mapping (e.g., 8502:8501)

  - ModuleNotFoundError: sklearn in Airflow

  - Add scikit-learn to airflow/requirements-airflow.txt and rebuild, or

  - pip install scikit-learn inside the Airflow image (prefer rebuild)

  - dbt package errors

  - Keep packages.yml at dbt_utils: ">=1.3.0,<2.0.0" and run dbt deps

  - Permissions in Snowflake

  - The configured role needs USAGE on database/warehouse and DDL/DML rights on schemas/tables

