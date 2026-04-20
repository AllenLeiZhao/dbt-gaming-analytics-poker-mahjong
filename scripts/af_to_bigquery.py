"""
af_to_bigquery.py
Pull AppsFlyer data and write to BigQuery raw_appsflyer dataset
Reports:
  1. installs_report       - user-level install attribution
  2. in_app_events_report  - user-level in-app events
  3. organic_installs      - organic install attribution
  4. partners_by_date      - daily cost/revenue aggregate
  5. cohort_report         - cohort ROI by Day 0/1/3/7/15/30
Location: dbt-gaming-analytics/scripts/af_to_bigquery.py
"""

import requests
import pandas as pd
from io import StringIO
from google.cloud import bigquery
from datetime import datetime, timedelta
import time
import os
import json
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────
APP_ID     = "com.cwjoy.pokermahjong"
API_TOKEN  = os.getenv("AF_API_TOKEN")
PROJECT_ID = "game-analytics-492418"
DATASET    = "raw_appsflyer"

# Pull last 30 days
END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
# ─────────────────────────────────────────────────────────

# API base URLs
BASE_URL_RAW    = "https://hq1.appsflyer.com/api/raw-data/export/app"
BASE_URL_AGG    = "https://hq1.appsflyer.com/api/agg-data/export/app"
BASE_URL_COHORT = "https://hq1.appsflyer.com/api/cohorts/v1/data/app"

# Raw data report endpoints
RAW_REPORTS = {
    "raw_af_installs":         "installs_report/v5",
    "raw_af_events":           "in_app_events_report/v5",
    "raw_af_organic_installs": "organic_installs_report/v5",
}

# Aggregate report endpoints
AGG_REPORTS = {
    "raw_af_cost": "partners_by_date_report/v5",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename AF columns to snake_case for BigQuery compatibility"""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("/", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("%", "pct")
        .str.replace(".", "_")
    )
    return df


def pull_raw_report(report_type: str, endpoint: str, base_url: str) -> pd.DataFrame:
    """Call AppsFlyer Pull API (GET) and return a DataFrame"""
    url = f"{base_url}/{APP_ID}/{endpoint}"
    params = {
        "from":     START_DATE,
        "to":       END_DATE,
        "timezone": "UTC",
        "currency": "USD",
    }
    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    print(f"[{report_type}] Pulling data from {START_DATE} to {END_DATE} ...")
    resp = requests.get(url, params=params, headers=headers, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"API request failed: {resp.status_code} - {resp.text[:300]}")

    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    print(f"[{report_type}] Done — {len(df)} rows, {len(df.columns)} columns")
    return df


def pull_cohort_report() -> pd.DataFrame:
    """Call AppsFlyer Cohort API (POST) and return a DataFrame"""
    url = f"{BASE_URL_COHORT}/{APP_ID}?format=csv"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    payload = {
        "cohort_type":        "user_acquisition",
        "min_cohort_size":    1,
        "preferred_timezone": False,
        "from":               START_DATE,
        "to":                 END_DATE,
        "aggregation_type":   "cumulative",
        "per_user":           True,
        "groupings":          ["pid", "date"],
        "kpis":               ["revenue", "roi", "roas"],
        "filters": {
            "period": [0, 1, 3, 7, 15, 30]
        }
    }

    print(f"[raw_af_cohort] Pulling cohort data from {START_DATE} to {END_DATE} ...")
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)

    if resp.status_code != 200:
        print(f"  WARNING: Cohort API returned {resp.status_code} - {resp.text[:300]}")
        return pd.DataFrame()

    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    print(f"[raw_af_cohort] Done — {len(df)} rows, {len(df.columns)} columns")
    return df


def write_to_bq(df: pd.DataFrame, table_name: str):
    """Write DataFrame to BigQuery in append mode"""
    if df.empty:
        print(f"[{table_name}] Empty DataFrame, skipping BQ write.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"[{table_name}] Successfully written {len(df)} rows to {table_ref}")


def main():
    print("=== AppsFlyer → BigQuery ===")
    print(f"App ID : {APP_ID}")
    print(f"Project: {PROJECT_ID}.{DATASET}")
    print(f"Date range: {START_DATE} to {END_DATE}\n")

    # 1. Install attribution (non-organic)
    df = pull_raw_report("raw_af_installs", RAW_REPORTS["raw_af_installs"], BASE_URL_RAW)
    write_to_bq(normalize_columns(df), "raw_af_installs")
    time.sleep(2)

    # 2. In-app events
    df = pull_raw_report("raw_af_events", RAW_REPORTS["raw_af_events"], BASE_URL_RAW)
    write_to_bq(normalize_columns(df), "raw_af_events")
    time.sleep(2)

    # 3. Organic installs
    df = pull_raw_report("raw_af_organic_installs", RAW_REPORTS["raw_af_organic_installs"], BASE_URL_RAW)
    write_to_bq(normalize_columns(df), "raw_af_organic_installs")
    time.sleep(2)

    # 4. Cost aggregate (partners by date)
    df = pull_raw_report("raw_af_cost", AGG_REPORTS["raw_af_cost"], BASE_URL_AGG)
    write_to_bq(normalize_columns(df), "raw_af_cost")
    time.sleep(2)

    # 5. Cohort ROI (Day 0/1/3/7/15/30)
    df = pull_cohort_report()
    if not df.empty:
        write_to_bq(normalize_columns(df), "raw_af_cohort")
    else:
        print("[raw_af_cohort] No data returned, skipping.")

    print("\n=== All done ===")


if __name__ == "__main__":
    main()