"""
af_to_bigquery.py
Pull AppsFlyer installs, in-app events, and cost aggregate data
Write to BigQuery raw_appsflyer dataset
Location: dbt-gaming-analytics/scripts/af_to_bigquery.py
"""

import requests
import pandas as pd
from io import StringIO
from google.cloud import bigquery
from datetime import datetime, timedelta
import time
import os
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

BASE_URL        = "https://hq1.appsflyer.com/api/raw-data/export/app"
BASE_URL_AGG    = "https://hq1.appsflyer.com/api/agg-data/export/app"

REPORTS = {
    "raw_af_installs": "installs_report/v5",
    "raw_af_events":   "in_app_events_report/v5",
}

REPORTS_AGG = {
    "raw_af_cost":  "partners_by_date_report/v5",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename AF Title Case columns to snake_case for BigQuery"""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


def pull_report(report_type: str, endpoint: str, base_url: str) -> pd.DataFrame:
    """Call AppsFlyer Pull API and return a DataFrame"""
    url = f"{base_url}/{APP_ID}/{endpoint}"
    params = {
        "from":     START_DATE,
        "to":       END_DATE,
        "timezone": "UTC",
        "currency": "USD",
    }
    headers = {
        "Authorization": f"Bearer {API_TOKEN}"
    }

    print(f"[{report_type}] Pulling data from {START_DATE} to {END_DATE} ...")
    resp = requests.get(url, params=params, headers=headers, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"API request failed: {resp.status_code} - {resp.text[:300]}")

    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    print(f"[{report_type}] Done — {len(df)} rows, {len(df.columns)} columns")
    return df


def write_to_bq(df: pd.DataFrame, table_name: str):
    """Write DataFrame to BigQuery in append mode"""
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

    # 1. Install attribution
    df_installs = pull_report("raw_af_installs", REPORTS["raw_af_installs"], BASE_URL)
    df_installs = normalize_columns(df_installs)
    print(f"[raw_af_installs] Writing {len(df_installs)} rows to BQ ...")
    write_to_bq(df_installs, "raw_af_installs")

    time.sleep(2)

    # 2. In-app events
    df_events = pull_report("raw_af_events", REPORTS["raw_af_events"], BASE_URL)
    df_events = normalize_columns(df_events)
    print(f"[raw_af_events] Writing {len(df_events)} rows to BQ ...")
    write_to_bq(df_events, "raw_af_events")

    time.sleep(2)

    # 3. Cost aggregate report (partners by date)
    df_cost = pull_report("raw_af_cost", REPORTS_AGG["raw_af_cost"], BASE_URL_AGG)
    df_cost = normalize_columns(df_cost)
    print(f"[raw_af_cost] Writing {len(df_cost)} rows to BQ ...")
    write_to_bq(df_cost, "raw_af_cost")

    print("\n=== All done ===")


if __name__ == "__main__":
    main()