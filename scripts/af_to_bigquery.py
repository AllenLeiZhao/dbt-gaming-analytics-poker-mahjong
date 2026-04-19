"""
af_to_bigquery.py
Pull AppsFlyer installs (attribution) + in-app events data and write to BigQuery dbt_prod dataset
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
API_TOKEN = os.getenv("AF_API_TOKEN")
PROJECT_ID = "game-analytics-492418"
DATASET = "raw_appsflyer"

# Pull last 30 days
END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
# ─────────────────────────────────────────────────────────

BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app"

REPORTS = {
    "raw_af_installs": "installs_report/v5",
    "raw_af_events":   "in_app_events_report/v5",
}

# Core fields for install attribution (AF uses Title Case with spaces)
INSTALL_COLUMNS = [
    "AppsFlyer ID", "Advertising ID", "Install Time",
    "Media Source", "Campaign", "Adset", "Ad", "Channel",
    "Country Code", "Platform", "App Version",
    "Is Retargeting", "Attribution Lookback", "Install Time",
]

# Core fields for in-app events
EVENT_COLUMNS = [
    "AppsFlyer ID", "Advertising ID", "Install Time", "Event Time",
    "Event Name", "Event Value", "Event Revenue", "Event Revenue Currency",
    "Media Source", "Campaign", "Country Code", "Platform",
]


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


def pull_report(report_type: str, endpoint: str) -> pd.DataFrame:
    """Call AppsFlyer Pull API and return a DataFrame"""
    url = f"{BASE_URL}/{APP_ID}/{endpoint}"
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

    # 1. Install attribution — keep all 81 columns, normalize names to snake_case
    df_installs = pull_report("raw_af_installs", REPORTS["raw_af_installs"])
    df_installs = normalize_columns(df_installs)
    print(f"[raw_af_installs] Writing {len(df_installs)} rows to BQ ...")
    write_to_bq(df_installs, "raw_af_installs")

    time.sleep(2)  # Avoid AppsFlyer rate limiting

    # 2. In-app events — keep all 81 columns, normalize names to snake_case
    df_events = pull_report("raw_af_events", REPORTS["raw_af_events"])
    df_events = normalize_columns(df_events)
    print(f"[raw_af_events] Writing {len(df_events)} rows to BQ ...")
    write_to_bq(df_events, "raw_af_events")

    print("\n=== All done ===")


if __name__ == "__main__":
    main()