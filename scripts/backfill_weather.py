"""Backfill historical weather data from Open-Meteo into BigQuery.

Usage:
    python scripts/backfill_weather.py \
        --project <YOUR_PROJECT_ID> \
        --start-date 2023-01-01 \
        --end-date 2023-12-31
"""

import argparse
import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.main import fetch_weather, upsert_to_bigquery  # noqa: E402
import ingestion.main as ingestion_mod  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Backfill weather data")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="chicago_taxi_weather", help="BQ dataset")
    parser.add_argument("--table", default="bronze_weather_daily", help="BQ table")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--chunk-days", type=int, default=60, help="Days per API request")
    args = parser.parse_args()

    # Override module-level config with CLI args
    ingestion_mod.GCP_PROJECT = args.project
    ingestion_mod.BQ_DATASET = args.dataset
    ingestion_mod.BQ_TABLE = args.table

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    chunk = timedelta(days=args.chunk_days)

    total = 0
    current = start
    while current <= end:
        chunk_end = min(current + chunk - timedelta(days=1), end)
        print(f"Fetching {current} to {chunk_end} ...")
        rows = fetch_weather(current.isoformat(), chunk_end.isoformat())
        count = upsert_to_bigquery(rows)
        print(f"  -> Upserted {count} rows")
        total += count
        current = chunk_end + timedelta(days=1)

    print(f"\nBackfill complete: {total} total rows ingested.")


if __name__ == "__main__":
    main()
