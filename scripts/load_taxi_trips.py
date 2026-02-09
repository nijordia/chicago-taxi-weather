"""Load filtered taxi trips from BigQuery public dataset into our project.

Usage:
    python scripts/load_taxi_trips.py \
        --project <YOUR_PROJECT_ID>
"""

import argparse
from google.cloud import bigquery


def main():
    parser = argparse.ArgumentParser(description="Load filtered taxi trips from public BQ")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="chicago_taxi_weather", help="BQ dataset")
    parser.add_argument("--table", default="bronze_taxi_trips_filtered", help="BQ table")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    dest_table = f"{args.project}.{args.dataset}.{args.table}"

    query = f"""
    INSERT INTO `{dest_table}` (
        unique_key, taxi_id, trip_start_timestamp, trip_end_timestamp,
        trip_seconds, trip_miles, pickup_census_tract, dropoff_census_tract,
        pickup_community_area, dropoff_community_area, fare, tips, tolls,
        extras, trip_total, payment_type, company,
        pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude,
        pickup_location, dropoff_location
    )
    SELECT
        src.unique_key, src.taxi_id, src.trip_start_timestamp, src.trip_end_timestamp,
        src.trip_seconds, src.trip_miles,
        CAST(src.pickup_census_tract AS STRING),
        CAST(src.dropoff_census_tract AS STRING),
        src.pickup_community_area, src.dropoff_community_area,
        src.fare, src.tips, src.tolls, src.extras, src.trip_total,
        src.payment_type, src.company,
        src.pickup_latitude, src.pickup_longitude,
        src.dropoff_latitude, src.dropoff_longitude,
        CAST(src.pickup_location AS STRING),
        CAST(src.dropoff_location AS STRING)
    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` src
    LEFT JOIN `{dest_table}` dst
        ON src.unique_key = dst.unique_key
    WHERE src.trip_start_timestamp >= '2023-06-01'
      AND src.trip_start_timestamp < '2024-01-01'
      AND dst.unique_key IS NULL
    """

    print(f"Loading filtered taxi trips into {dest_table} ...")
    print("This may take a few minutes (free-tier query on ~6 months of data).")

    job = client.query(query)
    job.result()
    print(f"Done. Rows affected: {job.num_dml_affected_rows}")


if __name__ == "__main__":
    main()
