"""Cloud Function: Daily weather ingestion from Open-Meteo into BigQuery."""

import os
import json
import functions_framework
import requests
from datetime import date, timedelta, datetime, timezone
from google.cloud import bigquery

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "chicago_taxi_weather")
BQ_TABLE = os.environ.get("BQ_TABLE", "bronze_weather_daily")

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
CHICAGO_LAT = 41.8781
CHICAGO_LON = -87.6298
TIMEZONE = "America/Chicago"

DAILY_PARAMS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "weather_code",
    "shortwave_radiation_sum",
]

WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Drizzle: Light",
    53: "Drizzle: Moderate",
    55: "Drizzle: Dense",
    56: "Freezing drizzle: Light",
    57: "Freezing drizzle: Dense",
    61: "Rain: Slight",
    63: "Rain: Moderate",
    65: "Rain: Heavy",
    66: "Freezing rain: Light",
    67: "Freezing rain: Heavy",
    71: "Snow fall: Slight",
    73: "Snow fall: Moderate",
    75: "Snow fall: Heavy",
    77: "Snow grains",
    80: "Rain showers: Slight",
    81: "Rain showers: Moderate",
    82: "Rain showers: Violent",
    85: "Snow showers: Slight",
    86: "Snow showers: Heavy",
    95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def fetch_weather(start_date: str, end_date: str) -> list[dict]:
    """Fetch daily weather data from Open-Meteo archive API."""
    params = {
        "latitude": CHICAGO_LAT,
        "longitude": CHICAGO_LON,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(DAILY_PARAMS),
        "timezone": TIMEZONE,
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    daily = resp.json().get("daily", {})

    rows = []
    for i, day_str in enumerate(daily.get("time", [])):
        code = daily["weather_code"][i]
        rows.append(
            {
                "date": day_str,
                "temperature_2m_max_c": daily["temperature_2m_max"][i],
                "temperature_2m_min_c": daily["temperature_2m_min"][i],
                "temperature_2m_mean_c": daily["temperature_2m_mean"][i],
                "precipitation_sum_mm": daily["precipitation_sum"][i],
                "rain_sum_mm": daily["rain_sum"][i],
                "snowfall_sum_cm": daily["snowfall_sum"][i],
                "wind_speed_10m_max_kmh": daily["wind_speed_10m_max"][i],
                "wind_gusts_10m_max_kmh": daily["wind_gusts_10m_max"][i],
                "weather_code": code,
                "weather_description": WMO_CODES.get(code, "Unknown"),
                "shortwave_radiation_sum_mj": daily["shortwave_radiation_sum"][i],
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "data_source": "open-meteo",
            }
        )
    return rows


def upsert_to_bigquery(rows: list[dict]) -> int:
    """Idempotent upsert: load into a temp table, then MERGE into target."""
    if not rows:
        return 0

    client = bigquery.Client(project=GCP_PROJECT)
    target = f"`{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    staging = f"{GCP_PROJECT}.{BQ_DATASET}._staging_weather"

    staging_ref = bigquery.Table(staging)
    staging_ref.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("temperature_2m_max_c", "FLOAT64"),
            bigquery.SchemaField("temperature_2m_min_c", "FLOAT64"),
            bigquery.SchemaField("temperature_2m_mean_c", "FLOAT64"),
            bigquery.SchemaField("precipitation_sum_mm", "FLOAT64"),
            bigquery.SchemaField("rain_sum_mm", "FLOAT64"),
            bigquery.SchemaField("snowfall_sum_cm", "FLOAT64"),
            bigquery.SchemaField("wind_speed_10m_max_kmh", "FLOAT64"),
            bigquery.SchemaField("wind_gusts_10m_max_kmh", "FLOAT64"),
            bigquery.SchemaField("weather_code", "INT64"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("shortwave_radiation_sum_mj", "FLOAT64"),
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("data_source", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    load_job = client.load_table_from_json(rows, staging_ref, job_config=job_config)
    load_job.result()

    merge_sql = f"""
    MERGE {target} AS T
    USING `{staging}` AS S
    ON T.date = S.date
    WHEN MATCHED THEN UPDATE SET
        temperature_2m_max_c       = S.temperature_2m_max_c,
        temperature_2m_min_c       = S.temperature_2m_min_c,
        temperature_2m_mean_c      = S.temperature_2m_mean_c,
        precipitation_sum_mm       = S.precipitation_sum_mm,
        rain_sum_mm                = S.rain_sum_mm,
        snowfall_sum_cm            = S.snowfall_sum_cm,
        wind_speed_10m_max_kmh     = S.wind_speed_10m_max_kmh,
        wind_gusts_10m_max_kmh     = S.wind_gusts_10m_max_kmh,
        weather_code               = S.weather_code,
        weather_description        = S.weather_description,
        shortwave_radiation_sum_mj = S.shortwave_radiation_sum_mj,
        ingestion_timestamp        = S.ingestion_timestamp,
        data_source                = S.data_source
    WHEN NOT MATCHED THEN INSERT ROW
    """
    merge_job = client.query(merge_sql)
    merge_job.result()

    client.delete_table(staging, not_found_ok=True)

    return len(rows)


@functions_framework.http
def ingest_weather(request):
    """HTTP entry point. Ingests yesterday's weather data."""
    yesterday = date.today() - timedelta(days=1)
    date_str = yesterday.isoformat()

    print(f"Ingesting weather for {date_str}")
    rows = fetch_weather(date_str, date_str)
    count = upsert_to_bigquery(rows)

    msg = f"Ingested {count} row(s) for {date_str}"
    print(msg)
    return json.dumps({"status": "ok", "message": msg}), 200
