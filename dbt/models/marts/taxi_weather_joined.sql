with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
)

select
    t.unique_key,
    t.taxi_id,
    t.trip_start_timestamp,
    t.trip_end_timestamp,
    t.trip_seconds,
    t.trip_miles,
    t.trip_date,
    round(t.trip_seconds / 60.0, 2) as duration_minutes,
    t.fare,
    t.tips,
    t.tolls,
    t.extras,
    t.trip_total,
    t.payment_type,
    t.company,
    t.pickup_community_area,
    t.dropoff_community_area,
    t.pickup_latitude,
    t.pickup_longitude,
    t.dropoff_latitude,
    t.dropoff_longitude,
    w.temperature_2m_max_c,
    w.temperature_2m_min_c,
    w.temperature_2m_mean_c,
    w.precipitation_sum_mm,
    w.rain_sum_mm,
    w.snowfall_sum_cm,
    w.wind_speed_10m_max_kmh,
    w.wind_gusts_10m_max_kmh,
    w.weather_code,
    w.weather_description,
    w.weather_category,
    w.shortwave_radiation_sum_mj
from trips as t
left join weather as w
    on t.trip_date = w.date
