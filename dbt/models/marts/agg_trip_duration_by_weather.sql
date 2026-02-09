with joined as (
    select * from {{ ref('taxi_weather_joined') }}
)

select
    trip_date,
    weather_code,
    weather_description,
    weather_category,
    temperature_2m_mean_c,
    precipitation_sum_mm,
    rain_sum_mm,
    snowfall_sum_cm,
    wind_speed_10m_max_kmh,
    wind_gusts_10m_max_kmh,
    count(*)                                as trip_count,
    round(avg(duration_minutes), 2)         as avg_duration_minutes,
    round(min(duration_minutes), 2)         as min_duration_minutes,
    round(max(duration_minutes), 2)         as max_duration_minutes,
    round(avg(trip_miles), 2)               as avg_trip_miles,
    round(avg(trip_total), 2)               as avg_trip_total
from joined
group by
    trip_date,
    weather_code,
    weather_description,
    weather_category,
    temperature_2m_mean_c,
    precipitation_sum_mm,
    rain_sum_mm,
    snowfall_sum_cm,
    wind_speed_10m_max_kmh,
    wind_gusts_10m_max_kmh
