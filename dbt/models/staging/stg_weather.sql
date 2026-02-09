with source as (
    select * from {{ source('bronze', 'bronze_weather_daily') }}
),

codes as (
    select * from {{ ref('weather_code_mapping') }}
)

select
    s.date,
    s.temperature_2m_max_c,
    s.temperature_2m_min_c,
    s.temperature_2m_mean_c,
    s.precipitation_sum_mm,
    s.rain_sum_mm,
    s.snowfall_sum_cm,
    s.wind_speed_10m_max_kmh,
    s.wind_gusts_10m_max_kmh,
    s.weather_code,
    coalesce(c.description, s.weather_description, 'Unknown') as weather_description,
    coalesce(c.category, 'Other')                             as weather_category,
    s.shortwave_radiation_sum_mj
from source as s
left join codes as c
    on s.weather_code = c.weather_code
