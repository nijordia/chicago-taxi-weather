with source as (
    select * from {{ source('bronze', 'bronze_taxi_trips_filtered') }}
),

-- The public Chicago taxi dataset contains duplicate unique_key values.
-- Deduplicate by keeping the most recent record per unique_key.
deduplicated as (
    select
        *,
        row_number() over (
            partition by unique_key
            order by trip_start_timestamp desc
        ) as _rn
    from source
)

select
    unique_key,
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    cast(trip_seconds as int64)       as trip_seconds,
    cast(trip_miles as float64)       as trip_miles,
    pickup_community_area,
    dropoff_community_area,
    cast(fare as float64)             as fare,
    cast(tips as float64)             as tips,
    cast(tolls as float64)            as tolls,
    cast(extras as float64)           as extras,
    cast(trip_total as float64)       as trip_total,
    payment_type,
    company,
    pickup_latitude,
    pickup_longitude,
    dropoff_latitude,
    dropoff_longitude,
    date(trip_start_timestamp)        as trip_date
from deduplicated
where _rn = 1
  and trip_seconds is not null
  and trip_seconds > 0
  and trip_miles is not null
  and trip_miles >= 0
