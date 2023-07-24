with trips renamed as
(
    <<<<<<< HEAD
    ============================
    >>>>>>> 29814fd (mart tables and seed made)
    select 'bike' as type, started_at_ts, ended_at_ts
    from {{ ref('stg__bike_data')}}
    union all
    select 'fhv' as type, pickup_datetime, dropoff_datetime
    from {{ ref('stg__fhv_tripdata')}}
    union all
    select 'fhvhv' as type, pickup_datetime, dropoff_datetime
    from {{ ref('stg__fhvhv_tripdata')}}
    union all
    select 'green' as type, lpep_pickup_datetime, lpep_dropoff_datetime
    from {{ ref('stg__green_tripdata')}}
    union all
    select 'yellow' as type, tpep_pickup_datetime, tpep_dropoff_datetime
    from {{ ref('stg__yellow_tripdata')}}
)

select
    type,
    started_at_ts,
    ended_at_ts,
    datediff('minute', started_at_ts, ended_at_ts) as duration_min,
    datediff('second', started_at_ts, ended_at_ts) as duration_sec,
<<<<<<<<<< HEAD
from trips_renamed
==============
from trips_renamed
>>>>>>>>>> 29814fd (mart tables and seed made)
