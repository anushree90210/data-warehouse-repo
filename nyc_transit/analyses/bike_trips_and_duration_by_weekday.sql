select
    weekday(started_at_ts) as weekday,
    count(*) as total_trips,
    sum(duration_sec) as total_trips_duration_secs
from {{ ref('mart__fact_all_bike_trips')}}
group by all