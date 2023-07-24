with inter_borough_trips as (
    select
        weekday(t.started_at_ts) as weekday,
        count(*) as total_trips,
        sum(CASE WHEN l.pickup_datetime <> l.dropoff_datetime THEN 1 ELSE 0 END) as trips_different_start_end
    from
        {{ ref('mart__fact_all_taxi_trips') }} t
    INNER JOIN
        {{ ref('mart__dim_locations') }} l
    ON
        t.locationid = l.pulocationid
    INNER JOIN
        {{ ref('mart__fact_all_trips') }} f
    ON
        t.type = f.type
    group by
        WEEKDAY(t.started_at_ts)
)
SELECT
    weekday,
    total_trips,
    trips_different_start_end,
    (trips_different_start_end * 100.0 / total_trips) as percentage_different_start_end
FROM
    inter_borough_trips;
