WITH taxi_trips AS (
    SELECT
        *,
        LEAD(pickup_datetime) OVER (PARTITION BY l.zone ORDER BY pickup_datetime) AS next_pickup_time
    FROM mart__fact_all_taxi_trips t
    LEFT JOIN mart__dim_locations l ON t.pulocationid = l.locationid
)

SELECT
    zone,
    AVG(DATEDIFF('minute', pickup_datetime, next_pickup_time)) AS avg_time_between_pickups
FROM
    taxi_trips
WHERE
    next_pickup_time IS NOT NULL
GROUP BY
    zone;
