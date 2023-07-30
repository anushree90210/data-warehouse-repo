WITH trips_by_borough AS (
    SELECT
        l.borough AS borough,
        t.type AS trip_type,
        COUNT(*) AS trip_count
    FROM
        {{ ref("mart__fact_all_taxi_trips") }} t
    JOIN
        {{ ref("mart__dim_locations") }} l ON t.pulocationid = l.locationid
    GROUP BY
        l.borough, t.type
)

SELECT
    *
FROM trips_by_borough
