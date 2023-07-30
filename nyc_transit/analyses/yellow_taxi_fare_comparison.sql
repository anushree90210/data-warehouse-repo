WITH fare_comparison AS (
    SELECT
        fare_amount,
        pulocationid,
        l.borough AS pickup_borough,
        AVG(fare_amount) OVER () AS overall_average_fare,
        AVG(fare_amount) OVER (PARTITION BY pulocationid) AS zone_average_fare,
        AVG(fare_amount) OVER (PARTITION BY l.borough) AS borough_average_fare
    FROM {{ source("main", "yellow_tripdata") }} t
    JOIN {{ ref("mart__dim_locations") }} l ON t.pulocationid = l.locationid
)
SELECT
    fare_amount,
    pickup_borough,
    overall_average_fare,
    zone_average_fare,
    borough_average_fare
FROM fare_comparison;
