WITH weather_data AS (
    SELECT
        name,
        date,
        prcp,
        CAST(prcp AS FLOAT) AS precipitation
    FROM {{ ref("stg__central_park_weather") }}
),

weather_data_with_avg AS (
    SELECT
        name,
        date,
        prcp,
        precipitation,
        AVG(precipitation) OVER (
            ORDER BY date
            ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
        ) AS seven_day_moving_average_prcp
    FROM weather_data
)

SELECT
    name,
    date,
    prcp,
    seven_day_moving_average_prcp
FROM
    weather_data_with_avg