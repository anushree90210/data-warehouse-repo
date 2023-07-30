WITH weather_data AS (
    SELECT
        name,
        date,
        CAST(prcp AS FLOAT) AS precipitation,
        CAST(snow AS FLOAT) AS snow
    FROM {{ ref("stg__central_park_weather") }}
),

seven_day_window AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY date) AS row_num
    FROM weather_data
)

SELECT
    name,
    date,
    precipitation,
    snow,
    MIN(precipitation) OVER seven_days AS seven_day_moving_min_prcp,
    MAX(precipitation) OVER seven_days AS seven_day_moving_max_prcp,
    AVG(precipitation) OVER seven_days AS seven_day_moving_avg_prcp,
    SUM(precipitation) OVER seven_days AS seven_day_moving_sum_prcp,
    MIN(snow) OVER seven_days AS seven_day_moving_min_snow,
    MAX(snow) OVER seven_days AS seven_day_moving_max_snow,
    AVG(snow) OVER seven_days AS seven_day_moving_avg_snow,
    SUM(snow) OVER seven_days AS seven_day_moving_sum_snow
FROM
    seven_day_window
WINDOW seven_days AS (
    PARTITION BY name
    ORDER BY row_num
    RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
);
