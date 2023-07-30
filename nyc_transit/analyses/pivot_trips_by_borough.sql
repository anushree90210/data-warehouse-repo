SELECT
    SUM(CASE WHEN borough = 'Bronx' THEN trip_count ELSE 0 END) AS trips_in_bronx,
    SUM(CASE WHEN borough = 'Brooklyn' THEN trip_count ELSE 0 END) AS trips_in_brooklyn,
    SUM(CASE WHEN borough = 'Manhattan' THEN trip_count ELSE 0 END) AS trips_in_manhattan,
    SUM(CASE WHEN borough = 'Queens' THEN trip_count ELSE 0 END) AS trips_in_queens,
    SUM(CASE WHEN borough = 'Staten Island' THEN trip_count ELSE 0 END) AS trips_in_staten_island,
    SUM(trip_count) AS total_trips
FROM {{ ref("mart__fact_trips_by_borough") }}
