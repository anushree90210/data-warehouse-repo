SELECT
    l.borough AS borough,
    l.zone AS zone,
    COUNT(*) AS trip_count,
    AVG(CAST(TRY_CAST(t.duration_sec AS NUMERIC) AS NUMERIC)) AS average_duration_sec
FROM
    {{ ref('mart__fact_all_bike_trips') }} t
JOIN
    {{ ref('mart__dim_locations') }} l ON TRY_CAST(t.start_station_id AS INT64) = l.locationid
GROUP BY
    l.borough,
    l.zone;
