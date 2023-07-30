
select
    l.zone AS zone,
    COUNT(*) AS total_trips
FROM
    {{ ref('mart__fact_all_taxi_trips') }} t
JOIN
    {{ ref('mart__dim_locations') }} l ON t.pulocationid = l.locationid
GROUP BY
    l.zone
HAVING
    COUNT(*) < 100000;
