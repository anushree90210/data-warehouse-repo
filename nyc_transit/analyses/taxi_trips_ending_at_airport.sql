select
    count(*) as total_trips_ending_at_airport
from
    {{ ref('mart__dim_locations') }}
WHERE
    service_zone IN ('Airports', 'EWR');
