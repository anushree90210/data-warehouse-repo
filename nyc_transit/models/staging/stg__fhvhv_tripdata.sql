WITH source AS (
    SELECT * FROM {{ source('main', 'fhvhv_tripdata') }}
),
renamed AS (
    SELECT
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        trip_miles,
        trip_time,
        base_passenger_fare,
        tolls,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,
        CASE WHEN "shared_request_flag" = 'Y' THEN true ELSE false END AS shared_request_flag,
        CASE WHEN "shared_match_flag" = 'Y' THEN true ELSE false END AS shared_match_flag,
        CASE WHEN "access_a_ride_flag" = 'Y' THEN true ELSE false END AS access_a_ride_flag,
        CASE WHEN "wav_request_flag" = 'Y' THEN true ELSE false END AS wav_request_flag,
        CASE WHEN "wav_match_flag" = 'Y' THEN true ELSE false END AS wav_match_flag,
        filename
    FROM source
)
SELECT * FROM renamed
