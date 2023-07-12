WITH source AS (
    SELECT * FROM {{ source('main', 'green_tripdata') }}
),
renamed AS (
    SELECT
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        CASE WHEN "store_and_fwd_flag" = 'Y' THEN true ELSE false END AS store_and_fwd_flag,
        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        filename
    FROM source
)
SELECT * FROM renamed
