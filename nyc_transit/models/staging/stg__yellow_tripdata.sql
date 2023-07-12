WITH source AS (
    SELECT * FROM {{ source('main', 'yellow_tripdata') }}
),
renamed AS (
    SELECT
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        CASE WHEN "store_and_fwd_flag" = 'Y' THEN true ELSE false END AS store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        filename
    FROM source
)
SELECT * FROM renamed
