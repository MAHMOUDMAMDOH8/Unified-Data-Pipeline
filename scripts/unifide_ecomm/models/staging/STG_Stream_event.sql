{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='order_sk',
        indexes=[
            {"columns": ['order_sk'], 'unique': False},
            {"columns": ['SESSION_ID'], 'unique': False},
            {"columns": ['USER_ID'], 'unique': False}
        ],
        target_schema='staging'
    )
}}

with valid_stream_orders as (
    SELECT
        "ORDER_ID"::VARCHAR(25) AS order_id,
        "USER_ID"::VARCHAR(25) AS user_id,
        "SESSION_ID"::VARCHAR(25) AS session_id,
        "TIMESTAMP"::TIMESTAMP AS order_date,
        "CARRIER"::VARCHAR(25) AS carrier,
        "WAREHOUSE"::VARCHAR(30) AS warehouse,
        "STATUS"::VARCHAR(25) AS status,
        "SERVICE"::VARCHAR(25) AS service,
        "SHIPPING_METHOD"::VARCHAR(50) AS shipping_method,
        "SHIPPING_ADDRESS"::VARCHAR(150) AS shipping_address,
        "SESSION_DURATION"::INT AS session_duration,
        "ORDER_AMOUNT"::FLOAT AS order_amount,
        "CURRENCY"::VARCHAR(5) AS currency,
        "EVENT_TYPE"::VARCHAR(25) AS event_type,
        "DEVICE_TYPE"::VARCHAR(25) AS device_type,
        "DELIVERY_ESTIMATE"::VARCHAR(50) AS delivery_estimate,
        "TRACKING_NUMBER" as tracking_number,
        "LEVEL" as level
    FROM {{ source('ecommerce_row', 'RAW_EVENTS') }}
    WHERE 
        "ORDER_ID" IS NOT NULL
        AND "USER_ID" IS NOT NULL
        AND "TIMESTAMP" IS NOT NULL
),

final AS (
    SELECT 
        MD5(order_id || user_id || session_id || order_date) AS order_sk,
        *
    FROM valid_stream_orders
)

{% if is_incremental() %}
-- Incremental logic: insert only new rows
SELECT * 
FROM final
WHERE order_sk NOT IN (SELECT DISTINCT order_sk FROM {{ this }})

{% else %}
-- Full refresh logic: load all rows
SELECT * FROM final
{% endif %}
