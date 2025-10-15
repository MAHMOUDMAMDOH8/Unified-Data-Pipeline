{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='customer_sk',
        indexes=[{"columns": ['customer_sk'], "unique": true}],
        target_schema='GOLD'
    )
}}
with offline_customer as (
    SELECT
        CUSTOMER_SK,
        USER_ID,
        USERNAME,
        FULL_NAME,
        CITY,
        COUNTRY,
        DATA_SOURCES,
        IS_ONLINE
    FROM {{ref('STG_Customers')}}
),
online_customers as (
    SELECT
        CUSTOMER_SK,
        USER_ID,
        USERNAME,
        FULL_NAME,
        CITY,
        COUNTRY,
        DATA_SOURCES,
        IS_ONLINE
    from {{ref('STG_Users')}}
),
Final as (
    SELECT * from offline_customer
    UNION ALL
    SELECT * FROM online_customers
)

{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from Final
where customer_sk not in (select distinct customer_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from Final
{% endif %}