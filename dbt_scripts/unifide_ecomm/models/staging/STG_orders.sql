{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='order_sk',
        indexes = 
        [
            {"columns": ['order_sk'], 'unique': False},
            {"columns": ['customer_id'], 'unique': False},
            {"columns": ['employee_id'], 'unique': False},
        ],
        target_schema='staging'
    )
}}
with valid_order as (
    SELECT
        "ORDER_ID" :: varchar(10) as order_id,
        "CUSTOMER_ID" :: varchar(50) as user_id,
        "EMPLOYEE_ID" ::varchar(20) as employee_id,
        "ORDER_DATE":: TIMESTAMP as order_date,
        "REQUIRED_DATE":: TIMESTAMP as required_date,
        "SHIPPED_DATE":: TIMESTAMP as shiped_date,
        "SHIP_CITY"::varchar(20) as ship_city,
        "SHIP_COUNTRY":: VARCHAR(25)  as ship_country,
        'offline_store' as data_sources
    from {{source('ecommerce_row','RAW_ORDERS')}}
    where 
         "ORDER_ID" is not null 
         AND "CUSTOMER_ID" is not NULL
         AND "EMPLOYEE_ID" is not NULL
         AND "ORDER_DATE" is not NULL
),
Final as (
    SELECT
        md5(order_id || user_id || employee_id || data_sources || order_date) as  order_sk , * 
    from valid_order
) 

{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from Final
where order_sk not in (select distinct order_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from Final
{% endif %}