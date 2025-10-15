{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='customer_sk',
        indexes=[{"columns": ['customer_sk'], "unique": true}],
        target_schema='staging'
    )
}}

with Valid_users as (
    select 
        "CUSTOMER_ID"::varchar(50) as user_id,
        "FAX"::varchar(100) as username,
        "CONTACT_NAME"::varchar(75) as full_name,
        "CITY"::varchar(35) as city,
        "COUNTRY"::varchar(40) as country,
        FALSE as is_online,
        'offline_store' as data_sources
    from {{ source('ecommerce_row', 'RAW_CUSTOMERS') }}
    where 
        "USER_ID" is not null
        and "FULL_NAME" is not null
        and "CITY" is not null
        and "COUNTRY" is not null
),

Final as (
    select
        md5(user_id || data_sources) as customer_sk,
        *
    from Valid_users
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
