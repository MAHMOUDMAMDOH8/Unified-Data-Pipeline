{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='date_sk',
        indexes=[
            {"columns": ['date_sk'], "unique": true},
            {"columns": ['time_sk'], "unique": false}
        ],
        target_schema='GOLD'
    )
}}

with stream_date as (
    select distinct "ORDER_DATE"::timestamp as full_date 
    from {{ ref('STG_Stream_event') }}
    where "ORDER_DATE" is not null
),
db_date as (
    select distinct "ORDER_DATE"::timestamp as full_date 
    from {{ ref('STG_orders') }}
    where "ORDER_DATE" is not null
),
all_dates as (
    select full_date from stream_date
    union
    select full_date from db_date
),
dim_date as (
    select
        md5(cast(full_date as varchar)) as date_sk,
        full_date,
        extract(hour from full_date) as hour,
        extract(minute from full_date) as minute,
        extract(second from full_date) as second,
        md5(
            cast(
                (extract(hour from full_date) * 3600) +
                (extract(minute from full_date) * 60) +
                extract(second from full_date)
                as varchar
            )
        ) as time_sk,
        extract(year from full_date) as year,
        extract(month from full_date) as month,
        extract(day from full_date) as day,
        extract(dayofmonth from full_date) as day_of_month,
        to_varchar(full_date, 'Month') as month_name,
        to_varchar(full_date, 'Day') as day_name,
        case when dayofweek(full_date) in (6,7) then true else false end as is_weekend,
        extract(year from dateadd(month, 3, full_date)) as fiscal_year,
        extract(month from dateadd(month, 3, full_date)) as fiscal_month,

        current_timestamp() as dbt_created_at,
        current_timestamp() as dbt_updated_at
    from all_dates
)

{% if is_incremental() %}
-- Incremental load: only insert new timestamps
select *
from dim_date
where date_sk not in (select distinct date_sk from {{ this }})

{% else %}
-- Full refresh load
select * from dim_date
{% endif %}
