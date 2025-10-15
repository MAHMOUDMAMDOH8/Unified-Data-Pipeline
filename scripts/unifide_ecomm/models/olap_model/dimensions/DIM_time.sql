{{
    config(
        materialized='table',
        unique_key='time_sk',
        indexes=[{"columns": ['time_sk'], "unique": true}],
        target_schema='GOLD'
    )
}}

with time_values as (
    select 
        seq4() as second_of_day
    from table(generator(rowcount => 24*60*60))

),

dim_time as (
    select
        md5(cast(second_of_day as varchar)) as time_sk,
        dateadd(second, second_of_day, to_time('00:00:00')) as full_time,

        extract(hour from dateadd(second, second_of_day, to_time('00:00:00'))) as hour,
        extract(minute from dateadd(second, second_of_day, to_time('00:00:00'))) as minute,
        extract(second from dateadd(second, second_of_day, to_time('00:00:00'))) as second,

        case 
            when extract(hour from dateadd(second, second_of_day, to_time('00:00:00'))) < 12 then 'AM'
            else 'PM'
        end as am_pm,

        case 
            when extract(hour from dateadd(second, second_of_day, to_time('00:00:00'))) between 0 and 5 then 'Early Morning'
            when extract(hour from dateadd(second, second_of_day, to_time('00:00:00'))) between 6 and 11 then 'Morning'
            when extract(hour from dateadd(second, second_of_day, to_time('00:00:00'))) between 12 and 17 then 'Afternoon'
            else 'Evening'
        end as time_period,

        current_timestamp() as etl_created_at,
        current_timestamp() as etl_updated_at
    from time_values
)

select * from dim_time
