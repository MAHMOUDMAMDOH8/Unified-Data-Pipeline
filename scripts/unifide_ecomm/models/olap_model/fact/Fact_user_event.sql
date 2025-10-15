{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='event_key',
        indexes=[
            {"columns": ['event_key'], "unique": True},
            {"columns": ['customer_sk'], "unique": False},
            {"columns": ['session_start_date_sk'], "unique": False},
            {"columns": ['session_end_date_sk'], "unique": False},
            {"columns": ['delivery_estimate_date_sk'], "unique": False}
        ],
        target_schema='GOLD'
    )
}}

with stream_data as (
    select
        md5(e.user_id || e.order_id || e.session_id) as event_key,
        e.user_id,
        e.order_id,
        e.session_id,
        e.event_type,
        e.device_type,
        e.service,
        e.session_duration,
        e.order_date as session_start,
        e.delivery_estimate,
        e.tracking_number,
        e.level,

        dateadd(second, e.session_duration, e.order_date) as session_end,
        datediff(hour, e.order_date, e.delivery_estimate) as delivery_hours,

        case 
            when e.session_duration < 300 then 'Short'
            when e.session_duration between 300 and 1200 then 'Medium'
            else 'Long'
        end as session_length_category,

        case 
            when lower(e.device_type) like '%mobile%' then 'Mobile'
            when lower(e.device_type) like '%tablet%' then 'Tablet'
            when lower(e.device_type) like '%desktop%' or lower(e.device_type) like '%laptop%' then 'Desktop'
            else 'Other'
        end as device_category,

        case 
            when lower(e.event_type) in ('click', 'view', 'browse') then 'Engagement'
            when lower(e.event_type) in ('order', 'purchase', 'checkout') then 'Transaction'
            else 'Other'
        end as event_group,

        case 
            when e.delivery_estimate is not null 
                and datediff(day, e.order_date, e.delivery_estimate) > 3 then 'Delayed'
            else 'On Time'
        end as delivery_status

    from {{ ref('STG_Stream_event') }} e
),

final as (
    select
        s.event_key,
        c.customer_sk,
        s.order_id,
        s.session_id,
        s.event_type,
        s.device_type,
        s.device_category,
        s.service,
        s.session_duration,
        s.session_length_category,
        s.event_group,
        s.delivery_status,
        s.tracking_number,
        s.level,
        s.delivery_hours,

        ds.date_sk as session_start_date_sk,

        current_timestamp() as etl_created_at,
        current_timestamp() as etl_updated_at

    from stream_data s
    left join {{ ref('DIM_Customers') }} c
        on s.user_id = c.user_id
    left join {{ ref('DIM_Date') }} ds
        on to_date(s.session_start) = to_date(ds.full_date)
)

{% if is_incremental() %}
select *
from final
where event_key not in (select distinct event_key from {{ this }})

{% else %}
select * from final
{% endif %}
