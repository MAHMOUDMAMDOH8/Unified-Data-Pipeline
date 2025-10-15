{{  
    config(
        materialized='incremental',
        strategy='append',
        unique_key='order_key',
        indexes=[
            {"columns": ['order_sk'], "unique": False},
            {"columns": ['customer_sk'], "unique": False},
            {"columns": ['employee_sk'], "unique": False},
            {"columns": ['product_sk'], "unique": False},
            {"columns": ['date_sk'], "unique": False}
        ],
        target_schema='GOLD'
    )
}}

with stream_data as (
    select
        i.order_sk,
        e.order_date,
        e.user_id,
        i.product_id,
        null as employee_id,
        e.status,
        'online' as payment_method,
        e.carrier,
        'online_store' as channel,
        e.shipping_method,
        e.warehouse,
        i.price,
        i.quantity,
        e.order_amount,
        e.currency
    from {{ ref('STG_Stream_event') }} e
    join {{ ref('STG_stream_event_item') }} i
        on e.order_id = i."ORDER_ID"
),

db_data as (
    select
        od.order_sk,
        o.order_date,
        o.user_id,
        od.product_id,
        o.employee_id,
        'completed' as status,
        'cash' as payment_method,
        null as carrier,
        'offline_store' as channel,
        'hand truck' as shipping_method,
        'WH-USA' as warehouse,
        od.price,
        od.quantity,
        od.price * od.quantity as order_amount,
        'USD' as currency
    from {{ ref('STG_orders') }} o
    join {{ ref('STG_orders_ditals') }} od
        on o.order_id = od."ORDER_ID"
),

union_data as (
    select * from stream_data
    union all
    select * from db_data
),

final as (
    select
        md5(
            coalesce(f.order_sk, '') || '-' ||
            coalesce(c.customer_sk, '') || '-' ||
            coalesce(e.employee_sk, '-1') || '-' ||
            coalesce(p.product_sk, '') || '-' ||
            coalesce(d.date_sk, '')
        ) as order_key,

        f.order_sk,
        c.customer_sk,
        coalesce(e.employee_sk, -1) as employee_sk,
        p.product_sk,
        d.date_sk,

        f.status,
        f.payment_method,
        f.carrier,
        f.channel,
        f.shipping_method,
        f.warehouse,
        f.price,
        f.quantity,
        f.order_amount,
        f.currency,

        current_timestamp() as etl_created_at,
        current_timestamp() as etl_updated_at

    from union_data f
    left join {{ ref('DIM_Customers') }} c
        on f.user_id = c.user_id
    left join {{ ref('DIM_Employee') }} e
        on f.employee_id = e.employee_id
    left join {{ ref('DIM_product') }} p
        on f.product_id = p.product_id
    left join {{ ref('DIM_Date') }} d
        on f.order_date = d.full_date
)


{% if is_incremental() %}
    select *
    from final
    where order_key not in (select distinct order_key from {{ this }})
{% else %}
    select * from final
{% endif %}
