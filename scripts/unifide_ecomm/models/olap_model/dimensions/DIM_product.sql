{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='product_sk',
        indexes=[{"columns": ['product_sk'], "unique": true}],
        target_schema='staging'
    )
}}

with valid_product as (
    select *
    from {{ ref('STG_Products') }}
),

valid_category as (
    select  
        distinct product_id, 
        category 
    from {{ ref('STG_stream_event_item') }}
),

final as (
    select
        product_sk,
        p.product_id,       
        p.product_name,
        p.price,
        c.category,
        p.units_in_stock,
        p.snapshot_timestamp,
        p.data_sources
    from valid_product p  
    left join valid_category c
        on p.product_id = c.product_id
)

{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from final
where product_sk not in (select distinct product_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from final
{% endif %}
