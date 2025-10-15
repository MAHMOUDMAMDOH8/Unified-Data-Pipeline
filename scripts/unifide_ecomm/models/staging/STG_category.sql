{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='category_sk',
        indexes=[{"columns": ['category_sk'], "unique": true}],
        target_schema='staging'
    )
}}
with valid_category as (
    select
        "CATEGORY_ID"::VARCHAR(20) as category_id,
        "CATEGORY_NAME"::VARCHAR(50) as category_name,
        'offline_store' as data_sources
    from {{source('ecommerce_row','RAW_CATEGORIES')}}
    where CATEGORY_ID is not null
),
final as (
    SELECT
        md5(category_id || data_sources) as category_sk , *
    from valid_category
)
{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from Final
where category_sk not in (select distinct category_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from Final
{% endif %}