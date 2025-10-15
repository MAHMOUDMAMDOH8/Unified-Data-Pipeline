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
    SELECT *
    from {{ref('CDC_product')}}
),
Final as (
    SELECT
        md5(product_id || data_sources) as product_sk , *
    from valid_product
)

{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from Final
where product_sk not in (select distinct product_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from Final
{% endif %}