{% snapshot CDC_product %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['UNIT_PRICE', 'units_in_stock']
    )
}}

select 
    "PRODUCT_ID"::VARCHAR(20) as product_id,       
    "PRODUCT_NAME"::varchar(250) as product_name,
    "UNIT_PRICE"::numeric as price,
    "CATEGORY_ID"::VARCHAR(5) as category_id,
    current_timestamp as snapshot_timestamp ,
    "UNITS_IN_STOCK" :INTEGER as units_in_stock,
    'offline_store' as data_sources
from {{ source('ecommerce_row', 'RAW_PRODUCTS') }}

{% endsnapshot %}