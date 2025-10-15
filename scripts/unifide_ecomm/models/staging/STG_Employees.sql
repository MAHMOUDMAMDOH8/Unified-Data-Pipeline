{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='Employee_sk',
        indexes=[{"columns": ['Employee_sk'], "unique": true}],
        target_schema='staging'
    )
}}
with valid_employee as (
    select
        "EMPLOYEE_ID" :: VARCHAR(20) as employee_id,
        concat("FIRST_NAME",' ', "LAST_NAME") ::VARCHAR(70) as name,
        "TITLE" ::VARCHAR(35) as title,
        "HIRE_DATE" :: DATE as hire_date,
        "CITY" :: VARCHAR(35) as city,
        "COUNTRY":: VARCHAR(50) as country,
        'offline_store' as data_sources
    from {{source('ecommerce_row','RAW_EMPLOYEES')}}
    where 
        "EMPLOYEE_ID" is not null
),
final as (
    select 
        md5(employee_id || data_sources) as Employee_sk , * 
    from valid_employee
)

{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from Final
where Employee_sk not in (select distinct Employee_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from Final
{% endif %}
