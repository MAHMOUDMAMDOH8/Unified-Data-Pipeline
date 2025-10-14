{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='Employee_sk',
        indexes=[{"columns": ['Employee_sk'], "unique": true}],
        target_schema='GOLD'
    )
}}
with valid_emp as(
    SELECT * from {{ref('STG_Employees')}}
)

{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from valid_emp
where Employee_sk not in (select distinct Employee_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from valid_emp
{% endif %}