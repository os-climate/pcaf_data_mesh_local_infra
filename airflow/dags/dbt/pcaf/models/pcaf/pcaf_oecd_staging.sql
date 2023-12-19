{{ config(materialized='view') }}

with oecd as (

  select * from {{ source('pcaf', 'oecd') }}

)

select * from oecd