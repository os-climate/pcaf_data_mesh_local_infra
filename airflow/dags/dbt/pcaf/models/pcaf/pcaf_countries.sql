{{ config(materialized='view') }}

with pcaf_countries as (
    select * from {{ref('pcaf_countries_staging')}}
                                   
)

select * from pcaf_countries;