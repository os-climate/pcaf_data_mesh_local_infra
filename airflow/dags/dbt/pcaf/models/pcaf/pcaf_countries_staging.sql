{{ config(materialized='view') }}

with pcaf_countries as (
    select * from {{ source('pcaf', 'pycountry') }}
                                   
)

select * from pcaf_countries;