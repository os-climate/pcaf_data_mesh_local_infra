{{ config(materialized='table') }}

with oecd_agg as (

select t.var,
    t.indicator,
    t.ind,
    t.par,
    sum(t.value) as sum_agg
from {{ref('pcaf_oecd_staging')}} t
join {{ref('countries')}} c 
on c.country_iso3_code = t.cou
group by t.var,
    t.indicator,
    t.ind,
    t.par

)

select * from oecd_agg