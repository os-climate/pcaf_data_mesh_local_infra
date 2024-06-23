{{ config(materialized='table') }}

with countries as (
select * from {{ref('pcaf_countries')}}
),

pcaf_oecd_staging as (
select * from {{ref('pcaf_oecd_staging')}}
),

oecd_agg as (
    select t.time,
        t.indicator,
        t.ind,
        t.par,
        sum(t.value) as sum_agg
    from pcaf_oecd_staging t
        left join countries c on c.country_iso3_code = t.cou
    where length(cou) = 3
        and (
            indicator <> 'Domestic CO2 emissions embodied in gross exports '
            or ind <> 'D35'
        )
    group by t.time,
        t.indicator,
        t.ind,
        t.par
),

cte_pivot as (
    select par,
        time,
        max(
            case
                when indicator = 'Foreign CO2 emissions embodied in gross imports'
                and ind = 'D35' then sum_agg
                else 0
            END
        ) as grimp_d35,
        max(
            case
                when indicator = 'Foreign CO2 emissions embodied in gross imports'
                and ind = 'DTOTAL' then sum_agg
                else 0
            end
        ) as grimp_dtotal,
        max(
            case
                when indicator = 'Domestic CO2 emissions embodied in gross exports '
                and ind = 'DTOTAL' then sum_agg
                else 0
            end
        ) as grexp_dtotal
    from oecd_agg t
    where 1 = 1
    group by par,
        time
),

cte_enhanced aS (
select par as country_iso3_code,
       time as validity_date,
       grimp_d35 as scope2_value,
       grimp_dtotal - grimp_d35 as scope3_value,
       grexp_dtotal as exported_emissions_value,
       grimp_dtotal - grexp_dtotal as  partial_consumption_value
from cte_pivot
)

select * from cte_enhanced