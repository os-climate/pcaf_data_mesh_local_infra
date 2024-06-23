{{ config(materialized='table') }}

with 
cte_country_scope as (
    select * from {{ref('pcaf_countries')}}
),

cte_primap as (
    select * from {{ref('pcaf_primap')}}
), 

cte_wdi_gdp as (
    select * from {{ref('pcaf_wdi')}} t
    where t.src_indicator = 'NY.GDP.MKTP.CD'
),

cte_wdi_ppp as (
    select * from {{ref('pcaf_wdi')}} t
    where t.src_indicator = 'NY.GDP.MKTP.PP.CD'
),

cte_wdi_pop as (
    select * from {{ref('pcaf_wdi')}} t
    where t.src_indicator = 'SP.POP.TOTL'
),

enhanced as (

    select  cosc.country_iso3_code
        ,prmp.validity_date
        ,coalesce(prmp.value, 0)                                as ghg_total_with_lulucf
        ,'CO2eq * kt'                                           as ghg_total_with_lulucf_units
        ,coalesce(prmp.value, 0)                                as ghg_total_without_lulucf
        ,'CO2eq * kt'                                           as ghg_total_without_lulucf_units
        ,coalesce(prmp.data_provider, 'n/a')                    as scope1_excl_source
        ,gdp.value                                              as gdp
        ,gdp.value_units                                        as gdp_units 
        ,ppp.value                                              as gdp_ppp
        ,ppp.value_units                                        as gdp_ppp_units
        ,coalesce(prmp.value, 0)/nullif(ppp.value, 0)*1000000   as ghg_intensity_with_lulucf_per_gdp
        ,'CO2eq * kt / USD'                                     as ghg_intensity_with_lulucf_per_gdp_units
        ,coalesce(prmp.value, 0)/nullif(ppp.value, 0)*1000000   as ghg_intensity_without_lulucf_per_gdp
        ,'CO2eq * kt / USD'                                     as ghg_intensity_without_lulucf_per_gdp_units
    from cte_country_scope cosc 
    left join cte_primap prmp 
    on cosc.country_iso3_code = prmp.country_iso_code 
    left join cte_wdi_gdp gdp
    on cosc.country_iso3_code = gdp.country_iso_code 
    and prmp.validity_date = gdp.validity_date 
    left join cte_wdi_ppp ppp
    on cosc.country_iso3_code = ppp.country_iso_code 
    and prmp.validity_date = ppp.validity_date 
)
select * from enhanced