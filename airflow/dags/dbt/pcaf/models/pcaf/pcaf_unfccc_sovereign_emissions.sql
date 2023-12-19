{{ config(materialized='table') }}

with cte_base as (
select validity_date, country_iso_code from {{ref('pcaf_unfccc_with_lulucf')}}
union 
select validity_date, country_iso_code from {{ref('pcaf_unfccc_without_lulucf')}}

), 

enhanced as (

    select  cteb.country_iso_code
        ,cteb.validity_date
        ,a.value                                                      as ghg_total_with_lulucf
        ,a.value_units                                                as ghg_total_with_lulucf_units
        ,coalesce(b.value, c.value, 0)                                as ghg_total_without_lulucf
        ,b.value_units                                                as ghg_total_without_lulucf_units
        ,coalesce(b.data_provider, c.data_provider, 'n/a')            as scope1_excl_source
        ,gdp.value                                                    as gdp
        ,gdp.value_units gdp_units 
        ,ppp.value                                                    as gdp_ppp
        ,ppp.value_units                                              as gdp_ppp_units
        ,coalesce(a.value, c.value, 0)/nullif(ppp.value, 0)*1000000   as ghg_intensity_with_lulucf_per_gdp
        ,'CO2eq * kt / USD'                                           as ghg_intensity_with_lulucf_per_gdp_units
        ,coalesce(b.value, c.value, 0)/nullif(ppp.value, 0)*1000000   as ghg_intensity_without_lulucf_per_gdp
        ,'CO2eq * kt / USD'                                           as ghg_intensity_without_lulucf_per_gdp_units
    from cte_base cteb 
    left join {{ref('pcaf_unfccc_with_lulucf')}} a 
    on cteb.country_iso_code = a.country_iso_code 
        and cteb.validity_date = a.validity_date
    left join {{ref('pcaf_unfccc_without_lulucf')}} b 
    on cteb.country_iso_code = b.country_iso_code 
        and cteb.validity_date = b.validity_date
    left join {{ref('pcaf_primap')}} c 
    on cteb.country_iso_code = c.country_iso_code 
        and cteb.validity_date = c.validity_date        
    left join {{ref('pcaf_wdi')}} gdp
    on cteb.country_iso_code = gdp.country_iso_code 
    and cteb.validity_date = gdp.validity_date 
    and gdp.attribute = 'GDP (current US$)'
    left join {{ref('pcaf_wdi')}} ppp
    on cteb.country_iso_code = ppp.country_iso_code 
    and cteb.validity_date = ppp.validity_date 
    and gdp.attribute = 'GDP, PPP (current international $)'
)
select * from enhanced