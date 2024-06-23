{{ config(materialized = 'table') }} 

with 
primap as (
    select prmp.*
    from {{ ref('pcaf_primap_sovereign_emissions') }} prmp
),
oecd as (
    select *
    from {{ ref('pcaf_oecd_sovereign_emissions') }}
),
wdi_pop as (
    SELECT t.country_iso_code,
        t.validity_date,
        t.value as population_value
    from {{ ref('pcaf_wdi') }} t
    where t.src_indicator = 'SP.POP.TOTL'
),
countries as (
    select *
    from {{ ref('pcaf_countries') }}
),
years_primap as (
    select distinct validity_date as validity_date
    from primap p 
),
max_oecd_year as (
    select max(validity_date) as max_oecd_date from oecd
),
cte_scope as (
    SELECT distinct y.validity_date,
        case when y.validity_date > o.max_oecd_date 
               then o.max_oecd_date 
             else y.validity_date end  as oecd_validity_date,
        c.country_iso3_code
    from countries c
        cross join years_primap y
        cross join max_oecd_year o
    where y.validity_date >  1900   
),
cte_base as (
    select s.validity_date,
        s.country_iso3_code,
        prmp.ghg_total_with_lulucf as scope1_incl_lulucf,
        prmp.ghg_total_with_lulucf_units,
        prmp.ghg_total_without_lulucf as scope1_excl_lulucf,
        prmp.ghg_total_without_lulucf_units,
        prmp.scope1_excl_source,
        prmp.gdp,
        prmp.gdp_units,
        prmp.gdp_ppp,
        prmp.gdp_ppp_units,
        prmp.ghg_intensity_with_lulucf_per_gdp,
        prmp.ghg_intensity_with_lulucf_per_gdp_units,
        prmp.ghg_intensity_without_lulucf_per_gdp,
        prmp.ghg_intensity_without_lulucf_per_gdp_units,
        o.scope2_value,
        o.validity_date as scope2_date,
        o.scope3_value,
        o.validity_date as scope3_date,
        o.exported_emissions_value,
        o.partial_consumption_value,
        w.population_value,
        case
            when w.population_value > 0 then 1.0 / w.population_value
            else 0
        end as partial_population,
        case
            when prmp.gdp_ppp > 0 then 1.0 / prmp.gdp_ppp
            else 0
        end as calc_attr_factor_yn,
        case
            when partial_consumption_value > 0
            and prmp.gdp_ppp > 0 then 1.0 / prmp.gdp_ppp
            else 0
        end as attr_partial_gdp_ppp,
        case
            when partial_consumption_value > 0
            and prmp.gdp_ppp > 0
            and prmp.gdp > 0 then 1.0 / prmp.gdp
            else 0
        end attr_partial_gdp
    from cte_scope s
        left join primap prmp on s.country_iso3_code = prmp.country_iso3_code
        and s.validity_date = prmp.validity_date
        left join oecd o on s.country_iso3_code = o.country_iso3_code
        and s.oecd_validity_date = o.validity_date
        left join wdi_pop w on s.country_iso3_code = w.country_iso_code
        and s.validity_date = w.validity_date
),
cte_enhanced as (
    select b.*,
        scope1_excl_lulucf + partial_consumption_value as consumption_emissions_excl_lulucf,
        scope1_incl_lulucf + partial_consumption_value as consumption_emissions_incl_lulucf,
        partial_population * scope1_excl_lulucf + partial_consumption_value as consumption_emissions_excl_lulucf_per_capita,
        partial_population * scope1_incl_lulucf + partial_consumption_value as consumption_emissions_incl_lulucf_per_capita,
        calc_attr_factor_yn * scope1_excl_lulucf / gdp_ppp as attribution_factor_scope1_excl_lulucf,
        calc_attr_factor_yn * scope1_incl_lulucf / gdp_ppp as attribution_factor_scope1_incl_lulucf,
        attr_partial_gdp_ppp * scope1_excl_lulucf + scope2_value + scope3_value as attribution_factor_excl_lulucf,
        attr_partial_gdp_ppp * scope1_incl_lulucf + scope2_value + scope3_value as attribution_factor_incl_lulucf,
        attr_partial_gdp * scope1_excl_lulucf + scope2_value + scope3_value as attribution_factor_excl_lulucf_gdp,
        attr_partial_gdp * scope1_incl_lulucf + scope2_value + scope3_value as attribution_factor_incl_lulucf_gdp
    from cte_base b
)
select *
from cte_enhanced