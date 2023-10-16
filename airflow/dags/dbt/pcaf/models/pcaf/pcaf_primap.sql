
{{ config(materialized='table') }}

with source_data as (

   {{ dbt_utils.unpivot(relation=ref('pcaf_primap_source'), 
              cast_to='double', 
              exclude=['area_iso3','unit', 'entity', 'category_ipcc2006_primap', 'scenario_primap_hist'],
              remove =['source'],
              field_name="validity_date",
              value_name="value",) }}
)

select 'Guetschow-et-al-2022-PRIMAP-hist_v2.4_11-Oct-2022.csv'    as src_indicator,
       'PRIMAP'        as data_provider,
        area_iso3      as country_iso_code,
        entity         as attribute,
        cast(replace(validity_date, 'year_', '') as int)    as validity_date,
        value,
        unit as value_units
from source_data
where 1=1 
and entity = 'KYOTOGHG (AR4GWP100)'
and category_ipcc2006_primap ='M.0.EL'
and scenario_primap_hist='HISTCR'
and value is not null

