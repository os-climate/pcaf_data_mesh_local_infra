{{ config(materialized='table') }}

with unpivot_primap as (

   {{ dbt_utils.unpivot(relation=ref('pcaf_primap_staging'), 
              cast_to='double', 
              exclude=['country_iso_code','value_units', 'attribute', 'category', 'scenario'],
              remove =['source'],
              field_name="validity_date",
              value_name="value") }}
),

enhanced as     
(
        select 
                'Guetschow-et-al-2022-PRIMAP-hist_v2.4_11-Oct-2022.csv'    as src_indicator,
                'PRIMAP'        as data_provider,
                country_iso_code,
                attribute,
                cast(replace(validity_date, 'year_', '') as int)    as validity_date,
                value,
                value_units
        from unpivot_primap
        where 1=1 
        and attribute = 'KYOTOGHG (AR4GWP100)'
        and category ='M.0.EL'
        and scenario='HISTCR'
        and value is not null

)

select * from enhanced