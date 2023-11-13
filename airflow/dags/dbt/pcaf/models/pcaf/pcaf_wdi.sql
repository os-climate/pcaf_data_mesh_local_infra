{{ config(materialized='table') }}

with source_data as (

   {{ dbt_utils.unpivot(relation=ref('pcaf_wdi_source'), 
              cast_to='double', 
              exclude=['indicator_code',
                       'country_name', 
                       'country_code',
                       'indicator_name'],
              field_name="validity_date",
              value_name="value",) }}

)

select indicator_code    as src_indicator,
       'WDI'             as data_provider,
        country_name     as country_name, 
        country_code     as country_iso_code,
        indicator_name   as attribute,
        cast(replace(validity_date, 'year_', '') as int)    as validity_date,
        value,
        'USD' as value_units
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

where value is not null