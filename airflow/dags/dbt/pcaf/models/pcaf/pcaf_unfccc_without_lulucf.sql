{{ config(materialized='view') }}

with source_data as (

   select party
          ,year
          ,sum(numbervalue) value      
   from {{ref('pcaf_unfccc_annexi_source')}}
   where year not in ('Base year')
   and category like 'Total GHG emissions without LULUCF'
   and measure = 'Net emissions/removals'
   and gas like 'Aggregate GHGs'
   and unit = 'kt CO2 equivalent' 
   group by party
           ,year
      
   union all
    
    select party
          ,year
          ,sum(numbervalue) value  
   from {{ref('pcaf_unfccc_nonannexi_source')}}
   where year not in ('Base year')
   and category like 'Total GHG emissions excluding LULUCF/LUCF'
   and measure = 'Net emissions/removals'
   and gas like 'Aggregate GHGs'
   and unit = 'gG CO2 equivalent' 
   group by party
           ,year
)

select 'UNFCCC API'    as rec_source,
       'UNFCCC'          as data_provider,
        party            as country_iso_code,
        'Time Series - GHG total without LULUCF, in kt CO₂ equivalent' as attribute,
        cast(year as int)    as validity_date,
        value,
        'kt CO2e' as value_units
from source_data
where value is not null