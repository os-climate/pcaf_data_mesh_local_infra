with unfccc as (

   select party
          ,year
          ,sum(numbervalue) value      
   from {{ref('pcaf_unfccc_annexi_staging')}}
   where year not in ('Base year')
   and category like 'Total GHG emissions with LULUCF'
   and measure = 'Net emissions/removals'
   and gas like 'Aggregate GHGs'
   and unit = 'kt CO2 equivalent' 
   group by party
           ,year
      
   union all
    
    select party
          ,year
          ,sum(numbervalue) value  
   from {{ref('pcaf_unfccc_nonannexi_staging')}}
   where year not in ('Base year')
   and category like 'Total GHG emissions including LULUCF/LUCF'
   and measure = 'Net emissions/removals'
   and gas like 'Aggregate GHGs'
   and unit = 'gG CO2 equivalent' 
   group by party
           ,year
),

enhanced as (

        select 'UNFCCC API'    as scr_indicator,
               'UNFCCC'        as data_provider,
                party          as country_iso_code,
                'Time Series - GHG total with LULUCF, in kt CO₂ equivalent' as attribute,
                cast(year as int)    as validity_date,
                value,
                'kt CO2e' as value_units
        from unfccc
        where value is not null
)

select * from enhanced