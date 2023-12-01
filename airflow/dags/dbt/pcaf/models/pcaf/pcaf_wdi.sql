with unpivot_wdi as (

   {{ dbt_utils.unpivot(relation=ref('pcaf_wdi_staging'), 
              cast_to='double', 
              exclude=['country_name','country_iso_code', 'attribute','attribute_code'],
              field_name="validity_date",
              value_name="value",) }}

),

enhanced as
( 
    select attribute_code as src_indicator,
            'WDI'             as data_provider,
            country_name, 
            country_iso_code,
            attribute,
            cast(replace(validity_date, 'year_', '') as int) as validity_date,
            value,
            'USD' as value_units
    from unpivot_wdi
    where value is not null
)

select * from enhanced