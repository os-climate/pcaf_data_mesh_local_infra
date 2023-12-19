def model(dbt, fal):

    import pycountry
    import pandas as pd
    
    dbt.config(on_table_exists='drop')

    df_country = pd.DataFrame([[country.alpha_3, country.alpha_2, country.name]  for country in pycountry.countries ], columns=['country_iso3_code', 'country_iso2_code', 'country_name'], dtype='string' )

    return df_country