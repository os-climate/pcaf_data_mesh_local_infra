with worldbank as (

    select * from {{ source('pcaf', 'worldbank')}}
)

SELECT
 "country code"   as country_iso_code
,"country name"   as country_name
,"indicator name" as attribute
,"indicator code" as attribute_code
,"1960" as year_1960 
,"1961" as year_1961
,"1962" as year_1962
,"1963" as year_1963
,"1964" as year_1964
,"1965" as year_1965
,"1966" as year_1966
,"1967" as year_1967
,"1968" as year_1968
,"1969" as year_1969
,"1970" as year_1970
,"1971" as year_1971
,"1972" as year_1972
,"1973" as year_1973
,"1974" as year_1974
,"1975" as year_1975
,"1976" as year_1976
,"1977" as year_1977
,"1978" as year_1978
,"1979" as year_1979
,"1980" as year_1980
,"1981" as year_1981
,"1982" as year_1982
,"1983" as year_1983
,"1984" as year_1984
,"1985" as year_1985
,"1986" as year_1986
,"1987" as year_1987
,"1988" as year_1988
,"1989" as year_1989
,"1990" as year_1990
,"1991" as year_1991
,"1992" as year_1992
,"1993" as year_1993
,"1994" as year_1994
,"1995" as year_1995
,"1996" as year_1996
,"1997" as year_1997
,"1998" as year_1998
,"1999" as year_1999
,"2000" as year_2000
,"2001" as year_2001
,"2002" as year_2002
,"2003" as year_2003
,"2004" as year_2004
,"2005" as year_2005
,"2006" as year_2006
,"2007" as year_2007
,"2008" as year_2008
,"2009" as year_2009
,"2010" as year_2010
,"2011" as year_2011
,"2012" as year_2012
,"2013" as year_2013
,"2014" as year_2014
,"2015" as year_2015
,"2016" as year_2016
,"2017" as year_2017
,"2018" as year_2018
,"2019" as year_2019
,"2020" as year_2020
,"2021" as year_2021
,"2022" as year_2022
FROM worldbank
