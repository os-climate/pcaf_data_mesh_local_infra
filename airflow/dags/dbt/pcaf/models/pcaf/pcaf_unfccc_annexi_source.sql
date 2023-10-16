{{ config(materialized='view') }}


SELECT party
      ,category
      ,classification
      ,measure
      ,gas
      ,unit
      ,year
      ,numbervalue
      ,stringvalue
from hive.pcaf.annexi
