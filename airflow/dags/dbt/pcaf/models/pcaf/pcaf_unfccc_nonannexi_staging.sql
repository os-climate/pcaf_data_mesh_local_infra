with non_annexi as (

      select * from {{ source('pcaf', 'non_annexi')}}

)

SELECT * from non_annexi
