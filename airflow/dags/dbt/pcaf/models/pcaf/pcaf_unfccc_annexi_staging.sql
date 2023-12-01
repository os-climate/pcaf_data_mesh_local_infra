with annexi as (

select * from {{ source('pcaf', 'annexi') }}

)

SELECT * from annexi
