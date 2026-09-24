with source as (

    select * from {{ source('raw', 'employees') }}

)

select
    cast(id as int)                as employee_id,
    trim(name)                     as employee_name,
    cast(age as int)               as age,
    trim(city)                     as city,
    cast(salary as decimal(12, 2)) as salary,
    ingested_at
from source
