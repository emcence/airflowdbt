with employees as (

    select * from {{ ref('stg_employees') }}

)

select
    employee_id,
    employee_name,
    city,
    salary,
    case
        when salary < 70000 then 'low'
        when salary < 85000 then 'mid'
        else 'high'
    end as salary_band
from employees
