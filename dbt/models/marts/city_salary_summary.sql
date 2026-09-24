-- Same aggregation as dags/spark_job.py, expressed in SQL and enriched with the region seed.
with employees as (

    select * from {{ ref('stg_employees') }}

),

regions as (

    select * from {{ ref('city_region') }}

)

select
    employees.city,
    regions.state,
    regions.region,
    count(employees.employee_id)   as employee_count,
    round(avg(employees.salary), 2) as avg_salary,
    max(employees.salary)          as max_salary,
    min(employees.age)             as min_age
from employees
left join regions
    on employees.city = regions.city
group by
    employees.city,
    regions.state,
    regions.region
