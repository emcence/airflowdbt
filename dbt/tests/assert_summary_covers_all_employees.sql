-- Singular test: the city summary must account for every staged employee.
select
    staged.total as staged_employees,
    summary.total as summarised_employees
from (select count(*) as total from {{ ref('stg_employees') }}) as staged
cross join (select sum(employee_count) as total from {{ ref('city_salary_summary') }}) as summary
where staged.total <> summary.total
