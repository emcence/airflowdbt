{#
  Fails for every row where the column falls outside [min_value, max_value].
  A tiny stand-in for dbt_utils.accepted_range, so the project needs no packages.
#}
{% test value_between(model, column_name, min_value, max_value) %}
select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}
{% endtest %}
