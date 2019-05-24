{% macro is_gt_zero(col) %}

case
  when {{ col }} > 0 then 1 else 0
end

{% endmacro %}
