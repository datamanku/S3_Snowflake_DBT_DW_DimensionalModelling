
{% test no_unwanted_spaces(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} != TRIM({{ column_name }})
{% endtest %}

{% test birthdate_in_range(model, column_name, min_date, max_date) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < TO_DATE('{{ min_date }}')
   OR {{ column_name }} > TO_DATE('{{ max_date }}')
{% endtest %}