{% test positive_value(model, column_name) %}
{#
    Custom test to ensure a column contains only positive values.
    Used for price columns that should never be zero or negative.

    Args:
        model: The model to test
        column_name: The column to check

    Returns:
        Records that violate the positive value constraint
#}

SELECT
    {{ column_name }} AS invalid_value
FROM {{ model }}
WHERE {{ column_name }} <= 0

{% endtest %}
