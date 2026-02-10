{% macro calculate_return(current_price, previous_price) %}
{#
    Calculate the return between two prices.

    Args:
        current_price: Current price expression
        previous_price: Previous price expression

    Returns:
        SQL expression calculating return as decimal

    Example:
        {{ calculate_return('close_price', 'LAG(close_price) OVER (...)') }}
#}
    CASE
        WHEN {{ previous_price }} IS NOT NULL
             AND {{ previous_price }} > 0
        THEN (({{ current_price }} - {{ previous_price }}) / {{ previous_price }})
        ELSE NULL
    END
{% endmacro %}
