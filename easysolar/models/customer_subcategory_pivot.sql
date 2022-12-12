SELECT
    customer_id,
    {{
        dbt_utils.pivot(
            "sub_category",
            dbt_utils.get_column_values(ref('orders_with_product_info'), 'sub_category'),
            then_value='sales'
        )
    }}
FROM {{ ref('orders_with_product_info') }}
GROUP BY customer_id
