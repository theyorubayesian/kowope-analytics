WITH orders as (
    SELECT * FROM {{ source("facts", "fact_order") }}
),
products as (
    SELECT 
        product_id,
        sub_category,
        category,
        product_name
    FROM {{ source("dimensions", "dim_product") }}
),
final as (
    SELECT * FROM orders 
    LEFT JOIN products 
    USING (product_id)
)

SELECT * FROM final