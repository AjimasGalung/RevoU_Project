-- All the user that orders a product --
WITH user_order AS
(
  SELECT
  EXTRACT(DATE FROM o.created_at) AS timestamp,
  u.id AS user_id,
  CONCAT(u.first_name," ",u.last_name) AS customer_name,
  u.age,
  o.gender,
  u.city,
  u.country,
  SUM(o.num_of_item) AS total_item_order,
  
  -- Sum all the status that has been shipped to the customer --
  SUM(CASE
        WHEN o.status = "Shipped" THEN o.num_of_item
        ELSE 0
        END) AS total_item_shipped

  FROM 
    bigquery-public-data.thelook_ecommerce.users AS u
      INNER JOIN
    bigquery-public-data.thelook_ecommerce.orders AS o
      ON u.id = o.user_id
  GROUP BY 1,2,3,4,5,6,7
  ORDER BY 1
),

-- All the product that is ordered by the user --
product_order AS
(
  SELECT
    oi.user_id,
    oi.order_id,
    oi.product_id,
    p.category AS product_category,
    p.name AS product_name,
    p.brand AS product_brand,
    SUM(oi.sale_price) AS sale_price,
    SUM(p.retail_price) AS retail_price

  FROM
    bigquery-public-data.thelook_ecommerce.order_items AS oi
      INNER JOIN
    bigquery-public-data.thelook_ecommerce.products AS p
      ON oi.product_id = p.id
  GROUP BY 1,2,3,4,5,6
  ORDER BY 3
)

-- Combine table of user_order and product_order --
SELECT 
  uo.timestamp,
  uo.user_id,
  po.order_id,
  po.product_id,
  uo.customer_name,
  uo.age,
  uo.gender,
  uo.city,
  uo.country,
  po.product_name,
  po.product_category,
  po.product_brand,
  uo.total_item_order,
  uo.total_item_shipped,
  ROUND(CAST(total_item_shipped AS FLOAT64)*100 / total_item_order,2)
    AS percentage_of_product_shipped,
  ROUND(po.sale_price,2) AS sale_price,
  DENSE_RANK() OVER(PARTITION BY uo.country ORDER BY po.sale_price DESC) AS rank

FROM 
  user_order AS uo
    INNER JOIN
  product_order AS po
    ON uo.user_id = po.user_id