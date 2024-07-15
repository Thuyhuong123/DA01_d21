--ad-hoc 1--
SELECT
    FORMAT_DATE('%Y-%m', created_at) AS month_year,
    COUNT(DISTINCT user_id) AS total_user,
    COUNT(order_id) AS total_order
FROM
    `bigquery-public-data.thelook_ecommerce.orders`
WHERE
    status = 'Complete'
    AND created_at BETWEEN '2019-01-01' AND '2022-05-01'
GROUP BY
    FORMAT_DATE('%Y-%m', created_at)
order by month_year
  
