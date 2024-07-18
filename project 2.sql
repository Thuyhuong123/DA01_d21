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
--ad-hoc 2--
select format_date('%Y-%m', created_at) as month_year,
count (distinct user_id) as distinct_users, 
avg(sale_price) as average_order_value
from bigquery-public-data.thelook_ecommerce.order_items
where created_at between '2019-01-01' and '2022-05-01'
group by format_date('%Y-%m', created_at)
order by month_year
--ad-hoc 3--
with twt_age as (
    select gender, min(age) as min_age, max (age) as max_age
    from bigquery-public-data.thelook_ecommerce.users 
    where  created_at between '2019-01-01' and '2022-05-01'
    group by gender
),
youngest as (select a.first_name, a.last_name, a.gender, t.min_age as age,'youngest' as tag
from bigquery-public-data.thelook_ecommerce.users as a
join twt_age as t on a.gender=t.gender),
oldest as (
select a.first_name, a.last_name, a.gender, t.max_age as age,'oldest' as tag
from bigquery-public-data.thelook_ecommerce.users as a
join twt_age as t on a.gender=t.gender
order by a.gender)
select * from youngest
union all
select * from oldest
order by gender
--ad-hoc 4--
with cte_rank as(select format_date('%Y-%m',b.created_at) as month_year,a.id as product_id, a.name as product_name, a.cost,b.sale_price as sales,
b.sale_price-a.cost as profit,
dense_rank () over (partition by format_date('%Y-%m',b.created_at) order by b.sale_price-a.cost) as rank_per_month
from bigquery-public-data.thelook_ecommerce.products as a
join bigquery-public-data.thelook_ecommerce.order_items as b
on a.id=b.product_id)
select * from cte_rank 
where rank_per_month<=5
--ad-hoc 5--
select a.category, format_date('%Y-%m-%d', b.created_at) as dates,
sum(b.sale_price ) as revenue
from bigquery-public-data.thelook_ecommerce.products as a
join bigquery-public-data.thelook_ecommerce.order_items as b
on a.id=b.product_id
where format_date('%Y-%m-%d', b.created_at) between '2022-01-15' and '2022-04-15'
group by category, dates
order by category, dates
--vw_ecommerce_analyst--
with cte as (
  select FORMAT_DATE('%Y-%m', a.created_at) as month,
  FORMAT_DATE('%Y', a.created_at) as year,
  b.category as product_category,
  sum(b.cost) as total_cost,
  sum(c.sale_price) as TPV, count(c.order_id) as TPO
  FROM bigquery-public-data.thelook_ecommerce.orders AS a
  JOIN bigquery-public-data.thelook_ecommerce.order_items as c
  ON a.order_id=c.order_id
  JOIN bigquery-public-data.thelook_ecommerce.products AS b
  ON c.product_id=b.id
  group by month, year, product_category
  )
select month, year, product_category,TPV,TPO,lag(TPV) over (partition by product_category order by month) as pre_rev,
 CONCAT(ROUND((TPV - LAG(TPV) OVER (PARTITION BY product_category ORDER BY month)) / LAG(TPV) OVER (PARTITION BY product_category ORDER BY month) * 100, 2),'%') as revenue_growth,
CONCAT(ROUND((TPO - LAG(TPO) OVER (PARTITION BY product_category ORDER BY month)) / LAG(TPO) OVER (PARTITION BY product_category ORDER BY month) * 100, 2), '%') as order_growth,
total_cost,
TPV-total_cost as total_profit,
TPV/total_cost as Profit_to_cost_ratio
from cte
--retention cohort--
with order_index as 
(select user_id, format_date('%Y-%m',first_purchase_date) as cohort_date, created_at,
(extract (year from created_at )- extract (year from first_purchase_date))*12
+ (extract (month from created_at )- extract (month from first_purchase_date)) +1 as index
from (
select user_id, 
min (created_at) over (partition by user_id) as first_purchase_date,
created_at
from bigquery-public-data.thelook_ecommerce.orders)),
xxx as (
select cohort_date, index, 
count(distinct user_id) as cnt
from order_index
where index between 1 and 4
group by cohort_date, index),
customer_cohort as(
select 
cohort_date,
sum(case when index=1 then cnt else 0 end) as m1,
sum(case when index=2 then cnt else 0 end) as m2,
sum(case when index=3 then cnt else 0 end) as m3,
sum(case when index=4 then cnt else 0 end) as m4
from xxx
group by cohort_date
order by cohort_date)
select 
cohort_date,
round(100.00* m1/m1,2) || '%' as m1,
round(100*m2/m1,2) || '%'  as m2,
round (100*m3/m1,2) || '%' as m3 ,
round (100*m4/m1,2 )|| '%' as m4
from customer_cohort

