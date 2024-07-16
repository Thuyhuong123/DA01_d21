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


