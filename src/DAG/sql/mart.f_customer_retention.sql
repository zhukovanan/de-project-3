create table if not exists mart.f_customer_retention (
period_id int4,
period_name varchar(30) default 'weekly',
category_id int4,
new_customers_count int8,
returning_customers_count int8,
refunded_customer_count int8,
new_customers_revenue numeric(10,2),
returning_customers_revenue numeric(10,2),
customers_refunded int8,
primary key (period_id, category_id));



with data_mart as (
select 
	week_of_year,
	id,
	item_id,
	status,
	customer_id,
	payment_amount,
	case when count(id) over(partition by customer_id, week_of_year order by week_of_year) = 1 then 1 else 0 end as is_new
from de.mart.f_sales fc
inner join de.mart.d_calendar dc 
on dc.date_id = fc.date_id)

insert into mart.f_customer_retention
select 
	week_of_year as period_id,
	'weekly' as period_name,
	item_id as category_id,
	count(distinct case when is_new = 1 then customer_id end) as new_customers_count,
	count(distinct case when is_new = 0 then customer_id end) as returning_customers_count,
	count(distinct case when status = 'refunded' then customer_id end) as refunded_customer_count,
	sum(case when is_new = 1 then payment_amount end) as new_customers_revenue,
	sum(case when is_new = 0 then payment_amount end) as returning_customers_revenue,
	count(case when status = 'refunded' then id end) as customers_refunded
from data_mart
where not exists (select *
                  from mart.f_customer_retention as rf
                  where data_mart.week_of_year = rf.period_id
                  and data_mart.item_id = rf.category_id)
group by
	week_of_year,
	item_id
