with temp_order_mart as (
    select 
        id,
        max(date_time) as max_date
    from staging.temp_user_order_log
    group by
        id
)


insert into staging.user_order_log
select 
    staging.temp_user_order_log.id,
    staging.temp_user_order_log.date_time,
    staging.temp_user_order_log.city_id,
    staging.temp_user_order_log.city_name,
    staging.temp_user_order_log.customer_id,
    staging.temp_user_order_log.first_name,
    staging.temp_user_order_log.last_name,
    staging.temp_user_order_log.item_id,
    staging.temp_user_order_log.item_name,
    staging.temp_user_order_log.quantity,
    staging.temp_user_order_log.payment_amount,
    staging.temp_user_order_log.status
from staging.temp_user_order_log 
inner join temp_order_mart
on temp_order_mart.id = staging.temp_user_order_log.id
and temp_order_mart.max_date = staging.temp_user_order_log.date_time
left join staging.user_order_log
on staging.user_order_log.id = staging.temp_user_order_log.id 
where staging.user_order_log.id is null;


with temp_activity_mart as (
    select 
        id,
        max(date_time) as max_date
    from staging.temp_user_activity_log
    group by
        id
)

insert into staging.user_activity_log
select 
    staging.temp_user_activity_log.id,
    staging.temp_user_activity_log.date_time,
    staging.temp_user_activity_log.action_id,
    staging.temp_user_activity_log.customer_id,
    staging.temp_user_activity_log.quantity
from staging.temp_user_activity_log
inner join temp_activity_mart
on temp_activity_mart.max_date = staging.temp_user_activity_log.date_time
and temp_activity_mart.id = staging.temp_user_activity_log.id
left join staging.user_activity_log
on staging.user_activity_log.id = staging.temp_user_activity_log.id 
where staging.user_activity_log.id is null;


with temp_customer_mart as (
    select 
        id,
        max(date_id) as max_date

    from staging.temp_customer_research
    group by
        id
)

insert into staging.customer_research
select 
    staging.temp_customer_research.id,
    staging.temp_customer_research.date_id,
    staging.temp_customer_research.category_id,
    staging.temp_customer_research.geo_id,
    staging.temp_customer_research.sales_qty,
    staging.temp_customer_research.sales_amt
from staging.temp_customer_research
inner join temp_customer_mart
on temp_customer_mart.max_date = staging.temp_customer_research.date_id
and temp_customer_mart.id = staging.temp_customer_research.id
left join staging.customer_research
on staging.customer_research.id = staging.temp_customer_research.id 
where staging.customer_research.id is null;







