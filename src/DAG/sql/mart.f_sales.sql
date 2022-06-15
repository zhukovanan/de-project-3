insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select 
    dc.date_id, 
    item_id, 
    customer_id, 
    city_id, 
    CASE WHEN status = 'refunded' then -1*quantity else quantity end, 
    CASE WHEN status = 'refunded' then -1*payment_amount else payment_amount end,
    status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}'
and not exists (select *
                from mart.f_sales as fc
                where fc.date_id = dc.date_id
                and fc.item_id = uol.item_id
                and fc.customer_id = uol.customer_id );