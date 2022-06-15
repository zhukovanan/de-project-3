alter table de.mart.f_sales
add column status varchar(30) default 'shipped';

alter table staging.user_order_log
add column status varchar(30) default 'shipped';
