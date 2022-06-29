CREATE TABLE IF NOT EXISTS staging.user_order_log(
   id serial ,
   date_time          TIMESTAMP ,
   city_id integer,
   city_name varchar (100),
   customer_id             BIGINT ,
   first_name varchar (100),
   last_name varchar (100),
   item_id integer,
   item_name    varchar(100),
   quantity             BIGINT ,
   payment_amount numeric(14,2),
   status varchar(30) default 'shipped',
   PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS main2 ON staging.user_order_log (customer_id);