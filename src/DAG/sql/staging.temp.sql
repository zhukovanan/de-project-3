CREATE TABLE IF NOT EXISTS staging.temp_user_activity_log(
   id serial ,
   date_time          TIMESTAMP ,
   action_id             BIGINT ,
   customer_id             BIGINT ,
   quantity             BIGINT ,
   load_id TIMESTAMP,
   run_id text,
   PRIMARY KEY (id,load_id, run_id)
);
CREATE INDEX IF NOT EXISTS main_temp1 ON staging.temp_user_activity_log (customer_id);

CREATE TABLE IF NOT EXISTS staging.temp_customer_research(
   id serial,
   date_id          TIMESTAMP ,
   category_id integer,
   geo_id   integer,
   sales_qty    integer,
   sales_amt numeric(14,2),
   load_id TIMESTAMP,
   run_id text,
   PRIMARY KEY (id, load_id, run_id)
);
CREATE INDEX IF NOT EXISTS main_temp3 ON staging.temp_customer_research (category_id);

CREATE TABLE IF NOT EXISTS staging.temp_user_order_log(
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
   load_id TIMESTAMP,
   run_id text,
   PRIMARY KEY (id,load_id, run_id)
);
CREATE INDEX IF NOT EXISTS main_temp2 ON staging.temp_user_order_log (customer_id);