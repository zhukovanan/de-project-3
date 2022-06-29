CREATE TABLE IF NOT EXISTS staging.customer_research(
   id serial,
   date_id          TIMESTAMP ,
   category_id integer,
   geo_id   integer,
   sales_qty    integer,
   sales_amt numeric(14,2),
   PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS main3 ON staging.customer_research (category_id);