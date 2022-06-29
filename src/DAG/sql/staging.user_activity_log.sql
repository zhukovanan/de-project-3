CREATE TABLE IF NOT EXISTS staging.user_activity_log(
   id serial ,
   date_time          TIMESTAMP ,
   action_id             BIGINT ,
   customer_id             BIGINT ,
   quantity             BIGINT ,
   PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS main1 ON staging.user_activity_log (customer_id);