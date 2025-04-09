-- Databricks notebook source
use catalog dev;
use schema naval_silver;

-- COMMAND ----------

CREATE OR REPLACE TABLE heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  
SELECT * FROM heartrate_device

-- COMMAND ----------

CREATE OR REPLACE VIEW naval_gold.agg_heartrate AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
SELECT * FROM naval_gold.agg_heartrate

-- COMMAND ----------

-- DBTITLE 1,Column
CREATE OR REPLACE VIEW naval_gold.agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('Bangalore') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('Bangalore') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

select * from naval_gold.agg_heartrate

-- COMMAND ----------

-- DBTITLE 1,Row
CREATE OR REPLACE VIEW naval_gold.agg_heartrate AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

select * from naval_gold.agg_heartrate

-- COMMAND ----------

CREATE OR REPLACE FUNCTION datamask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 


-- COMMAND ----------

SELECT datamask('naval@gmail.com') AS data

-- COMMAND ----------

drop view if exists naval_gold.agg_heartrate;

CREATE or replace VIEW naval_gold.agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('mudgalneha18@gmail.com') THEN datamask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device

-- COMMAND ----------

select * from naval_gold.agg_heartrate

-- COMMAND ----------

drop view if exists naval_gold.agg_heartrate;

CREATE or replace VIEW naval_gold.agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('dataeng') THEN mrn
    ELSE datamask(mrn)
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device

-- COMMAND ----------

select * from naval_gold.agg_heartrate

-- COMMAND ----------

drop view if exists naval_gold.agg_heartrate;

CREATE or replace VIEW naval_gold.agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN datamask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

select * from naval_gold.agg_heartrate
