-- Databricks notebook source
View:
(Virtual table)
1.Standard View-- persited (SQL)
2.temp view--- temp (sql) or pyspark

-- COMMAND ----------

select * from dev.naval.nifty_stock

-- COMMAND ----------

create or replace view dev.naval.stock_industry as 
(select Industry, count(*) as count from dev.naval.nifty_stock group by all)

-- COMMAND ----------

create or replace temp view stock_industry_temp as 
(select Industry, count(*) as count from dev.naval.nifty_stock group by all)

-- COMMAND ----------

use catalog dev;
show views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("s3://jpmdatabricks/metadata/super_store.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("super_store_temp")

-- COMMAND ----------

select * from super_store_temp
