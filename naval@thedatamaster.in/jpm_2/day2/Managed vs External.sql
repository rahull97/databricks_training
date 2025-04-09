-- Databricks notebook source
-- MAGIC %python
-- MAGIC delta table
-- MAGIC table in schema+ (metadata(parquet+delta_log))
-- MAGIC
-- MAGIC Two types of table
-- MAGIC
-- MAGIC 1. managed table (metadata(parquet+delta_log)) is stored in default location( databricks managed)
-- MAGIC 2. unmanaged/ External table (metadata(parquet+delta_log)) is managed by user 

-- COMMAND ----------

create table dev.naval.sales(id int)

-- COMMAND ----------

describe extended dev.naval.sales

-- COMMAND ----------

undrop table dev.naval.sales_ext

-- COMMAND ----------

describe history dev.naval.sales_ext

-- COMMAND ----------

drop table dev.naval.sales_ext

-- COMMAND ----------

-- DBTITLE 1,Managed
df=spark.read.csv("path")
df.write.saveAsTable("tblname")

-- COMMAND ----------

-- DBTITLE 1,External table
df=spark.read.csv("path")
df.write.option("path","s3:").saveAsTable("tblname")

-- COMMAND ----------

drop table dev.naval.sales_ext;

-- COMMAND ----------

create table dev.naval.sales_ext(id int) location 's3://jpmdatabricks/externaltable/naval/sales'

-- COMMAND ----------

describe extended dev.naval.sales_ext

-- COMMAND ----------

insert into dev.naval.sales_ext values(1), (2)

-- COMMAND ----------

select * from delta.`s3://jpmdatabricks/externaltable/naval/sales`
