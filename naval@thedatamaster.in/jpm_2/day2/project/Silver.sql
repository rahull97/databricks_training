-- Databricks notebook source
use catalog dev;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("naval_bronze.transaction")
-- MAGIC dffinal=df.dropDuplicates().drop("_rescued_data","ingestion_date").dropna()
-- MAGIC dffinal.write.mode("overwrite").saveAsTable("dev.naval_silver.transaction")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("naval_bronze.account_info")
-- MAGIC dffinal=df.dropDuplicates().dropna()
-- MAGIC dffinal.write.mode("overwrite").saveAsTable("dev.naval_silver.account_info")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("naval_bronze.customer_profile")
-- MAGIC dffinal=df.dropDuplicates().dropna()
-- MAGIC dffinal.write.mode("overwrite").saveAsTable("dev.naval_silver.customer_profile")
