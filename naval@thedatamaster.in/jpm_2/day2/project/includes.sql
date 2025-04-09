-- Databricks notebook source
create schema if not exists dev.naval_bronze;
create schema if not exists dev.naval_silver;
create schema if not exists dev.naval_gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_path="/Volumes/dev/naval/raw"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def add_ingestion(df):
-- MAGIC     df1=df.withColumn("ingestion_date",current_date())
-- MAGIC     return df1
