# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/jpm_2/day2/project/includes"

# COMMAND ----------

# DBTITLE 1,read_files
# MAGIC %sql
# MAGIC create or replace table dev.naval_bronze.transaction
# MAGIC as 
# MAGIC (SELECT *,current_timestamp() as ingestion_date FROM read_files(
# MAGIC     '/Volumes/dev/naval/raw/transactions.csv',
# MAGIC     format => 'csv'))

# COMMAND ----------

df=spark.read.json(f"{input_path}/account_info.json")
df.write.mode("overwrite").saveAsTable("dev.naval_bronze.account_info")

# COMMAND ----------

df=spark.read.json(f"{input_path}/customer_prfile.json")
df.write.mode("overwrite").saveAsTable("dev.naval_bronze.customer_profile")
