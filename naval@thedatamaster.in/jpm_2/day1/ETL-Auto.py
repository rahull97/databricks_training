# Databricks notebook source
# MAGIC %md
# MAGIC running includes notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/day1/inlcude"

# COMMAND ----------

#dbutils.widgets.help()
dbutils.widgets.text("catalog","dev")
dbutils.widgets.text("schema","default")
dbutils.widgets.text("table","tablename")
catalog=dbutils.widgets.get("catalog")
schema=dbutils.widgets.get("schema")
table_name=dbutils.widgets.get("table")

# COMMAND ----------

# DBTITLE 1,Extracting
# reading
df=spark.read.csv(f"{input_path}{table_name}.csv",header=True,inferSchema=True)

#ingestion column
df1=add_ingestion(df)

#saving to table
df1.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.spotify_songs
