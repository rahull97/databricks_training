# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Extracting
# reading
df=spark.read.csv("/Volumes/dev/naval/raw/1000_richest_people_in_the_world.csv",header=True,inferSchema=True)

#ingestion column
df1=df.withColumn("ingestion_date",current_date()).withColumnRenamed("Net Worth (in billions)","net_worth_in_billion")

#saving to table
#df1.write.mode("overwrite").option("delta.columnMapping.mode","name").saveAsTable("dev.naval.richest_people")
df1.write.mode("overwrite").saveAsTable("dev.naval.richest_people")
