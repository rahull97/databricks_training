# Databricks notebook source
# MAGIC %md
# MAGIC ### Ways to create a table
# MAGIC 1. Dataframe
# MAGIC 2. SQL
# MAGIC 3. UI

# COMMAND ----------

spark

# COMMAND ----------

data structure
1. rdd
2. dataframe
3. dataset


# COMMAND ----------

spark: lazy evaluation 

Transformation
Action(show, display, count)

# COMMAND ----------

df=spark.read.csv("/Volumes/dev/naval/raw/Spotify_Songs.csv")

# COMMAND ----------

# DBTITLE 1,Extracting
#df=spark.read.csv("/Volumes/dev/naval/raw/Spotify_Songs.csv",header=True)
df=spark.read.csv("/Volumes/dev/naval/raw/Spotify_Songs.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from. 

# COMMAND ----------

df.show()

# COMMAND ----------

Transformation


1. dataframe functions
.select
.alias
.filter
.withColumnRenamed
.withColumn


2. functions
col

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,select
#df.select("*").display()
#df.select("song_id".alias("songid")).display() ## error
df.select(col("song_id").alias("songid")).display()

# COMMAND ----------

# DBTITLE 1,filter
df.filter("song_id=1")

# COMMAND ----------

df.filter("song_id=1").display()

# COMMAND ----------

df.withColumnRenamed("song_id","songid").withColumnRenamed("artist_id","artistid")

# COMMAND ----------

df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------


