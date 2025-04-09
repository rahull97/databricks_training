# Databricks notebook source
Delta
1. SQL
2. Python
3. Dataframe

# COMMAND ----------

df=spark.read.csv("path")
df.write.saveAsTable("tbl_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev.naval.test(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dev.naval.test

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail dev.naval.test

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dev.naval.test

# COMMAND ----------

Internal of delta
1. parquet (data)
+
2. _delta_log(metadata)
    i. crc
    ii. json
    iii. checkpoint parquet file



# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev.naval.test values(11,'a');
# MAGIC insert into dev.naval.test values(22,'c');
# MAGIC insert into dev.naval.test values(33,'d');
# MAGIC insert into dev.naval.test values(233,'d');
# MAGIC insert into dev.naval.test values(23,'d');
# MAGIC insert into dev.naval.test values(43,'d');
# MAGIC insert into dev.naval.test values(353,'d');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.test version as of 16
# MAGIC --select * from dev.naval.test --timestamp as of '2025-04-08T05:52:04.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table dev.naval.test to version as of 16

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from dev.naval.test

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from dev.naval.test where id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.test

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize  dev.naval.test
# MAGIC zorder by (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.test

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum dev.naval.test

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum dev.naval.test retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum dev.naval.test retain 0 hours 
