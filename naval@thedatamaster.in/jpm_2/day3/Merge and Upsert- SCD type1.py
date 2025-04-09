# Databricks notebook source
data=([(1,'Ravi','Pune'),(2,'Vijay','Mumbai'),(3,'Ajay','Bangalore')])
schema='id INT, name STRING, city STRING'
df=spark.createDataFrame(data,schema)

# COMMAND ----------

data=([(1,'Ravi','Pune'),(2,'Vijay','Hyderbad'),(3,'Ajay','Delhi'),(5,'Naval','Mumbai')])
schema='id INT, name STRING, city STRING'
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev.naval.target_emp(id int, name string, city string)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.target_emp

# COMMAND ----------

#df.write.mode("append").saveAsTable("cgi_dev.naval.target_emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert * into cgi_dev.naval.target_emp from source_view

# COMMAND ----------

https://docs.delta.io/latest/delta-update.html

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dev.naval.target_emp t
# MAGIC USING source_view s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     name = s.name,
# MAGIC     city = s.city
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     id,
# MAGIC     name,
# MAGIC     city
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.id,
# MAGIC     s.name,
# MAGIC     s.city
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.target_emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.naval.target_emp version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dev.naval.target_emp
