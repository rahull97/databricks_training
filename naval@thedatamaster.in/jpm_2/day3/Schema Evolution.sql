-- Databricks notebook source
-- MAGIC %python
-- MAGIC data=([(1,"Virat"),(2,'Sachin')])
-- MAGIC schema="id int, name string"
-- MAGIC df=spark.createDataFrame(data,schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("dev.naval.cricket_data_new")

-- COMMAND ----------

ALTER TABLE dev.naval.cricket_data_new 
SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);

ALTER TABLE dev.naval.cricket_data_new
RENAME COLUMN id TO emp_id;

-- COMMAND ----------

UPDATE dev.naval.cricket_data_new SET emp_id = CAST(emp_id AS STRING);

-- COMMAND ----------

select * from dev.naval.cricket_data_new

-- COMMAND ----------

ALTER TABLE dev.naval.cricket_data_new SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
ALTER TABLE dev.naval.cricket_data_new ALTER COLUMN emp_id TYPE int;

-- COMMAND ----------

ALTER Table dev.naval.cricket_data_new alter column emp_id type int

-- COMMAND ----------


create table dev.naval_silver.cricket
select CAST(emp_id AS STRING) , name from dev.naval.cricket_data_new

-- COMMAND ----------

describe extended dev.naval.cricket_data_new

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("dev.naval.cricket_data_new")
-- MAGIC df1=df.select(col("id").cast("bigint"),"name")
-- MAGIC df1.write.mode("overwrite").option("overwriteSchema",True).saveAsTable("dev.naval.cricket_data_new")

-- COMMAND ----------

select * from dev.naval.cricket_data_new

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=([(3,"Rohit","Batsman"),(4,'Rahul',"Bowler")])
-- MAGIC schema="id int, name string, role string"
-- MAGIC df=spark.createDataFrame(data,schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=([(3,"Rohit","Batsman"),(4,'Rahul',"Bowler")])
-- MAGIC schema="emp_id int, emp_name string, role string"
-- MAGIC df=spark.createDataFrame(data,schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev.naval.cricket_data")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("append").option("overwriteSchema", "true").saveAsTable("dev.naval.cricket_data")

-- COMMAND ----------

describe history dev.naval.cricket_data

-- COMMAND ----------

restore dev.naval.cricket_data to version as of 1

-- COMMAND ----------

select * from dev.naval.cricket_data
