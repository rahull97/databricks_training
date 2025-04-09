-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![](/Volumes/dev/naval/raw/object-model-0ed879da6c005615e8a00db9bb10823c.png)

-- COMMAND ----------

use catalog dev;
use schema naval;

-- COMMAND ----------

create schema dev.naval

-- COMMAND ----------

create table demo (id int, name string)
