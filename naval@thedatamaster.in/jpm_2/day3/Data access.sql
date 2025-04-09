-- Databricks notebook source
grant privileges on object object_name to users

-- COMMAND ----------

grant usage on schema dev.naval_silver to `account users`

-- COMMAND ----------

show grants on schema dev.naval_silver
