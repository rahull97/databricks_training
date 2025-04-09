-- Databricks notebook source
select * from dev.naval_silver.sales_dlt_clean 

-- COMMAND ----------

select * from dev.naval_silver.products_dlt_scd1

-- COMMAND ----------

select distinct * from dev.naval_bronze.products_dlt

-- COMMAND ----------

select * from dev.naval_silver.customer_silver_scd where `__END_AT` is null

-- COMMAND ----------

select distinct * from dev.naval_bronze.customers_dlt
