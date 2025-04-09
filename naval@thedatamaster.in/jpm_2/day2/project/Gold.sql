-- Databricks notebook source
create or replace view dev.naval_gold.trans_customer_info as 
(SELECT t.transaction_id, t.amount, t.transaction_type, t.timestamp,
       c.name AS customer_name, c.email, a.account_type, a.account_balance
FROM dev.naval_silver.transaction t
LEFT JOIN dev.naval_silver.customer_profile c ON t.customer_id = c.customer_id
LEFT JOIN dev.naval_silver.account_info a ON t.customer_id = a.customer_id)

-- COMMAND ----------

create or replace view dev.naval_gold.total_amount_per_customer as 
(SELECT customer_id, SUM(amount) as total_amount, COUNT(*) as txn_count
FROM dev.naval_silver.transaction
GROUP BY customer_id)
