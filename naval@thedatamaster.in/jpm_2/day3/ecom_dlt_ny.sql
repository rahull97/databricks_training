-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE sales_dlt
AS SELECT *,current_timestamp() as ingestion_date FROM STREAM read_files(
  "/Volumes/dev/naval/sales/",
   format => "csv"
   );

-- COMMAND ----------

create or refresh streaming table naval_silver.sales_dlt_clean 
(CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
as select distinct * except(_rescued_data,ingestion_date ) from stream(sales_dlt)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_dlt
AS SELECT *, current_timestamp() as ingestion_date FROM STREAM read_files(
  "/Volumes/dev/naval/customers/",
   format => "csv"
   );

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE naval_silver.customer_silver_scd;

APPLY CHANGES INTO
 naval_silver.customer_silver_scd
FROM
  stream(customers_dlt)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum, ingestion_date)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE naval_silver.products_dlt_scd1;

APPLY CHANGES INTO
  naval_silver.products_dlt_scd1
FROM
  stream(products_dlt)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum, ingestion_date)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE products_dlt
AS SELECT *,current_timestamp() as ingestion_date FROM STREAM read_files(
  "/Volumes/dev/naval/products/",
   format => "csv"
   );

-- COMMAND ----------

create or refresh materialized view naval_gold.customers_active as 
select * from naval_silver.customer_silver_scd where `__END_AT` is null

-- COMMAND ----------

create or refresh materialized view naval_gold.summary as 
(SELECT 
    sales.order_id,
    sales.customer_id,
    sales.transaction_id,
    sales.product_id,
    sales.quantity,
    sales.discount_amount,
    sales.total_amount,
    sales.order_date,
    products.product_name,
    products.product_category,
    products.product_price,
    customers.customer_name,
    customers.customer_email
FROM 
    naval_silver.sales_dlt_clean AS sales
JOIN 
    naval_silver.products_dlt_scd1 AS products
ON 
    sales.product_id = products.product_id
JOIN 
    naval_gold.customers_active AS customers
ON 
    sales.customer_id = customers.customer_id)
