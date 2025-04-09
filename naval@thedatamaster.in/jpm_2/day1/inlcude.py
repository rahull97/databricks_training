# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path="/Volumes/dev/naval/raw/"

# COMMAND ----------

def add_ingestion(df):
    df1=df.withColumn("ingestion_date",current_date())
    return df1
