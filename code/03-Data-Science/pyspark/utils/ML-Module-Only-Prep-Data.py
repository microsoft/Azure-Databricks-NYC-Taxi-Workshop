# Databricks notebook source
# MAGIC %md
# MAGIC # Prep Data for ML Module
# MAGIC 
# MAGIC If you would like to run only Module 3 and not all of the other modules, then this notebook should be executed.
# MAGIC 
# MAGIC __NOTE:__ _Only one person needs to do this per Databricks workspace_

# COMMAND ----------

# DBTITLE 1,Create Spark SQL Database and Remove Old Table
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
# MAGIC DROP TABLE IF EXISTS taxi_db.taxi_trips_mat_view;

# COMMAND ----------

# DBTITLE 1,Download File to Temp Directory, Save as Spark SQL Table and remove the Temp File
import urllib.request
import os
from time import sleep

url = 'https://publicmldataeastus2.blob.core.windows.net/nyc-subset/nyc_taxi_sample.parquet'  
file_name = '/dbfs/tmp/nyc_taxi_sample.parquet'
download = urllib.request.urlretrieve(url, file_name)

# Sleep for 5 seconds to allow the download to fully finish
sleep(5)

df = spark.read.parquet('/tmp/nyc_taxi_sample.parquet')
df.write.format("delta").mode("overwrite").saveAsTable('taxi_db.taxi_trips_mat_view')
os.remove(file_name)