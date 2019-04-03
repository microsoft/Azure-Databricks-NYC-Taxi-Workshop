# Databricks notebook source
# MAGIC %md
# MAGIC # Prep Data for ML Module
# MAGIC 
# MAGIC If you would like to run only Module 3 and not all of the other modules, then this notebook should be executed.
# MAGIC 
# MAGIC __NOTE:__ _Only one person needs to do this per Databricks workspace_

# COMMAND ----------

# DBTITLE 1,Create SparkSQL Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;

# COMMAND ----------

# DBTITLE 1,Download File to Temp Directory
# MAGIC %sh wget https://publicmldataeastus2.blob.core.windows.net/nyc-subset/nyc_taxi_sample.parquet --output-document /dbfs/tmp/nyc_taxi_sample.parquet

# COMMAND ----------

# DBTITLE 1,Load File as Spark DataFrame
df = spark.read.parquet('/tmp/nyc_taxi_sample.parquet')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxi_db.taxi_trips_mat_view;

# COMMAND ----------

# DBTITLE 1,Save as SparkSQL Table
df.write.format("delta").mode("overwrite").saveAsTable('taxi_db.taxi_trips_mat_view')

# COMMAND ----------

# DBTITLE 1,Remove Temp File
# MAGIC %sh rm /dbfs/tmp/nyc_taxi_sample.parquet