# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC
# MAGIC 1) Database definition<BR> 
# MAGIC 2) External remote JDBC table definition

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create the taxi_db database in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1. Create database

# COMMAND ----------

from libs.dbname import dbname
taxi_db = dbname(db="taxi_db")
print("New db name: " + taxi_db)
spark.conf.set("nbvars.taxi_db", taxi_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.taxi_db};

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

display(spark.catalog.listDatabases())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create tables for trips and job history

# COMMAND ----------

# MAGIC %md
# MAGIC Create the following 3 tables:<br>
# MAGIC BATCH_JOB_HISTORY => Persist ETL job metadata<br>
# MAGIC TRIPS_BY_YEAR     => Report<br>
# MAGIC TRIPS_BY_HOUR     => Report<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxi_db.BATCH_JOB_HISTORY;
# MAGIC CREATE TABLE taxi_db.BATCH_JOB_HISTORY(
# MAGIC batch_id int,
# MAGIC batch_step_id int,
# MAGIC batch_step_description varchar(100),
# MAGIC batch_step_status varchar(30),
# MAGIC batch_step_time varchar(30)
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS taxi_db.TRIPS_BY_YEAR;
# MAGIC CREATE TABLE taxi_db.TRIPS_BY_YEAR(
# MAGIC taxi_type varchar(30),
# MAGIC trip_year int,
# MAGIC trip_count bigint
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS taxi_db.TRIPS_BY_HOUR;
# MAGIC CREATE TABLE taxi_db.TRIPS_BY_HOUR(
# MAGIC taxi_type varchar(30),
# MAGIC trip_year int,
# MAGIC pickup_hour int,
# MAGIC trip_count bigint
# MAGIC );

# COMMAND ----------


