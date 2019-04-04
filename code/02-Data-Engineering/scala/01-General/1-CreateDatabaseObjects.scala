// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC 1) Database definition<BR> 
// MAGIC 2) External remote JDBC table definition

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create the taxi_db database in Databricks

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.1. Create database

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.2. Validate

// COMMAND ----------

spark.catalog.listDatabases.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create tables in Azure SQL database table from the portal - data explorer

// COMMAND ----------

// MAGIC %md
// MAGIC Create the following 3 tables:<br>
// MAGIC BATCH_JOB_HISTORY => Persist ETL job metadata<br>
// MAGIC TRIPS_BY_YEAR     => Report<br>
// MAGIC TRIPS_BY_HOUR     => Report<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC DROP TABLE IF EXISTS dbo.BATCH_JOB_HISTORY;
// MAGIC CREATE TABLE BATCH_JOB_HISTORY(
// MAGIC batch_id int,
// MAGIC batch_step_id int,
// MAGIC batch_step_description varchar(100),
// MAGIC batch_step_status varchar(30),
// MAGIC batch_step_time varchar(30)
// MAGIC );
// MAGIC 
// MAGIC DROP TABLE IF EXISTS TRIPS_BY_YEAR;
// MAGIC CREATE TABLE TRIPS_BY_YEAR(
// MAGIC taxi_type varchar(30),
// MAGIC trip_year int,
// MAGIC trip_count bigint
// MAGIC );
// MAGIC 
// MAGIC DROP TABLE IF EXISTS TRIPS_BY_HOUR;
// MAGIC CREATE TABLE TRIPS_BY_HOUR(
// MAGIC taxi_type varchar(30),
// MAGIC trip_year int,
// MAGIC pickup_hour int,
// MAGIC trip_count bigint
// MAGIC );
// MAGIC 
// MAGIC ```