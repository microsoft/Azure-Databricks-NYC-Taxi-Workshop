# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC
# MAGIC 1) Create a student-specific database<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create the taxi_db database in Databricks

# MAGIC Specific for the student

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
