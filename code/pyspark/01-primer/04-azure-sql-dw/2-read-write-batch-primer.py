# Databricks notebook source
# MAGIC %md
# MAGIC # Azure SQL Datawarehouse - batch read/write primer
# MAGIC In this exercise, we will:<br>
# MAGIC 1.  **WRITE primer**: Read curated Chicago crimes data in a Delta table, from the lab on Azure Storage, and insert into Azure SQL Datawarehouse<br>
# MAGIC 2.  **READ primer**:  Read from Azure SQL Datawarehouse<br>
# MAGIC <br>
# MAGIC **Dependency:** <br>
# MAGIC Successful completion of the primer lab on Azure Blob Storage<br>
# MAGIC 
# MAGIC **Docs:**<br>
# MAGIC https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html#

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Review of source data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CRIMES_DB.CHICAGO_CRIMES_CURATED;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from CRIMES_DB.CHICAGO_CRIMES_CURATED;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Connectivity parameters

# COMMAND ----------

#JDBC URL
jdbcURL = dbutils.secrets.get(scope = "gws-sql-dw", key = "conexion-string")

#Storage account credentials for tempDir access
spark.conf.set(
  "fs.azure.account.key.gwsblobsa.blob.core.windows.net",
  dbutils.secrets.get(scope = "gws-blob-storage", key = "storage-acct-key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Azure SQL Datawarehouse - setup

# COMMAND ----------

# MAGIC %md
# MAGIC 3.1. Create a SQL DW master key <br>
# MAGIC This is a one-time activity the first time you use the SQL Datawarehouse.<br>
# MAGIC 
# MAGIC ```
# MAGIC CREATE MASTER KEY;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2. Create table in the SQL Datawarehouse from the portal - with query explorer UI<br>
# MAGIC 
# MAGIC ```
# MAGIC DROP TABLE IF EXISTS chicago_crimes_curated_summary;
# MAGIC CREATE TABLE chicago_crimes_curated_summary
# MAGIC (case_id varchar(200),
# MAGIC primary_type varchar(200),
# MAGIC arrest_made varchar(200),
# MAGIC case_year varchar(200),
# MAGIC case_month varchar(200),
# MAGIC case_day_of_month varchar(200));
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write to Azure SQL Datawarehouse

# COMMAND ----------

# MAGIC %md
# MAGIC 4.1.  Create dataframe

# COMMAND ----------

df = spark.sql("select case_id,primary_type,cast(arrest_made as string),case_year,case_month,case_day_of_month from crimes_db.chicago_crimes_curated")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4.2.  Append mode to a pre-created table

# COMMAND ----------

df.write \
  .format("com.databricks.spark.sqldw") \
  .mode("append") \
  .option("url", jdbcURL) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "chicago_crimes_curated_summary") \
  .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir") \
  .save()

# COMMAND ----------

#Repartition for control over parallelism
#df.repartition(20000).write...

# COMMAND ----------

# MAGIC %md
# MAGIC 4.3.  Create mode to a non-existent table

# COMMAND ----------

df.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcURL) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "chicago_crimes_curated_summary_new") \
  .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC 4.4.  Overwrite mode to a pre-existing table

# COMMAND ----------

df.write \
  .format("com.databricks.spark.sqldw") \
  .mode("overwrite") \
  .option("url", jdbcURL) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "chicago_crimes_curated_summary") \
  .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Read from Azure SQL Datawarehouse

# COMMAND ----------

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcURL) \
  .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", "select primary_type as crime_type, count(*) as crime_count from chicago_crimes_curated_summary group by primary_type") \
  .load()

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Predicate and projection pushdown

# COMMAND ----------

# MAGIC %md https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html#query-pushdown-into-sql-dw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 7. Housekeeping

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Temporary directory cleaning - Azure Blob Storage:<br>
# MAGIC You need to periodically clear the scratch directory where the connector stores data.<br>
# MAGIC https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html#temporary-data-management

# COMMAND ----------

# MAGIC %md
# MAGIC 2) Temporary object cleaning - Azure SQL Datawarehouse:<br>
# MAGIC Due to driver crashes or accidental cluster termination, temporary objects that need clearing may not get cleared. <br>
# MAGIC The link below details housekeeping to be done.<br>
# MAGIC https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html#temporary-object-management