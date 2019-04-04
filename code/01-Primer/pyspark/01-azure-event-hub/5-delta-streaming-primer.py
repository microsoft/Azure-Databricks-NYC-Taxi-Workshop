# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Event Hub: Delta primer for a streaming scenario
# MAGIC 
# MAGIC ### What's in this exercise?
# MAGIC Databricks Delta in the streaming scenario.<br>
# MAGIC 
# MAGIC **Dependency**: <br>
# MAGIC Completion of stream-consumer-primer lab.<br>
# MAGIC 
# MAGIC **Docs**: <br>
# MAGIC Azure Event Hub - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
# MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0. Create external table on Delta dataset
# MAGIC In the previous lab, we ingested from Event Hub and sinked to DBFS in Delta format.
# MAGIC In this section, we will create an external table on top of the dataset, so we can query it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS crimes_delta_db;
# MAGIC 
# MAGIC USE crimes_delta_db;
# MAGIC DROP TABLE IF EXISTS chicago_crimes_stream_aeh;
# MAGIC CREATE TABLE chicago_crimes_stream_aeh
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/workshop/curated/crimes/chicago-crimes-stream-delta-aeh/";

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0. Run queries on the unbounded table
# MAGIC In the previous lab, we ingested from Event Hub and sinked to DBFS in Delta format.
# MAGIC In this section, we will create an external table on top of the dataset, so we can query it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from crimes_delta_db.chicago_crimes_stream_aeh;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Repeated runs will show the count incrementing
# MAGIC SELECT count(*) from crimes_delta_db.chicago_crimes_stream_aeh;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.0. Optimizing performance
# MAGIC In the display below you will see very tiny files in < 35KB size. These are not optimal for processing with Spark and will impact peformance.<br>
# MAGIC We will run OPTMIMIZE <tablename> command to conact the small files into larger ones.<br>
# MAGIC One of the responsibilties of a data engineer is to schedule periodic compaction.

# COMMAND ----------

#The myriads of files
display(dbutils.fs.ls("/mnt/workshop/curated/crimes/chicago-crimes-stream-delta-aeh/"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Number of files at query execution time
# MAGIC DESCRIBE DETAIL crimes_delta_db.chicago_crimes_stream_aeh;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Run compaction
# MAGIC OPTIMIZE crimes_delta_db.chicago_crimes_stream_aeh;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Recheck file count
# MAGIC DESCRIBE DETAIL crimes_delta_db.chicago_crimes_stream_aeh;