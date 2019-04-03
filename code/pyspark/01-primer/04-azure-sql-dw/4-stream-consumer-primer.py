# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Event Hub: Structured Streaming - consumer primer
# MAGIC 
# MAGIC ### What's in this exercise?
# MAGIC We will leverage structured streaming to ingest crime data from Azure Event Hub persist to Azure SQL Datawarehouse.<br>
# MAGIC 
# MAGIC **Dependency**: <br>
# MAGIC Completion of stream-producer-primer lab.<br>
# MAGIC The producer should be running or have completed.<br>
# MAGIC 
# MAGIC **Docs**: <br>
# MAGIC Azure Event Hub - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
# MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html<br>
# MAGIC Databricks-Azure SQL Datawarehouse - https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html#streaming-support

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0. Read stream from Azure Event Hub

# COMMAND ----------

import json

#// 1) AEH consumer related
#// Replace connection string with your instances'.
connectionString = dbutils.secrets.get(scope = "gws-crimes-aeh", key = "conexion-string")
ehConf = {
  'eventhubs.connectionString' : connectionString,
  'eventhubs.consumerGroup' : "spark-streaming-cg",
}

# Start from end of stream
startOffset = "@latest"

# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

# Put the positions into the Event Hub config dictionary
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

#// 2) Consume from AEH
streamingDF = spark.readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0. Parse events from Azure Event Hub

# COMMAND ----------

#// 3) Parse
from pyspark.sql.types import *

partParsedStreamDF = streamingDF \
  .withColumn("aeh_offset", streamingDF["offset"].cast(LongType())) \
  .withColumn("time_readable", streamingDF["enqueuedTime"].cast(TimestampType())) \
  .withColumn("time_as_is", streamingDF["enqueuedTime"].cast(LongType())) \
  .withColumn("json_payload", streamingDF["body"].cast(StringType())) \
  .select("aeh_offset", "time_readable", "time_as_is", "json_payload") 

partParsedStreamDF.printSchema
partParsedStreamDF.createOrReplaceTempView("part_parsed_stream")

# COMMAND ----------

#// 4) Parse further
from pyspark.sql.functions import *
from pyspark.sql.types import *

consumableDF = partParsedStreamDF.select(get_json_object(partParsedStreamDF['json_payload'], "$.case_id").cast(IntegerType()).alias("case_id"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.primary_type").alias("primary_type"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.arrest_made").cast(BooleanType()).alias("arrest_made"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_year").cast(IntegerType()).alias("case_year"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_month").cast(IntegerType()).alias("case_month"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_day_of_month").cast(IntegerType()).alias("case_day_of_month"))  

consumableDF.printSchema      

# COMMAND ----------

#Console output of parsed events
#consumableDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

consumableDF.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.0. Sink to Azure SQL Datawarehouse

# COMMAND ----------

# MAGIC %md
# MAGIC Create table in the datawarehouse from the portal - with query explorer UI<br>
# MAGIC 
# MAGIC ```
# MAGIC create table chicago_crimes_curated_summary
# MAGIC (case_id varchar(200),
# MAGIC primary_type varchar(200),
# MAGIC arrest_made varchar(200),
# MAGIC case_year varchar(200),
# MAGIC case_month varchar(200),
# MAGIC case_day_of_month varchar(200));
# MAGIC ```

# COMMAND ----------

#JDBC URL
jdbcURL = dbutils.secrets.get(scope = "gws-sql-dw", key = "conexion-string")

#Storage account credentials for tempDir access
spark.conf.set(
  "fs.azure.account.key.gwsblobsa.blob.core.windows.net",
  dbutils.secrets.get(scope = "gws-blob-storage", key = "storage-acct-key"))

# COMMAND ----------

#// 5) Persist to Azure SQL database

#// AEH checkpoint related
dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-sub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=True)

#// Start streaming to SQL DW

consumableDF.writeStream \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcURL) \
  .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "chicago_crimes_curated_summary") \
  .option("checkpointLocation", dbfsCheckpointDirPath) \
  .start()

# COMMAND ----------

