// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Event Hub: Structured Streaming - consumer primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will leverage structured streaming to ingest crime data from Azure Event Hub persist to Azure SQL Datawarehouse.<br>
// MAGIC 
// MAGIC **Dependency**: <br>
// MAGIC Completion of stream-producer-primer lab.<br>
// MAGIC The producer should be running or have completed.<br>
// MAGIC 
// MAGIC **Docs**: <br>
// MAGIC Azure Event Hub - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
// MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html<br>
// MAGIC Databricks-Azure SQL Datawarehouse - https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html#streaming-support

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read stream from Azure Event Hub

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._

// 1) AEH consumer related
// Replace connection string with your instances'.
val aehConsumerConnString = dbutils.secrets.get(scope = "gws-crimes-aeh", key = "conexion-string")
val aehConsumerParams =
  EventHubsConf(aehConsumerConnString)
  .setConsumerGroup("spark-streaming-cg")
  .setStartingPosition(EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(1000)

// COMMAND ----------

// 2) Consume from AEH
val streamingDF = spark.readStream.format("eventhubs").options(aehConsumerParams.toMap).option("numPartitions", "12").load()

val partParsedStreamDF =
  streamingDF
  .withColumn("aeh_offset", $"offset".cast(LongType))
  .withColumn("time_readable", $"enqueuedTime".cast(TimestampType))
  .withColumn("time_as_is", $"enqueuedTime".cast(LongType))
  .withColumn("json_payload", $"body".cast(StringType))
  .select("aeh_offset", "time_readable", "time_as_is", "json_payload")

partParsedStreamDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Parse events from Azure Event Hub

// COMMAND ----------

// 3) Parse events
import org.apache.spark.sql.functions._

val consumableDF = partParsedStreamDF.select(get_json_object($"json_payload", "$.case_id").cast(StringType).alias("case_id"),
                                          get_json_object($"json_payload", "$.primary_type").cast(StringType).alias("primary_type"),
                                          get_json_object($"json_payload", "$.arrest_made").cast(StringType).alias("arrest_made"),
                                          get_json_object($"json_payload", "$.case_year").cast(StringType).alias("case_year"),
                                          get_json_object($"json_payload", "$.case_month").cast(StringType).alias("case_month"),
                                          get_json_object($"json_payload", "$.case_day_of_month").cast(StringType).alias("case_day_of_month"))

consumableDF.printSchema

// COMMAND ----------

//Console output of parsed events
//consumableDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

consumableDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Sink to Azure SQL Datawarehouse

// COMMAND ----------

// MAGIC %md
// MAGIC Create table in the datawarehouse from the portal - with query explorer UI<br>
// MAGIC 
// MAGIC ```
// MAGIC create table chicago_crimes_curated_summary
// MAGIC (case_id varchar(200),
// MAGIC primary_type varchar(200),
// MAGIC arrest_made varchar(200),
// MAGIC case_year varchar(200),
// MAGIC case_month varchar(200),
// MAGIC case_day_of_month varchar(200));
// MAGIC ```

// COMMAND ----------

// 4) Credentials
//JDBC URL
val jdbcURL = dbutils.secrets.get(scope = "gws-sql-dw", key = "conexion-string")

//Storage account credentials for tempDir access
spark.conf.set(
  "fs.azure.account.key.gwsblobsa.blob.core.windows.net",
  dbutils.secrets.get(scope = "gws-blob-storage", key = "storage-acct-key"))

// COMMAND ----------

// 5) Persist to Azure SQL database
import org.apache.spark.sql.streaming.Trigger

// AEH checkpoint related
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-sub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

// Start streaming to SQL DW
val query =
  consumableDF
    .writeStream
    .format("com.databricks.spark.sqldw")
    .option("url", jdbcURL)
    .option("forwardSparkAzureStorageCredentials", "true")
    .option("dbTable", "chicago_crimes_curated_summary")
    .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir")
    .option("checkpointLocation", dbfsCheckpointDirPath)
    .trigger(Trigger.ProcessingTime(5000))
    .start()
