// Databricks notebook source
// MAGIC %md
// MAGIC # Azure SQl database: Structured Streaming - consumer primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will leverage structured streaming to ingest crime data from Azure Event Hub persist to Azure SQL database.<br>
// MAGIC Because structured streaming does not natively feature a JDBC sink, we will create one (notebook 4-jdbcsink).
// MAGIC 
// MAGIC **Dependency**: <br>
// MAGIC Completion of stream-producer-primer lab.<br>
// MAGIC The producer should be running or have completed.<br>
// MAGIC 
// MAGIC **Docs**: <br>
// MAGIC Azure Event Hub - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
// MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html<br>
// MAGIC JDBC sink - blog - https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html<br>
// MAGIC JDBC sink sample notebook - https://docs.databricks.com/_static/notebooks/structured-streaming-etl-kafka.html <br>

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
val streamingDF = spark.readStream.format("eventhubs").options(aehConsumerParams.toMap).load()

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
// MAGIC ### 3.0. Sink to Azure SQL database

// COMMAND ----------

// MAGIC %md
// MAGIC 4) Create table in the database from the portal - with query explorer UI<br>
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

// 4) Credentials and connection
val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbcHostname = "gws-server.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "gws_sql_db"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// MAGIC %run ../03-azure-sql-db/4-jdbcsink

// COMMAND ----------

// 5) Persist to Azure SQL database
import org.apache.spark.sql.streaming.Trigger

val writer = new JDBCSink(jdbcUrl,jdbcUsername, jdbcPassword)
val query =
  consumableDF
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(Trigger.ProcessingTime(5000))
    .start()