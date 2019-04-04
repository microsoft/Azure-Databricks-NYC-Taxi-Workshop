// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Cosmos DB - stream ingest primer - with aggregation focus
// MAGIC In this exercise, we will:<br>
// MAGIC Consume crime events from Azure Event Hub, with Spark structured streaming, and **compute count by crime type and persist** to Azure Cosmos DB collection.
// MAGIC <br>
// MAGIC 
// MAGIC **Dependency:** <br>
// MAGIC Publish events to Azure Event Hub<br>
// MAGIC 
// MAGIC **Docs:**<br>
// MAGIC Databricks - Azure Cosmos DB: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html<br>
// MAGIC Azure Cosmos DB - Spark connector guide: https://github.com/Azure/azure-cosmosdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide<br>
// MAGIC Azre Cosmos DB - Spark connector - performance: https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Azure Event Hub with spark structured streaming

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

// Parse events
import org.apache.spark.sql.functions._

val consumableDF = partParsedStreamDF.select(get_json_object($"json_payload", "$.primary_type").alias("case_type"),
                                            get_json_object($"json_payload", "$.primary_type").alias("id"))//Adding an "id" field - so for any case type there is only one document/record 
val aggregatedDF = consumableDF.groupBy("case_type","id").count()     
consumableDF.printSchema

// COMMAND ----------

//Console output of parsed events
//aggregatedDF.writeStream.outputMode("complete").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

aggregatedDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Persist the stream to Azure Cosmos DB - SQL API (Document store)

// COMMAND ----------

import org.apache.spark.sql.functions._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

// 1) Credentials - Cosmos DB
val cdbEndpoint = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-uri")
val cdbAccessKey = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-key")

// COMMAND ----------

// 2) Destination conf - Cosmos DB
val cosmosDbWriteConfigMap = Map(
"Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbAccessKey,
  "Database" -> "gws-db",
  "Collection" -> "chicago_crimes_curated_stream_aggr",
  "Upsert" -> "true")

// 3) AEH checkpoint 
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-sub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)//remove if needed

// 4) Persist to Cosmos DB
val query = aggregatedDF
  .writeStream
  .outputMode("complete")
  .format(classOf[CosmosDBSinkProvider].getName).options(cosmosDbWriteConfigMap)
  .option("checkpointLocation",dbfsCheckpointDirPath)
  .trigger(Trigger.ProcessingTime(1000 * 1)) 
  .start()