// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Cosmos DB - stream ingest primer
// MAGIC In this exercise, we will:<br>
// MAGIC Consume crime events from Azure Event Hub, with Spark structured streaming, and persist to Azure Cosmos DB collection.
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

val consumableDF = partParsedStreamDF.select(get_json_object($"json_payload", "$.case_id").cast(IntegerType).alias("case_id"),
                                          get_json_object($"json_payload", "$.case_nbr").alias("case_nbr"),
                                          get_json_object($"json_payload", "$.case_dt_tm").alias("case_dt_tm"),
                                          get_json_object($"json_payload", "$.block").alias("block"),
                                          get_json_object($"json_payload", "$.iucr").alias("iucr"),
                                          get_json_object($"json_payload", "$.primary_type").alias("primary_type"),
                                          get_json_object($"json_payload", "$.description").alias("description"),
                                          get_json_object($"json_payload", "$.location_description").alias("location_description"),
                                          get_json_object($"json_payload", "$.arrest_made").cast(BooleanType).alias("arrest_made"),
                                          get_json_object($"json_payload", "$.was_domestic").cast(BooleanType).alias("was_domestic"),
                                          get_json_object($"json_payload", "$.beat").cast(IntegerType).alias("beat"),
                                          get_json_object($"json_payload", "$.district").cast(IntegerType).alias("district"),
                                          get_json_object($"json_payload", "$.ward").cast(IntegerType).alias("ward"),
                                          get_json_object($"json_payload", "$.fbi_code").alias("fbi_code"),
                                          get_json_object($"json_payload", "$.x_coordinate").cast(IntegerType).alias("x_coordinate"),
                                          get_json_object($"json_payload", "$.y_coordinate").cast(IntegerType).alias("y_coordinate"),
                                          get_json_object($"json_payload", "$.case_year").cast(IntegerType).alias("case_year"),
                                          get_json_object($"json_payload", "$.updated_dt").alias("updated_dt"),
                                          get_json_object($"json_payload", "$.latitude").cast(DoubleType).alias("latitude"),
                                          get_json_object($"json_payload", "$.longitude").cast(DoubleType).alias("longitude"),
                                          get_json_object($"json_payload", "$.location_coords").alias("location_coords"),
                                          get_json_object($"json_payload", "$.case_timestamp").cast(StringType).alias("case_timestamp"),
                                          get_json_object($"json_payload", "$.case_month").cast(IntegerType).alias("case_month"),
                                          get_json_object($"json_payload", "$.case_day_of_month").cast(IntegerType).alias("case_day_of_month"),          
                                          get_json_object($"json_payload", "$.case_hour").cast(IntegerType).alias("case_hour"),
                                          get_json_object($"json_payload", "$.case_day_of_week_nbr").cast(IntegerType).alias("case_day_of_week_nbr"),
                                          get_json_object($"json_payload", "$.case_day_of_week_name").alias("case_day_of_week_name"),
                                          get_json_object($"json_payload", "$.latitude_dec").cast(StringType).alias("latitude_dec"),
                                          get_json_object($"json_payload", "$.longitude_dec").cast(StringType).alias("longitude_dec")  
                                            )

consumableDF.printSchema

// COMMAND ----------

//Console output of parsed events
//consumableDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

consumableDF.printSchema

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
  "Collection" -> "chicago_crimes_curated_stream",
  "Upsert" -> "true")

// 3) AEH checkpoint 
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-sub/"
//dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)//remove if needed



// COMMAND ----------

// 4) Persist to Cosmos DB
val query = consumableDF
  .writeStream
  .outputMode("append")
  .format(classOf[CosmosDBSinkProvider].getName).options(cosmosDbWriteConfigMap)
  .option("checkpointLocation",dbfsCheckpointDirPath)
  .trigger(Trigger.ProcessingTime(1000 * 5)) 
  .start()