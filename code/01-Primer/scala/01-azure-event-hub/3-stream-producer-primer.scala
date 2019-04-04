// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Event Hub: Producer primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will leverage structured streaming to read crime data we downloaded and curated in the storage lab module, and publish to Azure Event Hub.<br>
// MAGIC 
// MAGIC **Dependency**: <br>
// MAGIC Completion of Azure storage lab<br>
// MAGIC 
// MAGIC **Docs**: <br>
// MAGIC Azure Event Hub - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
// MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.  Define connection string & event hub configuration

// COMMAND ----------

// 1) Define event hub configuration
import org.apache.spark.eventhubs._
//Replace connection string with your instances'.
val connectionString = dbutils.secrets.get(scope = "gws-crimes-aeh", key = "conexion-string")
val eventHubsConfWrite = EventHubsConf(connectionString)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create a dataframe containing the source data

// COMMAND ----------

val sourceDataDir = "/mnt/workshop/curated/crimes/chicago-crimes"

// COMMAND ----------

// 2) Define schema for the crime data
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType,BooleanType, DecimalType}

val DecimalType = org.apache.spark.sql.types.DataTypes.createDecimalType(10, 7)

val crimesSchema = StructType(Array(
    StructField("case_id", StringType, true),
    StructField("case_nbr", StringType, true),
    StructField("case_dt_tm", StringType, true),
    StructField("block", StringType, true),
    StructField("iucr", StringType, true),
    StructField("primary_type", StringType, true),
    StructField("description", StringType, true),
    StructField("location_description", StringType, true),
    StructField("arrest_made", StringType, true),
    StructField("was_domestic", StringType, true),
    StructField("beat", StringType, true),
    StructField("district", StringType, true),
    StructField("ward", StringType, true),
    StructField("community_area", StringType, true),
    StructField("fbi_code", StringType, true),
    StructField("x_coordinate", StringType, true),
    StructField("y_coordinate", StringType, true),
    StructField("case_year", StringType, true),
    StructField("updated_dt", StringType, true),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true),
    StructField("location_coords", StringType, true),
    StructField("case_timestamp", TimestampType, true),
    StructField("case_month", IntegerType, true),
    StructField("case_day_of_month", IntegerType, true),
    StructField("case_hour", IntegerType, true),
    StructField("case_day_of_week_nbr", IntegerType, true),
    StructField("case_day_of_week_name", StringType, true),
    StructField("latitude_dec", DecimalType, true),
    StructField("longitude_dec", DecimalType, true)
))

// COMMAND ----------

// 3a) Lets check out source to ensure we have the data we need
val sourceDF = spark.read.schema(crimesSchema).load(sourceDataDir)

// 3b) Materialize
sourceDF.show

// COMMAND ----------

// 4) Using structured streaming, read from storage into a dataframe
// Get the location of the curated crimes data from the storage lab - we will need to load from there
val sourceDF = spark.readStream.schema(crimesSchema).load(sourceDataDir)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Format the dataframe into an Event Hub compatible format
// MAGIC You will need to publish your data as a key-value pair of partition key and payload.<br> 
// MAGIC The author has chosen: 
// MAGIC - JSON as format for the payload
// MAGIC - The case ID for the partition key

// COMMAND ----------

// 5) Create dataframe in the format that event hub expects
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._ 

val producerDF = sourceDF.select($"case_id" as "partitionKey", (to_json(struct(
$"case_id",$"case_nbr",$"case_dt_tm",$"block",$"iucr",$"primary_type",$"description",$"location_description",$"arrest_made",$"was_domestic",$"beat",$"district",$"ward",$"community_area",$"fbi_code",$"x_coordinate",$"y_coordinate",$"case_year",$"updated_dt",$"latitude",$"longitude",$"location_coords",$"case_timestamp",$"case_month",$"case_day_of_month",$"case_hour",$"case_day_of_week_nbr",$"case_day_of_week_name",$"latitude_dec",$"longitude_dec"))).cast(StringType) as "body")

// COMMAND ----------

// 6) Review schema
producerDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ###  4. Publish to Azure Event Hub

// COMMAND ----------

// 7) Stream crime events to event hub
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// 8) Create a checkpoint directory 
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-pub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

// COMMAND ----------

// 9) For testing - stream to console
//producerDF.writeStream.outputMode("append").format("console").trigger(ProcessingTime("2 seconds")).start().awaitTermination()

// COMMAND ----------

// 10) Stream write to event hub

val query = producerDF
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .option("checkpointLocation", dbfsCheckpointDirPath)
    .options(eventHubsConfWrite.toMap)
    .trigger(ProcessingTime("2 seconds"))
    .start()
