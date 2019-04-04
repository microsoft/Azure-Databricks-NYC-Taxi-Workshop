# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Cosmos DB - stream ingest primer - with aggregation focus
# MAGIC In this exercise, we will:<br>
# MAGIC Consume crime events from Azure Event Hub, with Spark structured streaming, and **compute count by crime type and persist** to Azure Cosmos DB collection.
# MAGIC <br>
# MAGIC 
# MAGIC **Dependency:** <br>
# MAGIC Publish events to Azure Event Hub<br>
# MAGIC 
# MAGIC **Docs:**<br>
# MAGIC Databricks - Azure Cosmos DB: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html<br>
# MAGIC Azure Cosmos DB - Spark connector guide: https://github.com/Azure/azure-cosmosdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide<br>
# MAGIC Azre Cosmos DB - Spark connector - performance: https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0. Read from Azure Event Hub with Spark structured streaming

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

consumableDF = partParsedStreamDF.select( \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.primary_type").alias("case_type"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.primary_type").alias("id"))  
aggregatedDF = consumableDF.groupBy("case_type","id").count()     
consumableDF.printSchema  

# COMMAND ----------

# Console output of parsed events
# aggregatedDF.writeStream.outputMode("complete").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

aggregatedDF.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.0. Persist the stream to Azure Cosmos DB - SQL API (Document store)

# COMMAND ----------

#// 1) Credentials - Cosmos DB
cdbEndpoint = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-uri")
cdbAccessKey = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-key")

# COMMAND ----------

#// 2) AEH checkpoint 
dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-sub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=True) #//remove if needed

# COMMAND ----------

#// 3) Write configuration
writeConfig = {
 "Endpoint" : cdbEndpoint,
 "Masterkey" : cdbAccessKey,
 "Database" : "gws-db",
 "Collection" : "flights_fromsea",
 "Upsert" : "chicago_crimes_curated_stream_aggr"
}

# COMMAND ----------

#// 4) Persist to Cosmos DB

# HUSSEIN - Need code here

query = aggregatedDF
  .writeStream
  .outputMode("complete")
  .format(classOf[CosmosDBSinkProvider].getName).options(cosmosDbWriteConfigMap)
  .option("checkpointLocation",dbfsCheckpointDirPath)
  .trigger(Trigger.ProcessingTime(1000 * 1)) 
  .start()