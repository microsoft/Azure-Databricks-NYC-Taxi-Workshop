# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Cosmos DB - "change feed" - stream ingest primer 
# MAGIC In this exercise, we will:<br>
# MAGIC Consume change feed from Azure Cosmos DB, with Spark structured streaming, and persist to Databricks Delta.
# MAGIC <br>
# MAGIC 
# MAGIC **Change feed:** <br>
# MAGIC Azure Cosmos DB exposes every record created/updated via a Kafkaesque change feed that you can consume from with Spark Structured Streaming.<br>
# MAGIC Note: Only the latest event of each partition key is exposed (its compacted); If you have a requirement to consume every single transaction, unless you can consumer as fast enough - for VERY frequently changing data, you risk losing transactions.
# MAGIC 
# MAGIC **Dependency:** <br>
# MAGIC Publish events to Azure Cosmos DB<br>
# MAGIC 
# MAGIC **Docs:**<br>
# MAGIC Databricks - Azure Cosmos DB: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html<br>
# MAGIC Azure Cosmos DB - Spark connector guide: https://github.com/Azure/azure-cosmosdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide<br>
# MAGIC Azre Cosmos DB - Spark connector - performance: https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips<br>

# COMMAND ----------

#// 1) Destination directory for delta table, and for checkpoints
dbfsDestDirPath="/mnt/workshop/curated/crimes/chicago-crimes-stream-change-feed/"

#// 2) Checkpoint related
dbfsCheckpointDirPathCF="/mnt/workshop/scratch/checkpoints-crimes-cdb-sub/"
dbfsCheckpointDirPathDBFS="/mnt/workshop/scratch/checkpoints-crimes-cdb-cf-dbfs/"


#// 3) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=True)
dbutils.fs.rm(dbfsCheckpointDirPathCF, recurse=True)
dbutils.fs.rm(dbfsCheckpointDirPathDBFS, recurse=True)

#// 4) Cosmos DB secrets
cdbEndpoint = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-uri")
cdbAccessKey = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-key")

# COMMAND ----------

#// 5) Event Hub related
import json

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

#// 6) Destination conf - Cosmos DB
cosmosDbWriteConfigMap = {
  "Endpoint" : cdbEndpoint,
  "Masterkey" : cdbAccessKey,
  "Database" : "gws-db",
  "Collection" : "chicago_crimes_curated_batch",
  "changefeedqueryname": "change feed query",
  "changefeedstartfromthebeginning": "true",
  "changefeedCheckpointLocation": dbfsCheckpointDirPathCF,
  "Upsert" : "true"
}

# COMMAND ----------

#// 7) Read change feed stream
changeFeedDF = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(readConfigMap).load

# COMMAND ----------

#// 8) Review schema
changeFeedDF.printSchema

# COMMAND ----------

#// Extract required attributes
parsedChangeFeedDF = changeFeedDF.select("case_id","case_type", "case_year", "case_month", "case_day_of_month")

# COMMAND ----------

#// 7) Console output of parsed events - for testing ONLY
#// changeFeedDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

#// 8) Write to delta on DBFS
query = parsedChangeFeedDF.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", dbfsCheckpointDirPathDBFS) \
  .start(dbfsDestDirPath)