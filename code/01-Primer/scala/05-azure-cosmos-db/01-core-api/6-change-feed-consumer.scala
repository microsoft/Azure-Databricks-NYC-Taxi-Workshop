// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Cosmos DB - "change feed" - stream ingest primer 
// MAGIC In this exercise, we will:<br>
// MAGIC Consume change feed from Azure Cosmos DB, with Spark structured streaming, and persist to Databricks Delta.
// MAGIC <br>
// MAGIC 
// MAGIC **Change feed:** <br>
// MAGIC Azure Cosmos DB exposes every record created/updated via a Kafkaesque change feed that you can consume from with Spark Structured Streaming.<br>
// MAGIC Note: Only the latest event of each partition key is exposed (its compacted); If you have a requirement to consume every single transaction, unless you can consumer as fast enough - for VERY frequently changing data, you risk losing transactions.
// MAGIC 
// MAGIC **Dependency:** <br>
// MAGIC Publish events to Azure Cosmos DB<br>
// MAGIC 
// MAGIC **Docs:**<br>
// MAGIC Databricks - Azure Cosmos DB: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html<br>
// MAGIC Azure Cosmos DB - Spark connector guide: https://github.com/Azure/azure-cosmosdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide<br>
// MAGIC Azre Cosmos DB - Spark connector - performance: https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips<br>

// COMMAND ----------

import org.apache.spark.sql.functions._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._
import java.time._


// 1) Destination directory for delta table, and for checkpoints
val dbfsDestDirPath="/mnt/workshop/curated/crimes/chicago-crimes-stream-change-feed/"

// 2) Checkpoint related
val dbfsCheckpointDirPathCF="/mnt/workshop/scratch/checkpoints-crimes-cdb-sub/"
val dbfsCheckpointDirPathDBFS="/mnt/workshop/scratch/checkpoints-crimes-cdb-cf-dbfs/"

// 3) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
//dbutils.fs.rm(dbfsCheckpointDirPathCF, recurse=true)
dbutils.fs.rm(dbfsCheckpointDirPathDBFS, recurse=true)

// 4) Cosmos DB secrets
val cdbEndpoint = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-uri")
val cdbAccessKey = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-key")

// COMMAND ----------

// 5) Cosmos change feed config
val readConfigMap = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbAccessKey,
  "Database" -> "gws-db",
  "Collection" -> "chicago_crimes_curated_batch",
  "changefeedstartfromthebeginning" -> "true",
  "changefeedqueryname" -> "change feed query",
  "changefeedCheckpointLocation" -> dbfsCheckpointDirPathCF)

// COMMAND ----------

// 6) Read change feed stream
val changeFeedDF = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(readConfigMap).load()

// COMMAND ----------

changeFeedDF.printSchema

// COMMAND ----------

val parsedChangeFeedDF = changeFeedDF.select("case_id","case_type", "case_year", "case_month", "case_day_of_month")

// COMMAND ----------

// 7) Console output of parsed events - for testing ONLY
// changeFeedDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// 8) Write to delta on DBFS
val query = parsedChangeFeedDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", dbfsCheckpointDirPathDBFS)
  .start(dbfsDestDirPath)