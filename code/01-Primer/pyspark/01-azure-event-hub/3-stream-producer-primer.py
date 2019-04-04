# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Event Hub: Producer primer
# MAGIC 
# MAGIC ### What's in this exercise?
# MAGIC We will leverage structured streaming to read crime data we downloaded and curated in the storage lab module, and publish to Azure Event Hub.<br>
# MAGIC 
# MAGIC **Dependency**: <br>
# MAGIC Completion of Azure storage lab<br>
# MAGIC 
# MAGIC **Docs**: <br>
# MAGIC Azure Event Hub - Scala - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
# MAGIC Azure Event Hub - Python - Databricks integration: https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md<br>
# MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Define connection string & event hub configuration

# COMMAND ----------

# 1) Define event hub configuration

connectionString = dbutils.secrets.get(scope = "gws-crimes-aeh", key = "conexion-string")
ehConf = {
  'eventhubs.connectionString' : connectionString
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create a dataframe containing the source data

# COMMAND ----------

sourceDataDir = "/mnt/workshop/curated/crimes/chicago-crimes"

# COMMAND ----------

# 2) Define schema for the crime data
from pyspark.sql.types import *

crimesSchema = StructType([
    StructField("case_id", StringType(), True),
    StructField("case_nbr", StringType(), True),
    StructField("case_dt_tm", StringType(), True),
    StructField("block", StringType(), True),
    StructField("iucr", StringType(), True),
    StructField("primary_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("location_description", StringType(), True),
    StructField("arrest_made", StringType(), True),
    StructField("was_domestic", StringType(), True),
    StructField("beat", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("community_area", StringType(), True),
    StructField("fbi_code", StringType(), True),
    StructField("x_coordinate", StringType(), True),
    StructField("y_coordinate", StringType(), True),
    StructField("case_year", StringType(), True),
    StructField("updated_dt", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("location_coords", StringType(), True),
    StructField("case_timestamp", TimestampType(), True),
    StructField("case_month", IntegerType(), True),
    StructField("case_day_of_month", IntegerType(), True),
    StructField("case_hour", IntegerType(), True),
    StructField("case_day_of_week_nbr", IntegerType(), True),
    StructField("case_day_of_week_name", StringType(), True)
])

# COMMAND ----------

#// 3a) Lets check out source to ensure we have the data we need
sourceDF = spark.read.schema(crimesSchema).load(sourceDataDir)

#// 3b) Materialize
display(sourceDF)

# COMMAND ----------

#// 4) Using structured streaming, read from storage into a dataframe
#// Get the location of the curated crimes data from the storage lab - we will need to load from there
sourceDF = spark.readStream.schema(crimesSchema).load(sourceDataDir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Format the dataframe into an Event Hub compatible format
# MAGIC You will need to publish your data as a key-value pair of partition key and payload.<br> 
# MAGIC The author has chosen: 
# MAGIC - JSON as format for the payload
# MAGIC - The case ID for the partition key

# COMMAND ----------

producerDF = sourceDF.selectExpr("case_id AS partitionKey", "to_json(struct(*)) AS body")

# COMMAND ----------

#display(producerDF)

# COMMAND ----------

#// 6) Review schema
producerDF.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ###  4. Publish to Azure Event Hub

# COMMAND ----------

#// 7) Create a checkpoint directory 
dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-pub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=True)

# COMMAND ----------

# 8) For testing - stream to console
# producerDF.writeStream.outputMode("append").format("console").trigger(ProcessingTime("2 seconds")).start().awaitTermination()

# COMMAND ----------

# 9) Stream write to event hub

query = producerDF.writeStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .option("checkpointLocation", dbfsCheckpointDirPath) \
  .start()
