# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Event Hub: Stream consumer primer
# MAGIC 
# MAGIC ### What's in this exercise?
# MAGIC We will leverage structured streaming to ingest crime data from Azure Event Hub and persist to DBFS, to Delta.<br>
# MAGIC 
# MAGIC **Dependency**: <br>
# MAGIC Completion of stream-producer-primer lab.<br>
# MAGIC The producer should be running or have completed.<br>
# MAGIC 
# MAGIC **Docs**: <br>
# MAGIC Azure Event Hub - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html<br>
# MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html

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

# MAGIC %md
# MAGIC ### 2.0. Parse events from Azure Event Hub

# COMMAND ----------

#// 3) Parse events
from pyspark.sql.functions import *
from pyspark.sql.types import *

consumableDF = partParsedStreamDF.select(get_json_object(partParsedStreamDF['json_payload'], "$.case_id").cast(IntegerType()).alias("case_id"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_nbr").alias("case_nbr"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_dt_tm").alias("case_dt_tm"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.block").alias("block"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.iucr").alias("iucr"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.primary_type").alias("primary_type"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.description").alias("description"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.location_description").alias("location_description"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.arrest_made").cast(BooleanType()).alias("arrest_made"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.was_domestic").cast(BooleanType()).alias("was_domestic"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.beat").cast(IntegerType()).alias("beat"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.district").cast(IntegerType()).alias("district"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.ward").cast(IntegerType()).alias("ward"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.fbi_code").alias("fbi_code"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.x_coordinate").cast(IntegerType()).alias("x_coordinate"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.y_coordinate").cast(IntegerType()).alias("y_coordinate"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_year").cast(IntegerType()).alias("case_year"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.updated_dt").alias("updated_dt"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.latitude").cast(DoubleType()).alias("latitude"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.longitude").cast(DoubleType()).alias("longitude"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.location_coords").alias("location_coords"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_timestamp").cast(TimestampType()).alias("case_timestamp"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_month").cast(IntegerType()).alias("case_month"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_day_of_month").cast(IntegerType()).alias("case_day_of_month"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_hour").cast(IntegerType()).alias("case_hour"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_day_of_week_nbr").cast(IntegerType()).alias("case_day_of_week_nbr"), \
                                         get_json_object(partParsedStreamDF["json_payload"], "$.case_day_of_week_name").alias("case_day_of_week_name"))  

consumableDF.printSchema      

# COMMAND ----------

#Console output of parsed events
#consumableDF.writeStream.outputMode("append").format("console").option("truncate", False).start().awaitTermination()

# COMMAND ----------

#// 4) Destination directory for delta table, and for checkpoints
dbfsDestDirPath="/mnt/workshop/curated/crimes/chicago-crimes-stream-delta-aeh/"

#// 5) AEH checkpoint related
dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-crimes-aeh-sub/"

#// 6) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=True)
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --To clean up from prior execution for repeated runs of the lab
# MAGIC USE crimes_delta_db;
# MAGIC DROP TABLE IF EXISTS chicago_crimes_stream_aeh;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.0. Sink to Databricks Delta

# COMMAND ----------

#//7) Persist as delta format (Parquet) to curated zone to a delta table
consumableDF.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", dbfsCheckpointDirPath) \
  .start(dbfsDestDirPath)