// Databricks notebook source
// MAGIC %md
// MAGIC # HDInsight Kafka: Batch consumer - primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will ingest crime data from a Kafka topic, in batch, and persist to DBFS, to Delta.<br>
// MAGIC 
// MAGIC **Dependency**: <br>
// MAGIC Completion of stream-producer-primer lab.<br>
// MAGIC The producer should be running or have completed.<br>
// MAGIC 
// MAGIC **Docs**: <br>
// MAGIC Kafka - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/kafka.html<br>
// MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Kafka in batch

// COMMAND ----------

val kafkaTopic = "crimes_topic"
val kafkaBrokerAndPortCSV = "10.7.0.5:9092,10.7.0.6:9092,10.7.0.10:9092"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val kafkaSourceDF = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("subscribe", kafkaTopic)
  .option("kafka.group.id","workshop_batch")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .option("failOnDataLoss", "true")
  .load()

val formatedKafkaDF = kafkaSourceDF.selectExpr("CAST(key AS STRING) as case_id", "CAST(value AS STRING) as json_payload")
  .as[(String, String)]

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Parse events from kafka

// COMMAND ----------

val consumableDF = formatedKafkaDF.select(get_json_object($"json_payload", "$.case_id").cast(IntegerType).alias("case_id"),
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
                                          get_json_object($"json_payload", "$.case_timestamp").cast(TimestampType).alias("case_timestamp"),
                                          get_json_object($"json_payload", "$.case_month").cast(IntegerType).alias("case_month"),
                                          get_json_object($"json_payload", "$.case_day_of_month").cast(IntegerType).alias("case_day_of_month"),          
                                          get_json_object($"json_payload", "$.case_hour").cast(IntegerType).alias("case_hour"),
                                          get_json_object($"json_payload", "$.case_day_of_week_nbr").cast(IntegerType).alias("case_day_of_week_nbr"),
                                          get_json_object($"json_payload", "$.case_day_of_week_name").alias("case_day_of_week_name")
                                          )

// COMMAND ----------

consumableDF.show

// COMMAND ----------

consumableDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Sink to Databricks Delta

// COMMAND ----------

//Took the author 7.5 minutes

//1) Destination directory for delta table
val dbfsDestDirPath="/mnt/workshop/curated/crimes/chicago-crimes-batch-delta-kafka/"

//2) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)

//3) Persist as delta format (Parquet) to the curated zone 
consumableDF.write.format("delta").mode("overwrite").save(dbfsDestDirPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.0. Create external table

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS crimes_delta_db;
// MAGIC 
// MAGIC USE crimes_delta_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_batch_kafka;
// MAGIC CREATE TABLE chicago_crimes_batch_kafka
// MAGIC USING DELTA
// MAGIC LOCATION '/mnt/workshop/curated/crimes/chicago-crimes-batch-delta-kafka/';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.0. Run queries

// COMMAND ----------

// MAGIC %sql 
// MAGIC --Took the author 2 minutes for 1.5 GB of data/6.7 M rows
// MAGIC OPTIMIZE crimes_delta_db.chicago_crimes_batch_kafka;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from crimes_delta_db.chicago_crimes_batch_kafka;