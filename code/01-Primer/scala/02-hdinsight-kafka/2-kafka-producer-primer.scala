// Databricks notebook source
// MAGIC %md
// MAGIC # HDInsight Kafka: Producer primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will leverage structured streaming to read crime data we downloaded and curated in the storage lab module, and publish to a topic created in a HDInsight Kafka cluster.<br>
// MAGIC 
// MAGIC **Dependency**: <br>
// MAGIC Completion of Azure storage lab<br>
// MAGIC 
// MAGIC **Docs**: <br>
// MAGIC Kafka - Databricks integration: https://docs.databricks.com/spark/latest/structured-streaming/kafka.html<br>
// MAGIC Structured Streaming - https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 1.0. Publish to Kafka

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.1.  Define topic and broker list

// COMMAND ----------

val kafkaTopic = "crimes_topic"
val kafkaBrokerAndPortCSV = "10.7.0.5:9092,10.7.0.6:9092,10.7.0.10:9092"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2. Create a dataframe containing the source data

// COMMAND ----------

val sourceDF = spark.sql("SELECT * FROM crimes_db.chicago_crimes_curated")
sourceDF.printSchema
sourceDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.3. Format the dataframe into a Kafka compatible format
// MAGIC You will need to publish your data as a key value pair.

// COMMAND ----------

import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._ 

val producerDF = sourceDF.select($"case_id" as "key", (to_json(struct(
$"case_id",$"case_nbr",$"case_dt_tm",$"block",$"iucr",$"primary_type",$"description",$"location_description",$"arrest_made",$"was_domestic",$"beat",$"district",$"ward",$"community_area",$"fbi_code",$"x_coordinate",$"y_coordinate",$"case_year",$"updated_dt",$"latitude",$"longitude",$"location_coords",$"case_timestamp",$"case_month",$"case_day_of_month",$"case_hour",$"case_day_of_week_nbr",$"case_day_of_week_name"))) as "value")

// COMMAND ----------

producerDF.printSchema
producerDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC #####  2.0.4. Publish to Kafka

// COMMAND ----------

// Publish to kafka - took 38 seconds
producerDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("topic", kafkaTopic)
  .save