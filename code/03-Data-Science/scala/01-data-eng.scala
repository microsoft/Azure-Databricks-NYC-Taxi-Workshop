// Databricks notebook source
// MAGIC %md 
// MAGIC #### Dataset review
// MAGIC This dataset contains detailed trip-level data on New York City Taxi trips. It was collected from both drivers inputs as well as the GPS coordinates of individual cabs. 
// MAGIC 
// MAGIC 
// MAGIC The data is in a CSV format and has the following fields:
// MAGIC 
// MAGIC 
// MAGIC * tripID: a unique identifier for each trip
// MAGIC * VendorID: a code indicating the provider associated with the trip record
// MAGIC * tpep_pickup_datetime: date and time when the meter was engaged
// MAGIC * tpep_dropoff_datetime: date and time when the meter was disengaged
// MAGIC * passenger_count: the number of passengers in the vehicle (driver entered value)
// MAGIC * trip_distance: The elapsed trip distance in miles reported by the taximeter
// MAGIC * RatecodeID: The final rate code in effect at the end of the trip -1= Standard rate -2=JFK -3=Newark -4=Nassau or Westchester -5=Negotiated fare -6=Group ride
// MAGIC * store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip
// MAGIC * PULocationID: TLC Taxi Zone in which the taximeter was engaged
// MAGIC * DOLocationID: TLC Taxi Zone in which the taximeter was disengaged
// MAGIC * payment_type: A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip
// MAGIC * fare_amount: The time-and-distance fare calculated by the meter.
// MAGIC * extra: Miscellaneous extras and surcharges
// MAGIC * mta_tax: $0.50 MTA tax that is automatically triggered based on the metered rate in use
// MAGIC * tip_amount: Tip amount –This field is automatically populated for credit card tips. Cash tips are not included
// MAGIC * tolls_amount:Total amount of all tolls paid in trip
// MAGIC * improvement_surcharge: $0.30 improvement surcharge assessed trips at the flag drop.
// MAGIC * total_amount: The total amount charged to passengers. Does not include cash tips.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1. Load 

// COMMAND ----------

// MAGIC %md ### 1.1. Load trip transactions

// COMMAND ----------

//1.  Source, destination directories
val srcDataAbsPathTrips = "/mnt/workshop/staging/model-related/yellow-trips/yellow_tripdata_2017-*.csv" 
val destDataDirRootTrips = "/mnt/workshop/raw/nyctaxi/model-transactions/trips/"

// COMMAND ----------

//2.  Load from source into dataframe
val stagedTripsDF = spark.read.option("header","true").csv(srcDataAbsPathTrips)
stagedTripsDF.count
display(stagedTripsDF)

// COMMAND ----------

//3.  Typecasting columns and renaming
//All the data is of string datatype
//Lets cast it appropriately

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val formattedDF1=stagedTripsDF.select($"*", 
                                  $"tpep_pickup_datetime".cast(TimestampType).alias("pickup_time"),
                                  $"tpep_dropoff_datetime".cast(TimestampType).alias("dropoff_time"),
                                  $"fare_amount".cast(DoubleType).alias("fare"))
                                .drop("tpep_pickup_datetime","tpep_dropoff_datetime","","fare_amount")

// COMMAND ----------

val formattedDF2=formattedDF1.select($"pickup_time",
                                     $"dropoff_time",
                                     $"dropoff_time".cast(LongType).alias("dropoff_time_long"),
                                     $"trip_distance".cast(DoubleType),
                                     $"PULocationID".cast(IntegerType).alias("pickup_locn_id"),
                                     $"DOLocationID".cast(IntegerType).alias("dropoff_locn_id"),
                                     $"RatecodeID".cast(IntegerType).alias("rate_code_id"),
                                     $"store_and_fwd_flag",
                                     $"payment_type".cast(IntegerType),
                                     $"fare",$"extra".cast(DoubleType))
.withColumn("duration",unix_timestamp($"dropoff_time")-unix_timestamp($"pickup_time"))
.withColumn("pickup_hour",hour($"pickup_time"))
.withColumn("pickup_day",dayofweek($"pickup_time"))
.withColumn("pickup_day_month",dayofmonth($"pickup_time"))
.withColumn("pickup_minute",minute($"pickup_time"))
.withColumn("pickup_weekday",weekofyear($"pickup_time"))
.withColumn("pickup_month",month($"pickup_time"))
.filter($"fare">0 || $"duration">0 || $"fare"<5000)
.drop("pickup","dropoff_time","pickup_time")

// COMMAND ----------

//4.  Dataset review 1 - display
display(formattedDF2)

// COMMAND ----------

//6.  Dataset review 3 - descriptive statistics
formattedDF2.describe().show()

// COMMAND ----------

//7.  Now that we have a decent idea, lets persist to the raw information zone as parquet - a more space and query-efficient persistence format
import org.apache.spark.sql.SaveMode
formattedDF2.coalesce(2).write.mode(SaveMode.Overwrite).save(destDataDirRootTrips)

// COMMAND ----------

//8. Display file system to check file size - ensure you have at least 128 MB files or close
display(dbutils.fs.ls(destDataDirRootTrips))

// COMMAND ----------

//9) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(destDataDirRootTrips + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC //10.  Create external hive table, compute statistics

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC USE taxi_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS model_raw_trips;
// MAGIC CREATE TABLE IF NOT EXISTS model_raw_trips
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/raw/nyctaxi/model-transactions/trips/");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE model_raw_trips COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %md //11. Review table data

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.model_raw_trips