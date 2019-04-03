# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We run the common functions notebook so we can reuse capability defined there, and then...<BR>
# MAGIC 1) Load reference data in staging directory to reference data directory<BR> 
# MAGIC 2) Create external unmanaged Hive tables<BR>
# MAGIC 3) Create statistics for tables                          

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType

# COMMAND ----------

# Define source and destination directories
srcDataDirRoot = "/mnt/workshop/staging/reference-data/" #Root dir for source data
destDataDirRoot = "/mnt/workshop/curated/nyctaxi/reference/" #Root dir for consumable data

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Execute notebook with common/reusable functions 

# COMMAND ----------

# MAGIC %run "../01-General/2-CommonFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. List reference datasets

# COMMAND ----------

display(dbutils.fs.ls(srcDataDirRoot))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Define schema for raw reference data

# COMMAND ----------

# 1.  Taxi zone lookup
taxiZoneSchema = StructType([
    StructField("location_id", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("service_zone", StringType(), True)])

#2. Months of the year
tripMonthNameSchema = StructType([
    StructField("trip_month", StringType(), True),
    StructField("month_name_short", StringType(), True),
    StructField("month_name_full", StringType(), True)])

#3.  Rate code id lookup
rateCodeSchema = StructType([
    StructField("rate_code_id", IntegerType(), True),
    StructField("description", StringType(), True)])

#4.  Payment type lookup
paymentTypeSchema = StructType([
    StructField("payment_type", IntegerType(), True),
    StructField("abbreviation", StringType(), True),
    StructField("description", StringType(), True)])

#5. Trip type
tripTypeSchema = StructType([
    StructField("trip_type", IntegerType(), True),
    StructField("description", StringType(), True)])


#6. Vendor ID
vendorSchema = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("abbreviation", StringType(), True),
    StructField("description", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Load reference data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.1. Create function to load data

# COMMAND ----------

def loadReferenceData(srcDatasetName, srcDataFile, destDataDir, srcSchema, delimiter ):
  print("Dataset:  " + srcDatasetName)
  print(".......................................................")
  
  #Execute for idempotent runs
  print("....deleting destination directory - " + str(dbutils.fs.rm(destDataDir, recurse=True)))
  
  #Read source data
  refDF = (sqlContext.read.option("header", True)
                      .schema(srcSchema)
                      .option("delimiter",delimiter)
                      .csv(srcDataFile))
      
  #Write parquet output
  print("....reading source and saving as parquet")
  refDF.coalesce(1).write.parquet(destDataDir)
  
  #Delete residual files from job operation (_SUCCESS, _start*, _committed*)
  #print "....deleting flag files"
  #dbutils.fs.ls(destDataDir + "/").foreach(lambda i: if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
  
  print("....done")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.2. Load data

# COMMAND ----------

loadReferenceData("taxi zone",srcDataDirRoot + "taxi_zone_lookup.csv",destDataDirRoot + "taxi-zone",taxiZoneSchema,",")
loadReferenceData("trip month",srcDataDirRoot + "trip_month_lookup.csv",destDataDirRoot + "trip-month",tripMonthNameSchema,",")
loadReferenceData("rate code",srcDataDirRoot + "rate_code_lookup.csv",destDataDirRoot + "rate-code",rateCodeSchema,"|")
loadReferenceData("payment type",srcDataDirRoot + "payment_type_lookup.csv",destDataDirRoot + "payment-type",paymentTypeSchema,"|")
loadReferenceData("trip type",srcDataDirRoot + "trip_type_lookup.csv",destDataDirRoot + "trip-type",tripTypeSchema,"|")
loadReferenceData("vendor",srcDataDirRoot + "vendor_lookup.csv",destDataDirRoot + "vendor",vendorSchema,"|")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.3. Validate load

# COMMAND ----------

display(dbutils.fs.ls("/mnt/workshop/curated/nyctaxi/reference"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Hive tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS taxi_zone_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS taxi_zone_lookup(
# MAGIC location_id STRING,
# MAGIC borough STRING,
# MAGIC zone STRING,
# MAGIC service_zone STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/taxi-zone/';
# MAGIC 
# MAGIC ANALYZE TABLE taxi_zone_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.taxi_zone_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS trip_month_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS trip_month_lookup(
# MAGIC trip_month STRING,
# MAGIC month_name_short STRING,
# MAGIC month_name_full STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/trip-month/';
# MAGIC 
# MAGIC ANALYZE TABLE trip_month_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.trip_month_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS rate_code_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS rate_code_lookup(
# MAGIC rate_code_id INT,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/rate-code/';
# MAGIC 
# MAGIC ANALYZE TABLE rate_code_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.rate_code_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS payment_type_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS payment_type_lookup(
# MAGIC payment_type INT,
# MAGIC abbreviation STRING,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/payment-type/';
# MAGIC 
# MAGIC ANALYZE TABLE payment_type_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.payment_type_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS trip_type_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS trip_type_lookup(
# MAGIC trip_type INT,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/trip-type/';
# MAGIC 
# MAGIC ANALYZE TABLE trip_type_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.trip_type_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS vendor_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS vendor_lookup(
# MAGIC vendor_id INT,
# MAGIC abbreviation STRING,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/vendor/';
# MAGIC 
# MAGIC ANALYZE TABLE vendor_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.vendor_lookup;