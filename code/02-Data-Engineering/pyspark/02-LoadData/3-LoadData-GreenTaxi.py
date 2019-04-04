# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We will run the common functions notebook so we can reuse capability defined there, and then...<BR>
# MAGIC 1) Load green taxi data from the staging zone, homogenize schemas over the years, and persist to the raw information zone, in Delta format<BR> 
# MAGIC 2) Create external table definition<BR>
# MAGIC 3) Optimize the table                      

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType
from pyspark.sql.functions import *

# COMMAND ----------

#Source, destination directories
srcDataDirRoot = "/mnt/workshop/staging/transactional-data/" #Root dir for source data
destDataDirRoot = "/mnt/workshop/raw/nyctaxi/transactions/green-taxi" #Root dir for raw data in Parquet

#Canonical ordered column list for green taxi across years to homogenize schema
canonicalTripSchemaColList = ["taxi_type","vendor_id","pickup_datetime","dropoff_datetime","store_and_fwd_flag","rate_code_id","pickup_location_id","dropoff_location_id","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","ehail_fee","improvement_surcharge","total_amount","payment_type","trip_type","trip_year","trip_month"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Execute notebook with common/reusable functions 

# COMMAND ----------

# MAGIC %run "../01-General/2-CommonFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Define schema for source data
# MAGIC Different years have different schemas - fields added/removed

# COMMAND ----------

#Schema for data based on year and month

#2017
greenTripSchema2017H1 = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True)])

#Second half of 2016
greenTripSchema2016H2 = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("junk1", StringType(), True),
    StructField("junk2", StringType(), True)])

#2015 second half of the year and 2016 first half of the year
greenTripSchema2015H22016H1 = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True)])

#2015 first half of the year
greenTripSchema2015H1 = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("junk1", StringType(), True),
    StructField("junk2", StringType(), True)])

#August 2013 through 2014
greenTripSchemaPre2015 = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("junk1", StringType(), True),
    StructField("junk2", StringType(), True)])


# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Some functions

# COMMAND ----------

#1) Function to determine schema for a given year and month
#Input:  Year and month
#Output: StructType for applicable schema 
#Sample call: println(getSchemaStruct(2009,1))

def getTaxiSchema(tripYear, tripMonth):
  if((tripYear == 2013 and tripMonth > 7) or tripYear == 2014):
    taxiSchema = greenTripSchemaPre2015
  elif(tripYear == 2015 and tripMonth < 7):
    taxiSchema = greenTripSchema2015H1
  elif((tripYear == 2015 and tripMonth > 6) or (tripYear == 2016 and tripMonth < 7)):
    taxiSchema = greenTripSchema2015H22016H1
  elif(tripYear == 2016 and tripMonth > 6):
    taxiSchema = greenTripSchema2016H2
  elif(tripYear == 2017 and tripMonth < 7):
    taxiSchema = greenTripSchema2017H1
  
  return taxiSchema


# COMMAND ----------

#2) Function to add columns to dataframe as required to homogenize schema
#Input:  Dataframe, year and month
#Output: Dataframe with homogenized schema 
#Sample call: println(getSchemaHomogenizedDataframe(DF,2014,6))

def getSchemaHomogenizedDataframe(sourceDF,tripYear,tripMonth):
  if((tripYear == 2013 and tripMonth > 7) or tripYear == 2014):

    sourceDF = (sourceDF.withColumn("pickup_location_id", lit(0).cast("integer"))
              .withColumn("dropoff_location_id", lit(0).cast("integer"))
              .withColumn("improvement_surcharge",lit(0).cast("double"))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("green"))
              .withColumn("temp_pickup_longitude", col("pickup_longitude").cast("string"))
                                      .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
              .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast("string"))
                                      .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
              .withColumn("temp_pickup_latitude", col("pickup_latitude").cast("string"))
                                      .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
              .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast("string"))
                                      .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude"))

  elif(tripYear == 2015 and tripMonth < 7):

    sourceDF = (sourceDF.withColumn("pickup_location_id", lit(0).cast("integer"))
              .withColumn("dropoff_location_id", lit(0).cast("integer"))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("green"))
              .withColumn("temp_pickup_longitude", col("pickup_longitude").cast("string"))
                                      .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
              .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast("string"))
                                      .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
              .withColumn("temp_pickup_latitude", col("pickup_latitude").cast("string"))
                                      .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
              .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast("string"))
                                      .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude"))

  elif((tripYear == 2015 and tripMonth > 6) or (tripYear == 2016 and tripMonth < 7)):

    sourceDF = (sourceDF.withColumn("pickup_location_id", lit(0).cast("integer"))
              .withColumn("dropoff_location_id", lit(0).cast("integer"))
              .withColumn("junk1",lit(""))
              .withColumn("junk2",lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("green"))
              .withColumn("temp_pickup_longitude", col("pickup_longitude").cast("string"))
                                      .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
              .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast("string"))
                                      .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
              .withColumn("temp_pickup_latitude", col("pickup_latitude").cast("string"))
                                      .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
              .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast("string"))
                                      .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude"))

  elif(tripYear == 2016 and tripMonth > 6):

    sourceDF = (sourceDF.withColumn("pickup_longitude", lit(""))
              .withColumn("pickup_latitude", lit(""))
              .withColumn("dropoff_longitude", lit(""))
              .withColumn("dropoff_latitude", lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("green")))

  elif(tripYear == 2017 and tripMonth < 7):

    sourceDF = (sourceDF.withColumn("pickup_longitude", lit(""))
              .withColumn("pickup_latitude", lit(""))
              .withColumn("dropoff_longitude", lit(""))
              .withColumn("dropoff_latitude", lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("green"))
              .withColumn("junk1",lit(""))
              .withColumn("junk2",lit("")))

  else:
    sourceDF
  return sourceDF


# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Read CSV, homogenize schema across years, save as parquet

# COMMAND ----------

#Delete any residual data from prior executions for an idempotent run
dbutils.fs.rm(destDataDirRoot,recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS green_taxi_trips_raw;

# COMMAND ----------


#Green taxi data starts from 2013/08
for j in range(2016,2018):
    startMonth = None
    if j==2013: 
      startMonth=8 
    else: 
      startMonth=1

    endMonth = None
    if j==2017: 
      endMonth=6
    else: 
      endMonth=12

    for i in range(startMonth,endMonth+1): 
      #Source path  
      srcDataFile= "{}year={}/month={:02d}/type=green/green_tripdata_{}-{:02d}.csv".format(srcDataDirRoot,j,i,j,i)
      print ("Year={}; Month={}".format(j,i))
      print (srcDataFile)


      #Destination path  
      destDataDir = "{}/trip_year={}/trip_month={:02d}/".format(destDataDirRoot,j,i)
      
      #Source schema
      taxiSchema = getTaxiSchema(j,i)

      #Read source data
      taxiDF = (sqlContext.read.format("csv")
                      .option("header", "true")
                      .schema(taxiSchema)
                      .option("delimiter",",")
                      .load(srcDataFile).cache())

      #Add additional columns to homogenize schema across years
      taxiFormattedDF = getSchemaHomogenizedDataframe(taxiDF, j, i)

      #Order all columns to align with the canonical schema for green taxi
      taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList)

      #To make Hive Parquet format compatible with Spark Parquet format
      sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

      #Write parquet output, calling function to calculate number of partition files
      taxiCanonicalDF.coalesce(6).write.format("delta").mode("append").partitionBy("trip_year","trip_month").save(destDataDirRoot) 


# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Create external table definition

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS green_taxi_trips_raw;
# MAGIC CREATE TABLE IF NOT EXISTS green_taxi_trips_raw
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/workshop/raw/nyctaxi/transactions/green-taxi/';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from taxi_db.green_taxi_trips_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.green_taxi_trips_raw;