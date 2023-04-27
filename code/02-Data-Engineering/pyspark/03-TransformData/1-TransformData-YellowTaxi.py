# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC 
# MAGIC 1) Read raw data, augment with derived attributes, augment with reference data & persist<BR> 
# MAGIC 2) Create external unmanaged Hive tables<BR>
# MAGIC 3) Create statistics for tables                          

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Execute notebook with common/reusable functions 

# COMMAND ----------

# MAGIC %run "../01-General/2-CommonFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Read raw, augment, persist as parquet 

# COMMAND ----------

curatedDF = sql("""
  select distinct t.taxi_type,
      t.vendor_id as vendor_id,
      t.pickup_datetime,
      t.dropoff_datetime,
      t.store_and_fwd_flag,
      t.rate_code_id,
      t.pickup_location_id,
      t.dropoff_location_id,
      t.pickup_longitude,
      t.pickup_latitude,
      t.dropoff_longitude,
      t.dropoff_latitude,
      t.passenger_count,
      t.trip_distance,
      t.fare_amount,
      t.extra,
      t.mta_tax,
      t.tip_amount,
      t.tolls_amount,
      t.improvement_surcharge,
      t.total_amount,
      t.payment_type,
      t.trip_year,
      t.trip_month,
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tm.month_name_short,
      tm.month_name_full,
      pt.description as payment_type_description,
      rc.description as rate_code_description,
      tzpu.borough as pickup_borough,
      tzpu.zone as pickup_zone,
      tzpu.service_zone as pickup_service_zone,
      tzdo.borough as dropoff_borough,
      tzdo.zone as dropoff_zone,
      tzdo.service_zone as dropoff_service_zone,
      year(t.pickup_datetime) as pickup_year,
      month(t.pickup_datetime) as pickup_month,
      day(t.pickup_datetime) as pickup_day,
      hour(t.pickup_datetime) as pickup_hour,
      minute(t.pickup_datetime) as pickup_minute,
      second(t.pickup_datetime) as pickup_second,
      date(t.pickup_datetime) as pickup_date,
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second,
      date(t.dropoff_datetime) as dropoff_date
  from 
    taxi_db.yellow_taxi_trips_raw t
    left outer join nyctaxi_reference_data.vendor_lookup v 
      on (t.vendor_id = case when t.trip_year < "2015" then v.abbreviation else v.vendor_id end)
    left outer join nyctaxi_reference_data.trip_month_lookup tm 
      on (t.trip_month = tm.trip_month)
    left outer join nyctaxi_reference_data.payment_type_lookup pt 
      on (t.payment_type = case when t.trip_year < "2015" then pt.abbreviation else pt.payment_type end)
    left outer join nyctaxi_reference_data.rate_code_lookup rc 
      on (t.rate_code_id = rc.rate_code_id)
    left outer join nyctaxi_reference_data.taxi_zone_lookup tzpu 
      on (t.pickup_location_id = tzpu.location_id)
    left outer join nyctaxi_reference_data.taxi_zone_lookup tzdo 
      on (t.dropoff_location_id = tzdo.location_id)
  """)

curatedDFConformed = (curatedDF.withColumn("temp_vendor_id", col("vendor_id").cast("integer")).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id").withColumn("temp_payment_type", col("payment_type").cast("integer")).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type"))

#Save as parquet, partition by year and month
#curatedDFConformed.coalesce(15).write.partitionBy("trip_year", "trip_month").parquet(destDataDirRoot)

# COMMAND ----------

#Destination directory
destDataDirRoot = "/mnt/workshop/curated/nyctaxi/transactions/yellow-taxi" 

#Delete any residual data from prior executions for an idempotent run
dbutils.fs.rm(destDataDirRoot,recurse=True)

# COMMAND ----------

#Save as Delta, partition by year and month
curatedDFConformed.coalesce(10).write.format("delta").mode("append").partitionBy("trip_year","trip_month").save(destDataDirRoot)   

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.  Define external table

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxi_db;
# MAGIC DROP TABLE IF EXISTS yellow_taxi_trips_curated;
# MAGIC CREATE TABLE yellow_taxi_trips_curated
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/transactions/yellow-taxi';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.  Explore

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as trip_count from taxi_db.yellow_taxi_trips_curated 

# COMMAND ----------

# MAGIC %sql
# MAGIC select trip_year,trip_month, count(*) as trip_count from taxi_db.yellow_taxi_trips_curated group by trip_year,trip_month