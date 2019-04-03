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

#Destination directory
destDataDirRoot =  "/mnt/workshop/curated/nyctaxi/transactions/green-taxi" 

#Delete any residual data from prior executions for an idempotent run
dbutils.fs.rm(destDataDirRoot,recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS green_taxi_trips_curated;

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
  select 
      t.taxi_type,
      t.vendor_id,
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
      t.ehail_fee,
      t.improvement_surcharge,
      t.total_amount,
      t.payment_type,
      t.trip_type,
      t.trip_year,
      t.trip_month,
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tt.description as trip_type_description,
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
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second
  from 
    taxi_db.green_taxi_trips_raw t
    left outer join taxi_db.vendor_lookup v 
      on (t.vendor_id = v.vendor_id)
    left outer join taxi_db.trip_type_lookup tt 
      on (t.trip_type = tt.trip_type)
    left outer join taxi_db.trip_month_lookup tm 
      on (t.trip_month = tm.trip_month)
    left outer join taxi_db.payment_type_lookup pt 
      on (t.payment_type = pt.payment_type)
    left outer join taxi_db.rate_code_lookup rc 
      on (t.rate_code_id = rc.rate_code_id)
    left outer join taxi_db.taxi_zone_lookup tzpu 
      on (t.pickup_location_id = tzpu.location_id)
    left join taxi_db.taxi_zone_lookup tzdo 
      on (t.dropoff_location_id = tzdo.location_id)
  """)
#Save as Delta, partition by year and month
curatedDF.coalesce(3).write.format("delta").mode("append").partitionBy("trip_year","trip_month").save(destDataDirRoot) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.  Define external table

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS green_taxi_trips_curated;
# MAGIC CREATE TABLE green_taxi_trips_curated
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/transactions/green-taxi';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.  Explore

# COMMAND ----------

# MAGIC %sql
# MAGIC select trip_year,trip_month, count(*) as trip_count from taxi_db.green_taxi_trips_curated group by trip_year,trip_month
# MAGIC order by trip_year, trip_month

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from taxi_db.green_taxi_trips_curated

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/workshop/curated