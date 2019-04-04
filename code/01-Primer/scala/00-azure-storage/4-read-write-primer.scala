// Databricks notebook source
// MAGIC %md
// MAGIC # DBFS - read/write primer
// MAGIC In this exercise, we will:<br>
// MAGIC 1.  **Download** and curate Chicago crimes public dataset  - 1.5 GB of the Chicago crimes public dataset - has 6.7 million records.<BR>
// MAGIC 2.  **Upload the dataset to DBFS**, to the staging directory in DBFS<BR>
// MAGIC 3.  Read the CSV into a dataframe, **persist as parquet** to the raw directory<BR>
// MAGIC 4.  **Create an external table** on top of the dataset in the raw directory<BR>
// MAGIC 5.  **Explore with SQL construct**<BR>
// MAGIC 6.  **Curate** the dataset (dedupe, add additional dervived attributes of value etc) for subsequent labs<BR>
// MAGIC 7.  Do some basic **visualization**<BR>
// MAGIC   
// MAGIC Chicago crimes dataset:<br>
// MAGIC Website: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2<br>
// MAGIC Dataset: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD<br>
// MAGIC Metadata: https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf<br>
// MAGIC   
// MAGIC Referenes for Databricks:<br>
// MAGIC Working with blob storage: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html <br>
// MAGIC Visualization: https://docs.databricks.com/user-guide/visualizations/charts-and-graphs-scala.html
// MAGIC   

// COMMAND ----------

import org.apache.spark.{SparkConf, SparkContext}
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

// COMMAND ----------

//1) Local directory to download to
val localDirPath="/tmp/downloads/chicago-crimes-data/"
dbutils.fs.rm(localDirPath, recurse=true)
dbutils.fs.mkdirs(localDirPath)

// COMMAND ----------

//2) Download to driver local file system
//Download of 1.58 GB of data - took the author ~14 minutes
import sys.process._
import scala.sys.process.ProcessLogger

val wgetToExec = "wget -P " + localDirPath + " https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"
println(wgetToExec)
wgetToExec !!

// COMMAND ----------

// MAGIC %fs
// MAGIC mv file:/tmp/downloads/chicago-crimes-data/rows.csv?accessType=DOWNLOAD file:/tmp/downloads/chicago-crimes-data/chicago-crimes.csv

// COMMAND ----------

//3) Filename
val localFile="file:/tmp/downloads/chicago-crimes-data/chicago-crimes.csv"

//4) List the download
display(dbutils.fs.ls(localFile))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Upload from driver node to DBFS

// COMMAND ----------

//1) Create destination directory
val dbfsDirPath="/mnt/workshop/staging/crimes/chicago-crimes"
dbutils.fs.rm(dbfsDirPath, recurse=true)
dbutils.fs.mkdirs(dbfsDirPath)

// COMMAND ----------

//2) Upload to from localDirPath to dbfsDirPath
dbutils.fs.cp(localFile, dbfsDirPath, recurse=true)

//3) Clean up local directory
//dbutils.fs.rm(localFile)

//4) List dbfsDirPath
display(dbutils.fs.ls(dbfsDirPath))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Read raw CSV, persist to parquet

// COMMAND ----------

//1) Source directory
val dbfsSrcDirPath="/mnt/workshop/staging/crimes/chicago-crimes"

//2) Destination directory
val dbfsDestDirPath="/mnt/workshop/raw/crimes/chicago-crimes"

// COMMAND ----------

//3) Check first few lines
dbutils.fs.head(dbfsSrcDirPath + "/chicago-crimes.csv")

// COMMAND ----------

//4)  Read raw CSV
val sourceDF = spark.read.format("csv")
  .option("header", "true").load(dbfsSrcDirPath).toDF("case_id", "case_nbr", "case_dt_tm", "block", "iucr", "primary_type", "description", "location_description", "arrest_made", "was_domestic", "beat", "district", "ward", "community_area", "fbi_code", "x_coordinate", "y_coordinate", "case_year", "updated_dt", "latitude", "longitude", "location_coords")

sourceDF.printSchema
sourceDF.show

// COMMAND ----------

//5) Persist as parquet to raw zone
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
sourceDF.coalesce(2).write.parquet(dbfsDestDirPath)

//6) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(dbfsDestDirPath + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

display(dbutils.fs.ls(dbfsDestDirPath))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Define external table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS CRIMES_DB;
// MAGIC 
// MAGIC USE CRIMES_DB;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_raw;
// MAGIC CREATE TABLE IF NOT EXISTS chicago_crimes_raw
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/raw/crimes/chicago-crimes");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE chicago_crimes_raw COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Explore the raw dataset

// COMMAND ----------

// MAGIC %sql
// MAGIC USE crimes_db;
// MAGIC --SELECT * FROM chicago_crimes_raw;
// MAGIC SELECT count(*) FROM chicago_crimes_raw;
// MAGIC 
// MAGIC --6,701,049

// COMMAND ----------

// MAGIC  %md
// MAGIC  ### 6. Curate the dataset
// MAGIC  In this section, we will just parse the date and time for the purpose of analytics.

// COMMAND ----------

// 1) Read and curate
// Lets add some temporal attributes that can help us analyze trends over time

import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType, DecimalType}

val to_timestamp_func = to_timestamp($"case_dt_tm", "MM/dd/yyyy hh:mm:ss")

val rawDF = spark.sql("select * from crimes_db.chicago_crimes_raw")
val curatedDF = rawDF.withColumn("case_timestamp",to_timestamp_func)
                      .withColumn("case_month", month(col("case_timestamp")))
                      .withColumn("case_day_of_month", dayofmonth(col("case_timestamp")))
                      .withColumn("case_hour", hour(col("case_timestamp")))
                      .withColumn("case_day_of_week_nbr", dayofweek(col("case_timestamp")))
                      .withColumn("case_day_of_week_name", when(col("case_day_of_week_nbr") === lit(1), "Sunday")
                                                          .when(col("case_day_of_week_nbr") === lit(2), "Monday")
                                                          .when(col("case_day_of_week_nbr") === lit(3), "Tuesday")
                                                          .when(col("case_day_of_week_nbr") === lit(4), "Wednesday")
                                                          .when(col("case_day_of_week_nbr") === lit(5), "Thursday")
                                                          .when(col("case_day_of_week_nbr") === lit(6), "Friday")
                                                          .when(col("case_day_of_week_nbr") === lit(7), "Sunday")
                                 )
                      .withColumn("latitude_dec", col("latitude").cast(DecimalType(10,7)))
                      .withColumn("longitude_dec", col("longitude").cast(DecimalType(10,7)))
                      
                                                          
curatedDF.printSchema
curatedDF.show  

// COMMAND ----------

//2) Persist as parquet to curated storage zone
val dbfsDestDirPath="/mnt/workshop/curated/crimes/chicago-crimes"
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
curatedDF.coalesce(1).write.partitionBy("case_year","case_month").parquet(dbfsDestDirPath)

// COMMAND ----------

//3) List
display(dbutils.fs.ls(dbfsDestDirPath))

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS CRIMES_DB;
// MAGIC 
// MAGIC USE CRIMES_DB;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_curated;
// MAGIC CREATE TABLE chicago_crimes_curated
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/curated/crimes/chicago-crimes");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC MSCK REPAIR TABLE chicago_crimes_curated;
// MAGIC ANALYZE TABLE chicago_crimes_curated COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted crimes_db.chicago_crimes_curated;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from crimes_db.chicago_crimes_curated;
// MAGIC --select count(*) as crime_count from crimes_db.chicago_crimes_curated --where primary_type='THEFT';

// COMMAND ----------

// MAGIC  %md
// MAGIC  ### 7. Report on the dataset/visualize
// MAGIC  In this section, we will explore data and visualize

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from crimes_db.chicago_crimes_curated;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT case_year, count(*) AS crime_count FROM crimes_db.chicago_crimes_curated 
// MAGIC GROUP BY case_year ORDER BY case_year;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cast(cast(case_year as string) as date) as case_year, primary_type as case_type, count(*) AS crime_count 
// MAGIC FROM crimes_db.chicago_crimes_curated 
// MAGIC where primary_type in ('BATTERY','ASSAULT','CRIMINAL SEXUAL ASSAULT')
// MAGIC GROUP BY case_year,primary_type ORDER BY case_year;

// COMMAND ----------

// MAGIC %sql
// MAGIC select case_year,primary_type as case_type, count(*) as crimes_count 
// MAGIC from crimes_db.chicago_crimes_curated 
// MAGIC where (primary_type LIKE '%ASSAULT%' OR primary_type LIKE '%CHILD%') 
// MAGIC GROUP BY case_year, case_type 
// MAGIC ORDER BY case_year,case_type desc; 

// COMMAND ----------

// MAGIC %sql
// MAGIC select primary_type as case_type, count(*) as crimes_count 
// MAGIC from crimes_db.chicago_crimes_curated 
// MAGIC where (primary_type LIKE '%ASSAULT%' OR primary_type LIKE '%CHILD%') OR (primary_type='KIDNAPPING') 
// MAGIC GROUP BY case_type; 