# Databricks notebook source
# MAGIC %md
# MAGIC # DBFS - read/write primer
# MAGIC In this exercise, we will:<br>
# MAGIC 1.  **Download** and curate Chicago crimes public dataset  - 1.5 GB of the Chicago crimes public dataset - has 6.7 million records.<BR>
# MAGIC 2.  **Upload the dataset to DBFS**, to the staging directory in DBFS<BR>
# MAGIC 3.  Read the CSV into a dataframe, **persist as parquet** to the raw directory<BR>
# MAGIC 4.  **Create an external table** on top of the dataset in the raw directory<BR>
# MAGIC 5.  **Explore with SQL construct**<BR>
# MAGIC 6.  **Curate** the dataset (dedupe, add additional dervived attributes of value etc) for subsequent labs<BR>
# MAGIC 7.  Do some basic **visualization**<BR>
# MAGIC   
# MAGIC Chicago crimes dataset:<br>
# MAGIC Website: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2<br>
# MAGIC Dataset: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD<br>
# MAGIC Metadata: https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf<br>
# MAGIC   
# MAGIC Referenes for Databricks:<br>
# MAGIC Working with blob storage: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html <br>
# MAGIC Visualization: https://docs.databricks.com/user-guide/visualizations/charts-and-graphs-scala.html
# MAGIC   

# COMMAND ----------

# MAGIC %sh
# MAGIC # 1) Download dataset - gets downloaded to driver
# MAGIC wget "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"

# COMMAND ----------

# MAGIC %sh
# MAGIC # 2) Rename file
# MAGIC mv "rows.csv?accessType=DOWNLOAD" chicago-crimes.csv

# COMMAND ----------

from libs.dbname import dbname
from libs.tblname import tblname, username
uname = username()

# 3) List to validate if file exists
display(dbutils.fs.ls("file:/databricks/driver/chicago-crimes.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Upload from driver node to DBFS

# COMMAND ----------

# 1) Create destination directory
dbfs_dir_path = f"/mnt/workshop/users/{uname}/staging/crimes/chicago-crimes"
dbutils.fs.rm(dbfs_dir_path, recurse=True)
dbutils.fs.mkdirs(dbfs_dir_path)

# COMMAND ----------

# 2) Upload to from localDirPath to dbfs_dir_path
dbutils.fs.cp("file:/databricks/driver/chicago-crimes.csv", dbfs_dir_path, recurse=True)

# 3) Clean up local directory
# dbutils.fs.rm(localFile)

# 4) List dbfs_dir_path
display(dbutils.fs.ls(dbfs_dir_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read raw CSV, persist to parquet

# COMMAND ----------

# 1) Source directory
dbfs_src_dir_path = f"/mnt/workshop/users/{uname}/staging/crimes/chicago-crimes"

# 2) Destination directory
dbfs_dest_dir_path_raw = f"/mnt/workshop/users/{uname}/raw/crimes/chicago-crimes"

# COMMAND ----------

# 3) Check first few lines
dbutils.fs.head(dbfs_src_dir_path + "/chicago-crimes.csv")

# COMMAND ----------

# 4)  Read raw CSV
sourceDF = spark.read.format("csv").options(header='true', delimiter = ',').load(dbfs_src_dir_path).toDF("case_id", "case_nbr", "case_dt_tm", "block", "iucr", "primary_type", "description", "location_description", "arrest_made", "was_domestic", "beat", "district", "ward", "community_area", "fbi_code", "x_coordinate", "y_coordinate", "case_year", "updated_dt", "latitude", "longitude", "location_coords")

sourceDF.printSchema()
display(sourceDF)

# COMMAND ----------

# 5) Persist as parquet to raw zone
dbutils.fs.rm(dbfs_dest_dir_path_raw, recurse=True)
sourceDF.coalesce(2).write.parquet(dbfs_dest_dir_path_raw)

# COMMAND ----------

display(dbutils.fs.ls(dbfs_dest_dir_path_raw))




# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Define external table

# COMMAND ----------
crime = dblname(db="crime")
print("crime:" + repr(crime))
spark.conf.set("nbvars.crime", crime)
chicago_crimes_raw = tblname(db="crime", tbl="chicago_crimes_raw")
print("chicago_crimes_raw:" + repr(chicago_crimes_raw))
spark.conf.set("nbvars.chicago_crimes_raw", chicago_crimes_raw)
chicago_crimes_curated = tblname(db="crime", tbl="chicago_crimes_curated")
print("chicago_crimes_curated:" + repr(chicago_crimes_curated))
spark.conf.set("nbvars.chicago_crimes_curated", chicago_crimes_curated)
spark.conf.set("nbvars.dbfs_dest_dir_path_raw", dbfs_dest_dir_path_raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.crime};
# MAGIC
# MAGIC USE ${nbvars.crime};
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${nbvars.chicago_crimes_raw};
# MAGIC CREATE TABLE IF NOT EXISTS ${nbvars.chicago_crimes_raw}
# MAGIC USING parquet
# MAGIC OPTIONS (path "${nbvars.dbfs_dest_dir_path_raw}");
# MAGIC
# MAGIC ANALYZE TABLE ${nbvars.chicago_crimes_raw} COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Explore the raw dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC USE crimes_db;
# MAGIC --SELECT * FROM ${nbvars.chicago_crimes_raw};
# MAGIC SELECT count(*) FROM ${nbvars.chicago_crimes_raw};
# MAGIC
# MAGIC --6,701,049

# COMMAND ----------

# MAGIC  %md
# MAGIC  ### 6. Curate the dataset
# MAGIC  In this section, we will just parse the date and time for the purpose of analytics.

# COMMAND ----------

# 1) Read and curate
# Lets add some temporal attributes that can help us analyze trends over time

#from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType, DecimalType
#from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, udf

def get_day_name_from_weekday_nbr(weekday):
    if weekday == 0:
        return "Monday"
    if weekday == 1:
        return "Tuesday"
    if weekday == 2:
        return "Wednesday"
    if weekday == 3:
        return "Thursday"
    if weekday == 4:
        return "Friday"
    if weekday == 5:
        return "Saturday"
    if weekday == 6:
        return "Sunday"

udf_get_day_name_from_weekday_nbr = udf(get_day_name_from_weekday_nbr, StringType())

# Temp view names are local to notebooks
spark.sql(f"select * from {chicago_crimes_raw}").withColumn(
    "case_timestamp",
    to_timestamp("case_dt_tm","MM/dd/yyyy hh:mm:ss")).createOrReplaceTempView("raw_crimes")
curated_initial_df = spark.sql("select *, month(case_timestamp) as case_month,dayofmonth(case_timestamp) as case_day_of_month, hour(case_timestamp) as case_hour, dayofweek(case_timestamp) as case_day_of_week_nbr from raw_crimes")
curated_df = curated_initial_df.withColumn("case_day_of_week_name",
                                           udf_get_day_name_from_weekday_nbr("case_day_of_week_nbr"))

display(curated_df)


# COMMAND ----------

# 2) Persist as parquet to curated storage zone
dbfs_dest_dir_path_curated = f"/mnt/workshop/users/{uname}/curated/crimes/chicago-crimes"
dbutils.fs.rm(dbfs_dest_dir_path_curated, recurse=True)
curated_df.coalesce(1).write.partitionBy("case_year","case_month").parquet(dbfs_dest_dir_path_curated)

spark.conf.set("nbvars.dbfs_dest_dir_path_curated", dbfs_dest_dir_path_curated)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.crime};
# MAGIC
# MAGIC USE ${nbvars.crime};
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${nbvars.chicago_crimes_curated};
# MAGIC CREATE TABLE ${nbvars.chicago_crimes_curated}
# MAGIC USING parquet
# MAGIC OPTIONS (path "${nbvars.dbfs_dest_dir_path_curated}");
# MAGIC
# MAGIC MSCK REPAIR TABLE ${nbvars.chicago_crimes_curated};
# MAGIC ANALYZE TABLE ${nbvars.chicago_crimes_curated} COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted ${nbvars.chicago_crimes_curated};

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${nbvars.chicago_crimes_curated};
# MAGIC --select count(*) as crime_count from ${nbvars.chicago_crimes_curated} --where primary_type='THEFT';

# COMMAND ----------

# MAGIC  %md
# MAGIC  ### 7. Report on the dataset/visualize
# MAGIC  In this section, we will explore data and visualize

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT case_year, count(*) AS crime_count FROM ${nbvars.chicago_crimes_curated}
# MAGIC GROUP BY case_year ORDER BY case_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(cast(case_year as string) as date) as case_year, primary_type as case_type, count(*) AS crime_count
# MAGIC FROM ${nbvars.chicago_crimes_curated}
# MAGIC where primary_type in ('BATTERY','ASSAULT','CRIMINAL SEXUAL ASSAULT')
# MAGIC GROUP BY case_year,primary_type ORDER BY case_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC select case_year,primary_type as case_type, count(*) as crimes_count
# MAGIC from ${nbvars.chicago_crimes_curated}
# MAGIC where (primary_type LIKE '%ASSAULT%' OR primary_type LIKE '%CHILD%')
# MAGIC GROUP BY case_year, case_type
# MAGIC ORDER BY case_year,case_type desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select primary_type as case_type, count(*) as crimes_count
# MAGIC from ${nbvars.chicago_crimes_curated}
# MAGIC where (primary_type LIKE '%ASSAULT%' OR primary_type LIKE '%CHILD%') OR (primary_type='KIDNAPPING')
# MAGIC GROUP BY case_type;
