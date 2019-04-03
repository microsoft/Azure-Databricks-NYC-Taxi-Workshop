// Databricks notebook source
// MAGIC %md
// MAGIC # Azure SQL database - batch read/write primer
// MAGIC In this exercise, we will:<br>
// MAGIC 1.  **WRITE primer**: Read curated Chicago crimes data in a Delta table, from the lab on Azure Storage, and insert into Azure SQL Database<br>
// MAGIC 2.  **READ primer**:  Read from Azure SQL Database<br>
// MAGIC <br>
// MAGIC **Dependency:** <br>
// MAGIC Successful completion of the primer lab on Azure Blob Storage<br>
// MAGIC 
// MAGIC **Docs:**<br>
// MAGIC https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Review of source data

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from CRIMES_DB.CHICAGO_CRIMES_CURATED;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from CRIMES_DB.CHICAGO_CRIMES_CURATED;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Credentials

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Driver class

// COMMAND ----------

//Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Connectivity parameters

// COMMAND ----------

val jdbcHostname = "gws-server.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "gws_sql_db"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Persist to Azure SQL Database

// COMMAND ----------

// MAGIC %md
// MAGIC ### (1) Create table and then write to table...<br>
// MAGIC Looks great!!<br>
// MAGIC Problem: The datatypes wont exactly be right, not enough control<br>
// MAGIC Better: Create table ahead of time and look at (2)

// COMMAND ----------

spark.table("crimes_db.chicago_crimes_curated")
     .write
     .jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### (2) Append to table
// MAGIC Awesome..but...I want to append<br>

// COMMAND ----------

import org.apache.spark.sql.SaveMode

spark.table("crimes_db.chicago_crimes_curated")
     .write
     .mode(SaveMode.Append) // <--- Append to the existing table
     .jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties)

// COMMAND ----------

// MAGIC %md ### (3) Overwrite table<br>
// MAGIC VERY COOL..but...I want to overwrite!!<br>

// COMMAND ----------

import org.apache.spark.sql.SaveMode

spark.table("crimes_db.chicago_crimes_curated")
     .write
     .mode(SaveMode.Overwrite) // <--- Overwrite the existing table
     .jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties)

// COMMAND ----------

// MAGIC %md ### (4) Parallelize writes<br>
// MAGIC SOLD..but...I want to parallelize!!<br>

// COMMAND ----------

//Repartition & persist in append/overwrite mode per your requirement
val df = spark.sql("select * from CRIMES_DB.CHICAGO_CRIMES_CURATED")
df.repartition(200000).write.mode(SaveMode.Append).jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties)

// COMMAND ----------

// MAGIC %md ### (5) Predicate pushdown

// COMMAND ----------

// Note: The parentheses are required.
val pushdown_query = "(select * from chicago_crimes_curated where primary_type ='KIDNAPPING') kidnappings"
val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

// COMMAND ----------

// MAGIC %md ### (6) Explain plan

// COMMAND ----------

// Explain plan with no column selection returns all columns
spark.read.jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties).explain(true)

// COMMAND ----------

// Explain plan with column selection will prune columns and just return the ones specified
// Notice that only the 3 specified columns are in the explain plan
spark.read.jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties).select("case_id", "case_year", "primary_type").explain(true)

// COMMAND ----------

// You can push query predicates down too
// Notice the Filter at the top of the Physical Plan
spark.read.jdbc(jdbcUrl, "chicago_crimes_curated", connectionProperties).select("case_id", "case_year", "primary_type").where("primary_type = 'KIDNAPPING'").explain(true)

// COMMAND ----------

// MAGIC %md ### (7) Parallelize reads
// MAGIC These options specify the parallelism of the table read. lowerBound and upperBound decide the partition stride, but do not filter the rows in table. Therefore, Spark partitions and returns all rows in the table.
// MAGIC <br>We split the table read across executors on the emp_no column using the partitionColumn, lowerBound, upperBound, and numPartitions parameters.

// COMMAND ----------

val df = (spark.read.jdbc(url=jdbcUrl,
    table="chicago_crimes_curated",
    columnName="case_id",
    lowerBound=1L,
    upperBound=1000,
    numPartitions=100,
    connectionProperties=connectionProperties))
display(df)

// COMMAND ----------

// MAGIC %md ### (8) Spark SQL

// COMMAND ----------

// MAGIC %md https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html