# Databricks notebook source
# MAGIC %md
# MAGIC # Azure SQL database - batch read/write primer
# MAGIC In this exercise, we will:<br>
# MAGIC 1.  **WRITE primer**: Read curated Chicago crimes data in a Delta table, from the lab on Azure Storage, and insert into Azure SQL Database<br>
# MAGIC 2.  **READ primer**:  Read from Azure SQL Database<br>
# MAGIC <br>
# MAGIC **Dependency:** <br>
# MAGIC Successful completion of the primer lab on Azure Blob Storage<br>
# MAGIC 
# MAGIC **Docs:**<br>
# MAGIC https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Review of source data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CRIMES_DB.CHICAGO_CRIMES_CURATED where primary_type='BATTERY';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from CRIMES_DB.CHICAGO_CRIMES_CURATED where primary_type='BATTERY' and description = 'SIMPLE';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Credentials

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Driver class

# COMMAND ----------

driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Connectivity parameters

# COMMAND ----------

jdbcHostname = "gws-server.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "gws_sql_db"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

#// Create a Properties() object to hold the parameters.
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : driverClass
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Persist to Azure SQL Database

# COMMAND ----------

# MAGIC %md
# MAGIC ### (1) Create table and then write to table...<br>
# MAGIC Looks great!!<br>
# MAGIC Problem: The datatypes wont exactly be right, not enough control<br>
# MAGIC Better: Create table ahead of time and look at (2)

# COMMAND ----------

spark.sql("select * from CRIMES_DB.CHICAGO_CRIMES_CURATED where primary_type='BATTERY' and description = 'SIMPLE'").write \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("dbtable", "chicago_crimes_curated") \
  .option("user", jdbcUsername) \
  .option("password", jdbcPassword) \
  .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### (2) Append to table
# MAGIC Awesome..but...I want to append<br>

# COMMAND ----------

spark.sql("select * from crimes_db.chicago_crimes_curated where primary_type='BATTERY' and description <> 'SIMPLE'").write \
    .format("jdbc") \
    .mode("append")  \
    .option("url", jdbcUrl) \
    .option("dbtable", "chicago_crimes_curated") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .save()

# COMMAND ----------

# MAGIC %md ### (3) Overwrite table<br>
# MAGIC VERY COOL..but...I want to overwrite!!<br>

# COMMAND ----------

spark.sql("select * from crimes_db.chicago_crimes_curated where primary_type='BATTERY' and description = 'SIMPLE'").write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "chicago_crimes_curated") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md ### (4) Parallelize writes<br>
# MAGIC SOLD..but...I want to parallelize!!<br>

# COMMAND ----------

# Repartition & persist in append/overwrite mode per your requirement
df = spark.sql("select * from CRIMES_DB.CHICAGO_CRIMES_CURATED")
df.repartition(200000).write.mode("overwrite").format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "chicago_crimes_curated") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword).save()

# COMMAND ----------

# MAGIC %md ### (5) Predicate pushdown

# COMMAND ----------

#// Note: The parentheses are required.
pushdown_query = "(select * from chicago_crimes_curated where primary_type ='BATTERY') batteries"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# MAGIC %md ### (6) Spark SQL

# COMMAND ----------

# MAGIC %md https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html