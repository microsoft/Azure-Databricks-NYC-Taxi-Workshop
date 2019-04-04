# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Cosmos DB (core API - SQL/document-oriented) - batch read/write primer
# MAGIC In this exercise, we will:<br>
# MAGIC 1.  **WRITE primer**: Read curated Chicago crimes data in a table, from the lab on Azure Storage, and insert into Azure Cosmos DB<br>
# MAGIC 2.  **READ primer**:  Read from Azure Cosmos DBe<br>
# MAGIC 
# MAGIC 
# MAGIC **Dependency:** <br>
# MAGIC Successful completion of the primer lab on Azure Blob Storage<br>
# MAGIC 
# MAGIC **Docs:**<br>
# MAGIC Databricks - Azure Cosmos DB: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html<br>
# MAGIC Azure Cosmos DB - Spark connector guide: https://github.com/Azure/azure-cosmosdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide<br>
# MAGIC Azre Cosmos DB - Spark connector - performance: https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0. Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Attach the compatible Azure Cosmos DB Spark connector to your cluster-<br>
# MAGIC At the time of authoring, it was the uber jar at 
# MAGIC https://search.maven.org/search?q=a:azure-cosmosdb-spark_2.3.0_2.11

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0. Credentials

# COMMAND ----------

cdbEndpoint = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-uri")
cdbAccessKey = dbutils.secrets.get(scope = "gws-cosmos-db", key = "acct-key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.0. Review source dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC select case_id, primary_type as case_type, case_year, case_month, case_day_of_month from crimes_db.chicago_crimes_curated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.0. Upsert

# COMMAND ----------

df = spark.sql("select case_id, primary_type as case_type, case_year, case_month, case_day_of_month from crimes_db.chicago_crimes_curated where case_year=2018")
df.show

# COMMAND ----------

# Write configuration
writeConfig = {
 "Endpoint" : cdbEndpoint,
 "Masterkey" : cdbAccessKey,
 "Database" : "gws_db",
 "Collection" : "chicago_crimes_curated_batch",
 "Upsert" : "true"
}

# COMMAND ----------

# Write to Cosmos DB 
df.write.format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.0. Read

# COMMAND ----------

# Read Configuration
readConfig = {
  "Endpoint" : cdbEndpoint,
  "Masterkey" : cdbAccessKey,
  "Database" : "gws_db",
  "preferredRegions" : "East US 2;",
  "Collection" : "chicago_crimes_curated_batch",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "1000",
  "query_pagesize" : "2147483647",
  "query_custom" : "SELECT * from chicago_crimes_curated_batch c"
}

# Connect via azure-cosmosdb-spark to create Spark DataFrame
df = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()
#df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that there is a "id" field created automatically.  We will review this in the next lab module.