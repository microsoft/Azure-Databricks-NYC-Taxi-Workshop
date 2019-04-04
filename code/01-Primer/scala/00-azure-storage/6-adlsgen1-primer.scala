// Databricks notebook source
// MAGIC %md
// MAGIC # ADLS gen 1 - primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will complete the following in batch operations:
// MAGIC 1.  Load a file to ADLSGen1<br>
// MAGIC 2.  Create a dataset and persist to ADLSGen1, create external table and run queries<br>
// MAGIC 
// MAGIC References:<br>
// MAGIC Databricks ADLS Gen1 integration: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define configuration needed for Delta - ADLSGen1

// COMMAND ----------

// ADLSGen1 credentials
val accessToken = dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "access-token")
val clientID = dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "client-id")
val tenantID = "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "tenant-id") + "/oauth2/token"

// Add ADLS cred to Spark conf
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", clientID)
spark.conf.set("dfs.adls.oauth2.credential", accessToken)
spark.conf.set("dfs.adls.oauth2.refresh.url", tenantID)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Clean up residual objects from potential prior runs

// COMMAND ----------

//Clean up files from potential prior run
dbutils.fs.rm("/mnt/workshop-adlsgen1/gwsroot/test/", recurse=true)
dbutils.fs.rm("/mnt/workshop-adlsgen1/gwsroot/books/", recurse=true)
display(dbutils.fs.ls("/mnt/workshop-adlsgen1/gwsroot/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Basic Directory and File Operations

// COMMAND ----------

// MAGIC %sh
// MAGIC mkdir /tmp
// MAGIC wget -P /tmp "https://generalworkshopsa.blob.core.windows.net/demo/If-By-Kipling.txt"

// COMMAND ----------

// MAGIC %sh
// MAGIC cat  /tmp/If-By-Kipling.txt

// COMMAND ----------

//Create directory
dbutils.fs.mkdirs("/mnt/workshop-adlsgen1/gwsroot/test")

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/tmp/If-By-Kipling.txt /mnt/workshop-adlsgen1/gwsroot/test/

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/workshop-adlsgen1/gwsroot/test/

// COMMAND ----------

// MAGIC %fs
// MAGIC head /mnt/workshop-adlsgen1/gwsroot/test/If-By-Kipling.txt

// COMMAND ----------

//Lets delete directory and contents recursively
dbutils.fs.rm("/mnt/workshop-adlsgen1/gwsroot/test", recurse=true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.0. Create dataset

// COMMAND ----------

val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901)
).toDF("book_id", "book_author", "book_name", "book_pub_year")

booksDF.printSchema
booksDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.0. Persist as parquet to ADLSGen1

// COMMAND ----------

val parquetTableDirectory = "/mnt/workshop-adlsgen1/gwsroot/books-prq/"
dbutils.fs.rm(parquetTableDirectory, recurse=true)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS books_db_adlsgen1;
// MAGIC 
// MAGIC USE books_db_adlsgen1;
// MAGIC DROP TABLE IF EXISTS books_prq;

// COMMAND ----------

//Persist dataframe to delta format with coalescing 
booksDF.coalesce(1).write.save(parquetTableDirectory)

// COMMAND ----------

//List
display(dbutils.fs.ls(parquetTableDirectory ))

// COMMAND ----------

// MAGIC %sql
// MAGIC USE books_db_adlsgen1;
// MAGIC CREATE TABLE books_prq
// MAGIC USING PARQUET
// MAGIC LOCATION "adl://gwsadlsgen1sa.azuredatalakestore.net/gwsroot/books-prq/";

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM books_db_adlsgen1.books_prq;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.0. Persist as Delta to ADLSGen1

// COMMAND ----------

val deltaTableDirectory = "/mnt/workshop-adlsgen1/gwsroot/books/"
dbutils.fs.rm(deltaTableDirectory, recurse=true)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS books_db_adlsgen1;
// MAGIC 
// MAGIC USE books_db_adlsgen1;
// MAGIC DROP TABLE IF EXISTS books;

// COMMAND ----------

//Persist dataframe to delta format with coalescing 
booksDF.coalesce(1).write.format("delta").save(deltaTableDirectory)

// COMMAND ----------

//List
//display(dbutils.fs.ls(deltaTableDirectory + "/_delta_log/"))
display(dbutils.fs.ls(deltaTableDirectory ))

// COMMAND ----------

// MAGIC %sql
// MAGIC USE books_db_adlsgen1;
// MAGIC CREATE TABLE books
// MAGIC USING DELTA
// MAGIC LOCATION "adl://gwsadlsgen1sa.azuredatalakestore.net/gwsroot/books/";

// COMMAND ----------

// MAGIC %sql
// MAGIC describe extended books_db_adlsgen1.books;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM books_db_adlsgen1.books;