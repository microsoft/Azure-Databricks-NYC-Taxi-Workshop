# Databricks notebook source
# MAGIC %md
# MAGIC # File System Operations
# MAGIC
# MAGIC ### What's in this exercise?
# MAGIC
# MAGIC 101 on working with databricks file system (DBFS) backed by Azure blob storage

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0. Azure Blob Storage
# MAGIC
# MAGIC In the previous section, we mounted blob storage, we will use the scratch directory for this exercise.
# MAGIC The following are covered-
# MAGIC 1.  Create a directory
# MAGIC 2.  Download a file to local file system
# MAGIC 3.  Save the local file to blob storage
# MAGIC 4.  List blob storage directory contents
# MAGIC 5.  Delete the local file
# MAGIC 6.  Copy a file
# MAGIC 7.  Delete a file
# MAGIC 8.  Delete a directory

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.1. List contents of the scratch directory

# COMMAND ----------

# MAGIC %md
# MAGIC We got this errror because the directory already exists

# COMMAND ----------

display(dbutils.fs.ls("/mnt/workshop/scratch"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.2. Download a file to the local file system

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -P /tmp "https://generalworkshopsa.blob.core.windows.net/demo/If-By-Kipling.txt"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp | grep If

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /tmp/If-By-Kipling.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC head /tmp/If-By-Kipling.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.3. Load the file to Azure Blob Storage

# COMMAND ----------

dbutils.fs.cp("file:/tmp/If-By-Kipling.txt","/mnt/workshop/scratch/test/If-By-Kipling.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.4. List contents of Azure Blob Storage

# COMMAND ----------

display(dbutils.fs.ls("/mnt/workshop/scratch/test"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.5. Delete local file

# COMMAND ----------

dbutils.fs.rm("file:/tmp/If-By-Kipling.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.6. Copy a file

# COMMAND ----------

dbutils.fs.cp("/mnt/workshop/scratch/test/If-By-Kipling.txt","/mnt/workshop/scratch/test/If-By-Kipling-2.txt")
dbutils.fs.cp("/mnt/workshop/scratch/test/If-By-Kipling.txt","/mnt/workshop/scratch/test/If-By-Kipling-3.txt")
dbutils.fs.cp("/mnt/workshop/scratch/test/If-By-Kipling.txt","/mnt/workshop/scratch/test/If-By-Kipling-4.txt")
dbutils.fs.cp("/mnt/workshop/scratch/test/If-By-Kipling.txt","/mnt/workshop/scratch/test/If-By-Kipling-5.txt")
dbutils.fs.cp("/mnt/workshop/scratch/test/If-By-Kipling.txt","/mnt/workshop/scratch/test/If-By-Kipling-6.txt")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/workshop/scratch/test/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.7. Delete a file

# COMMAND ----------

dbutils.fs.rm("/mnt/workshop/scratch/test/If-By-Kipling-6.txt")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/workshop/scratch/test/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.8. Delete a directory recursively

# COMMAND ----------

dbutils.fs.rm("/mnt/workshop/scratch/test/",recurse=True)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/workshop/scratch/"))
