// Databricks notebook source
// MAGIC %run ./00-common

// COMMAND ----------

val batchID: Int = generateBatchID()

// COMMAND ----------

insertBatchMetadata(batchID,1,"Execute report 1","Started")
val executionStatusReport1 = dbutils.notebook.run("01-generate-report-1", 600)

if(executionStatusReport1 == "Pass")
  insertBatchMetadata(batchID,1,"Execute report 1","Completed")

// COMMAND ----------

var executionStatusReport2 = "-"
if(executionStatusReport1 == "Pass")
{
  insertBatchMetadata(batchID,2,"Execute report 2","Started")
  executionStatusReport2 = dbutils.notebook.run("02-generate-report-2", 60)
  insertBatchMetadata(batchID,2,"Execute report 2","Completed")
  
}

// COMMAND ----------

dbutils.notebook.exit(executionStatusReport2)

// COMMAND ----------

