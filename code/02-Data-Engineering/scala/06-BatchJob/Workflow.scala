// Databricks notebook source
// MAGIC %run ./GlobalVarsAndMethods

// COMMAND ----------

val batchID: Int = generateBatchID()

// COMMAND ----------

insertBatchMetadata(batchID,1,"Execute report 1","Started")
val executionStatusReport1 = dbutils.notebook.run("Report-1", 60)

if(executionStatusReport1 == "Pass")
  insertBatchMetadata(batchID,1,"Execute report 1","Completed")

// COMMAND ----------

var executionStatusReport2 = "-"
if(executionStatusReport1 == "Pass")
{
  insertBatchMetadata(batchID,2,"Execute report 2","Started")
  executionStatusReport2 = dbutils.notebook.run("Report-2", 60)
  insertBatchMetadata(batchID,2,"Execute report 2","Completed")
  
}

// COMMAND ----------

dbutils.notebook.exit(executionStatusReport2)