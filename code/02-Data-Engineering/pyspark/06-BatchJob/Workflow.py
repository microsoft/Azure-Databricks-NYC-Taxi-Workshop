# Databricks notebook source
# MAGIC %run ./GlobalVarsAndMethods

# COMMAND ----------

insertBatchMetadata(1,"Execute report 1","Started")
executionStatusReport1 = dbutils.notebook.run("Report-1", 60)

if(executionStatusReport1 == "Pass"):
  insertBatchMetadata(1,"Execute report 1","Completed")
else:
  insertBatchMetadata(1,"Execute report 1","Failed")

# COMMAND ----------

executionStatusReport2 = "-"
if(executionStatusReport1 == "Pass"):
  insertBatchMetadata(2,"Execute report 2","Started")
  executionStatusReport2 = dbutils.notebook.run("Report-2", 60)
  if(executionStatusReport2 == "Pass"):
    insertBatchMetadata(2,"Execute report 2","Completed")
  else:
    insertBatchMetadata(2,"Execute report 2","Failed")  


# COMMAND ----------

if(executionStatusReport1 == "Pass"):
  dbutils.notebook.exit(executionStatusReport2)
else:
  dbutils.notebook.exit(executionStatusReport1)