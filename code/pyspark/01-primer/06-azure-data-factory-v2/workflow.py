# Databricks notebook source
executionStatusReport1 = dbutils.notebook.run("01-generate-report-1", 600)

# COMMAND ----------

executionStatusReport2 = "-"
if executionStatusReport1 == "Pass":
  executionStatusReport2 = dbutils.notebook.run("02-generate-report-2", 60)

# COMMAND ----------

dbutils.notebook.exit(executionStatusReport2)