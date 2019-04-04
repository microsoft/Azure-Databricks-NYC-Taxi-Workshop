# Databricks notebook source
# MAGIC %run ./00-common

# COMMAND ----------

#Generate report
reportDF = spark.sql("SELECT case_year,primary_type as case_type, count(*) AS crime_count FROM crimes_db.chicago_crimes_curated GROUP BY case_year,primary_type")

# COMMAND ----------

#Persist report dataset to destination RDBMS
reportDF.coalesce(1).write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "CHICAGO_CRIMES_COUNT_BY_YEAR") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

dbutils.notebook.exit("Pass")