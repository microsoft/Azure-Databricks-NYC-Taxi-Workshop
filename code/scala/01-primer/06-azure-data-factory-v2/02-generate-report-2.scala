// Databricks notebook source
// MAGIC %run ./00-common

// COMMAND ----------

//Generate report
val reportDF = spark.sql("SELECT case_year,primary_type as case_type, count(*) AS crime_count FROM crimes_db.chicago_crimes_curated GROUP BY case_year,primary_type")

// COMMAND ----------

import org.apache.spark.sql.SaveMode

//Persist report dataset to destination RDBMS
reportDF.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "CHICAGO_CRIMES_COUNT_BY_YEAR", connectionProperties)

// COMMAND ----------

dbutils.notebook.exit("Pass")