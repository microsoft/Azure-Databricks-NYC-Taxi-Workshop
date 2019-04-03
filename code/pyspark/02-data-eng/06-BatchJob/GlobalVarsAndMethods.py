# Databricks notebook source
#Database credentials & details - for use with Spark scala for writing

#Secrets
jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")

#JDBC driver class & connection properties
driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcHostname = "gws-server.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "gws_sql_db"

#JDBC URI
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

#Properties() object to hold the parameters
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : driverClass
}

# COMMAND ----------

def generateBatchID():
  try:
    batchId = 0
    pushdown_query = "(select count(*) as record_count from BATCH_JOB_HISTORY) table_record_count"
    df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    recordCount = df.first()[0]

    if(recordCount == 0):
      batchId=1
    else:
      pushdown_query = "(select max(batch_id) as current_batch_id from BATCH_JOB_HISTORY) current_batch_id"
      df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
      batchId = int(df.first()[0]) + 1
    return batchId
  except Exception as e:
    return e  


# COMMAND ----------

from datetime import datetime

# COMMAND ----------

def insertBatchMetadata(batch_step_id,batch_step_description,batch_step_status):
	try:
		batch_id = generateBatchID()
		batch_step_time = str(datetime.now())
		queryString ='select "{}" as batch_id,"{}" as batch_step_id,"{}" as batch_step_description,"{}" as batch_step_status,"{}" as batch_step_time'.format(batch_id,batch_step_id,batch_step_description,batch_step_status,batch_step_time)
		insertQueryDF = sqlContext.sql(queryString)
		insertQueryDF.coalesce(1).write.jdbc(jdbcUrl, "batch_job_history", mode="append", properties=connectionProperties)
	except Exception as e:
		return e