// Databricks notebook source
//JDBC connectivity related
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")
val jdbcPort = 1433
val jdbcDatabase = "gws_sql_db"

//Replace with your server name
val jdbcHostname = "gws-server.database.windows.net"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

def generateBatchID(): Int = 
{
  var batchId: Int = 0
  var pushdown_query = "(select count(*) as record_count from BATCH_JOB_HISTORY) table_record_count"
  val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
  val recordCount = df.first().getInt(0)
  println("Record count=" + recordCount)
  
  if(recordCount == 0)
    batchId=1
  else 
  {
    pushdown_query = "(select max(batch_id) as current_batch_id from BATCH_JOB_HISTORY) current_batch_id"
    val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    batchId = df.first().getInt(0) + 1
  }
  batchId
}

// COMMAND ----------

import java.sql._
import java.util.Calendar

//Function to insert ETL batch metadata into RDBMS
def insertBatchMetadata(batchID: Int, processID: Int, activityName: String, activityStatus: String): Unit = 
{
    var conn: Connection = null
    var stmt: Statement = null
  
    val insertSql = """
    |insert into batch_job_history (batch_id,batch_step_id,batch_step_description,batch_step_status,batch_step_time)
    |values (?,?,?,?,?)
""".stripMargin
  
    try {
        Class.forName(driverClass)
        conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

        val preparedStmt: PreparedStatement = conn.prepareStatement(insertSql)
        preparedStmt.setInt(1, batchID)
        preparedStmt.setInt(2, processID)
        preparedStmt.setString(3, activityName)
        preparedStmt.setString(4, activityStatus)
        preparedStmt.setString(5, Calendar.getInstance().getTime().toString)
      
        preparedStmt.execute

        // cleanup
        preparedStmt.close
        conn.close
    } catch {
        case se: SQLException => se.printStackTrace
        case e:  Exception => e.printStackTrace
    } finally {
        try {
            if (stmt!=null) stmt.close
        } catch {
            case se2: SQLException => // nothing we can do
        }
        try {
            if (conn!=null) conn.close
        } catch {
            case se: SQLException => se.printStackTrace
        } //end finally-try
    } //end try
}