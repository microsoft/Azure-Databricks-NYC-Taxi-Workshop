// Databricks notebook source
// MAGIC %md
// MAGIC # JDBC Sink for Structured Streaming
// MAGIC Structued streaming does not feature a JDBC sink currently.<br>
// MAGIC The following is a custom sink we will use in the lab.

// COMMAND ----------

import java.sql._
import org.apache.spark.sql.ForeachWriter

class JDBCSink(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    var connection:java.sql.Connection = _
    var statement:java.sql.Statement = _

    def open(partitionId: Long, version: Long):Boolean = {
        Class.forName(driver)
        connection = java.sql.DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
    }

    def process(value: org.apache.spark.sql.Row): Unit = {        
    statement.executeUpdate("INSERT INTO chicago_crimes_curated_summary(case_id, primary_type, arrest_made,case_year, case_month, case_day_of_month) VALUES (" 
                 + "'" + value(0) + "'" + "," 
                 + "'" + value(1) + "'" + "," 
                 + "'" + value(2) + "'" + "," 
                 + "'" + value(3) + "'" + "," 
                 + "'" + value(4) + "'" + "," 
                 + "'" + value(5) + "'" + ");")
    }

    def close(errorOrNull:Throwable):Unit = {
        connection.close
    }
}


// COMMAND ----------

