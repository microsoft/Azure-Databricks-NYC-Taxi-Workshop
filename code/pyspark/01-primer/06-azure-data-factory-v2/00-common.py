# Databricks notebook source
jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")
driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcHostname = "gws-server.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "gws_sql_db"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

#// Create a Properties() object to hold the parameters.
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : driverClass
}