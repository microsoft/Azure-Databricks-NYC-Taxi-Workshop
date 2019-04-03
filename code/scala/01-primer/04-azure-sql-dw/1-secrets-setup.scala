// Databricks notebook source
// MAGIC %md
// MAGIC # Secure your SQL datawarehouse credentials with Databricks secrets
// MAGIC 
// MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
// MAGIC In this exercise, we will secure credentials for the Azure SQL Datawarehouse in Databricks secrets.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Install Databricks CLI on your Linux terminal if you have not already

// COMMAND ----------

// MAGIC %md
// MAGIC Follow instructions in the Azure storage primer for settung up and configuring Databricks CLI

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create a secrets scope for your SQL database on your Linux terminal
// MAGIC In this example - we are calling it bhoomi-storage.  You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-sql-dw```

// COMMAND ----------

// MAGIC %md
// MAGIC This will open a file for you to enter in your username.  Save and close the file and your secret is saved.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Set up your Azure SQL Datawarehouse connection string within the scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-sql-dw --key conexion-string```

// COMMAND ----------

// MAGIC %md
// MAGIC This will open a file for you to enter in your connection string.  Save and close the file and your secret is saved.

// COMMAND ----------

// MAGIC %md
// MAGIC ###  4. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md You should see the scope created.

// COMMAND ----------

// MAGIC %md
// MAGIC ###  5. List the secrets within the scope

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope gws-sql-dw```

// COMMAND ----------

// MAGIC %md You should see the secret created.