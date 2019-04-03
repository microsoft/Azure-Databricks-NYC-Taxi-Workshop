# Databricks notebook source
# MAGIC %md
# MAGIC # Secure your SQL database credentials with Databricks secrets
# MAGIC 
# MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
# MAGIC In this exercise, we will secure credentials for the Azure SQL Database in Databricks secrets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Install Databricks CLI on your Linux terminal

# COMMAND ----------

# MAGIC %md
# MAGIC Refer CLI setup of Azure storage - secrets primer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create a secrets scope for your SQL database on your Linux terminal
# MAGIC In this example - we are calling it bhoomi-storage.  You can name it per your preference.

# COMMAND ----------

# MAGIC %md
# MAGIC ```databricks secrets create-scope --scope gws-sql-db```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Set up your Azure SQL database username within the scope

# COMMAND ----------

# MAGIC %md
# MAGIC ```databricks secrets put --scope gws-sql-db --key username```

# COMMAND ----------

# MAGIC  %md
# MAGIC This will open a file for you to enter in your username.  Save and close the file and your secret is saved.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Set up your Azure SQL database password within the scope

# COMMAND ----------

# MAGIC %md
# MAGIC ```databricks secrets put --scope gws-sql-db --key password```

# COMMAND ----------

# MAGIC %md
# MAGIC This will open a file for you to enter in your password.  Save and close the file and your secret is saved.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  5. List your secret scopes

# COMMAND ----------

# MAGIC %md
# MAGIC ```databricks secrets list-scopes```

# COMMAND ----------

# MAGIC %md You should see the scope created.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  6. List the secrets within the scope

# COMMAND ----------

# MAGIC %md ```databricks secrets list --scope gws-sql-db```

# COMMAND ----------

# MAGIC %md You should see the secret created.