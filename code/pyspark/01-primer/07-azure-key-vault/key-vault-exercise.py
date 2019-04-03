# Databricks notebook source
# MAGIC %md
# MAGIC # Key vault backed secrets in Azure Databricks
# MAGIC 
# MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
# MAGIC   
# MAGIC **Pre-requisite**: You should have created an Azure Key Vault and completed the storage and SQL DW primers<br><br>
# MAGIC In this notebook, we will -<br>
# MAGIC   1.  Provision Azure Key Vault
# MAGIC   2.  Create your secret scope
# MAGIC   3.  Create your secrets in Azure Key Vault
# MAGIC   4.  Test using Azure Key Vault secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a secret scope in Azure Key Vault

# COMMAND ----------

# MAGIC %md
# MAGIC 1.  Go to https://<your_azure_databricks_url>#secrets/createScope (for example, https://eastus2.azuredatabricks.net#secrets/createScope).<br>
# MAGIC 2.  Create a scope
# MAGIC     - Enter scope name (gws-akv-sqldw)
# MAGIC     - Select "creator" for "Manage principal"
# MAGIC     - Enter DNS name of the Key Vault you created - you will find it in the portal, overview as DNS name
# MAGIC     - Enter the resource ID of the Key Vault you created - you will find it in the portal, in the properties page as resource ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Secure your SQL Datawarehouse credentials

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0.1. Set up your credentials within the secret scope

# COMMAND ----------

# MAGIC %md
# MAGIC List scopes to check if the scope you created exists:<br>
# MAGIC ```databricks secrets list-scopes```

# COMMAND ----------

# MAGIC %md
# MAGIC Create a secret in the portal

# COMMAND ----------

# MAGIC %md
# MAGIC List secrets within the scope to check if the scope you created exists:<br>
# MAGIC ```databricks secrets list --scope gws-akv-sqldw```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test the secrets created

# COMMAND ----------

#//JDBC URL
jdbcURL = dbutils.secrets.get(scope = "gws-akv-sqldw", key = "conexion-string")

#//Storage account credentials for tempDir access
spark.conf.set(
  "fs.azure.account.key.gwsblobsa.blob.core.windows.net",
  dbutils.secrets.get(scope = "gws-blob-storage", key = "storage-acct-key"))

# COMMAND ----------

df = spark.sql("select case_id,primary_type,cast(arrest_made as string),case_year,case_month,case_day_of_month from crimes_db.chicago_crimes_curated")

# COMMAND ----------

df.write \
  .format("com.databricks.spark.sqldw") \
  .mode('append')   \
  .option("url", jdbcURL)  \
  .option("forwardSparkAzureStorageCredentials", "true")  \
  .option("dbTable", "chicago_crimes_curated_summary")  \
  .option("tempDir", "wasbs://scratch@gwsblobsa.blob.core.windows.net/sqldwbatch-tempdir")  \
  .save()

# COMMAND ----------

