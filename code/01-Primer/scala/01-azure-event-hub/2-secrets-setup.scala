// Databricks notebook source
// MAGIC %md
// MAGIC # Secure your Azure Event Hub credentials with Databricks secrets
// MAGIC 
// MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
// MAGIC   
// MAGIC **Pre-requisite**: You should have created your Azure event Hub.<br><br>
// MAGIC In this notebook, we will -<br>
// MAGIC   1.  Install Dataricks CLI if not already installed
// MAGIC   2.  Secure credentials
// MAGIC   

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Install Databricks CLI on your Linux terminal if you have not already

// COMMAND ----------

// MAGIC %md
// MAGIC Follow instructions from the Azure storage primer module.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Secure your event hub credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.1. Create a secrets scope for your event hub
// MAGIC You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-crimes-aeh```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.2. Set up your credentials within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-crimes-aeh --key conexion-string```

// COMMAND ----------

// MAGIC %md
// MAGIC This will open a file for you to enter in your storage account key.  Save and close the file and your storage secret is saved.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.3. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md You should see the scope created.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.4. List the secrets within the scope created

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope gws-crimes-aeh```

// COMMAND ----------

// MAGIC %md You should see the secret created.