// Databricks notebook source
// MAGIC %md
// MAGIC # Secure your storage key with Databricks secrets
// MAGIC 
// MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
// MAGIC   
// MAGIC **Pre-requisite**: You should have created your Azure Blob Storage account and ADLS Gen2 account.<br><br>
// MAGIC In this notebook, we will -<br>
// MAGIC   1.  Install & configure Dataricks CLI
// MAGIC   2.  Secure blob storage credentials
// MAGIC   3.  Secure ADLS Gen1 storage credentials
// MAGIC   4.  Secure ADLS Gen2 storage credentials
// MAGIC   

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Install and configure Databricks CLI on your Linux terminal

// COMMAND ----------

// MAGIC %md ```sudo su -```

// COMMAND ----------

// MAGIC %md ```apt-get update```

// COMMAND ----------

// MAGIC %md
// MAGIC ``` apt install python-pip```

// COMMAND ----------

// MAGIC %md
// MAGIC ```pip install databricks-cli```

// COMMAND ----------

// MAGIC %md On the Databricks site, navigate to the user icon on the right-hand side, top, and click on "User setting".  Generate a token, capture to clipboard.

// COMMAND ----------

// MAGIC %md ```databricks configure --token```<br>
// MAGIC Enter the cluster URL..e.g.https://eastus2.azuredatabricks.net/<br>
// MAGIC AND<br>
// MAGIC Enter the token you captured<br>
// MAGIC <br>
// MAGIC You are now all set.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Secure your blob storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.1. Create a secrets scope for your storage account
// MAGIC You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-blob-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.2. Set up your storage account key within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-blob-storage --key storage-acct-key```

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

// MAGIC %md ```databricks secrets list --scope gws-blob-storage```

// COMMAND ----------

// MAGIC %md You should see the secret created.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Secure your Azure Data Lake Store Gen2 (ADLSGen2) storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.1. Create a secrets scope for your ADLSGen2 storage
// MAGIC You can name the scope per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-adlsgen2-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.2. Set up your ADLSGen2 credentials within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key storage-acct-key```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key client-id```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key client-secret```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key tenant-id```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.3. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.4. List the secrets within the scope created

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope gws-adlsgen2-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Secure your Azure Data Lake Store Gen1 (ADLSGen1) storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC Follow the provisionin documentation to ensure dependencies are met:<br>
// MAGIC 1.  Create a service principal, and an access token<br>
// MAGIC 2.  Create a directory in ADLS Gen 1<br>
// MAGIC 3.  Grant the service principal full access - top as well as child entities to the directory<br>
// MAGIC 4.  Capture the following information we will need for mounting:<br>
// MAGIC   - SPN Application ID - we will use this for client ID
// MAGIC   - Application Access token
// MAGIC   - AAD Tenant ID

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.1. Create a secrets scope for your ADLSGen1 storage
// MAGIC You can name the scope per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-adlsgen1-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.2. Set up your ADLSGen1 credentials within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen1-storage --key access-token```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen1-storage --key client-id```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen1-storage --key tenant-id```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.3. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.4. List the secrets within the scope created

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope gws-adlsgen1-storage```