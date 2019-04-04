# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Store Gen2
# MAGIC 
# MAGIC Mounting Azure storage in Azure Databricks allows you to access the cloud storage like they are directories.<BR>
# MAGIC   
# MAGIC ### What's in this exercise?
# MAGIC The scope of this workshop is restricted to access via Service Principal and AAD based pass through authentication is out of scope. We will mount ADLSGen2 to Databricks in this module.<BR>
# MAGIC 
# MAGIC #### Create ADLS Gen2 file system 
# MAGIC To mount an ADLSGen2 file system, the ADLS filesystem should be created first.<BR>
# MAGIC To create the ADLS file system, we need the storage account key.<BR>
# MAGIC 
# MAGIC   
# MAGIC #### Mount an ADLS Gen2 file system 
# MAGIC To mount an ADLS Gen2 file system, we need the following completed/the following information available.-<br>
# MAGIC 1.  Create an app registration in AAD; This creates a service principal with an App ID<BR>
# MAGIC 2.  Directory ID (AAD tenant ID)<BR>
# MAGIC 3.  Access tokey/key associated with the application ID<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Initialize root file system in ADLSGen2 
# MAGIC This is a one-time activity, typically performed as part of the release.  Consider externalizing this in your apps to a seperate notebook.

# COMMAND ----------

sourceToBeMounted = "abfss://gwsroot@gwsadlsgen2sa.dfs.core.windows.net/"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.gwsadlsgen2sa.dfs.core.windows.net", dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "storage-acct-key"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://gwsroot@gwsadlsgen2sa.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Define credentials for mounting

# COMMAND ----------

# Credentials
clientID = dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "client-id")
clientSecret = dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "client-secret")
tenantID = "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "tenant-id") + "/oauth2/token"

# ADLS config for mounting
adlsConfigs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": tenantID}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Mount ADLSGen2 file systems

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0.1. Mount a single file system

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /mnt/workshop-adlsgen2/gwsroot

# COMMAND ----------

# Unmount in case its already
dbutils.fs.unmount("/mnt/workshop-adlsgen2/gwsroot")

# COMMAND ----------

# Sample for mounting gwsroot file system
dbutils.fs.mount(
  source = "abfss://gwsroot@gwsadlsgen2sa.dfs.core.windows.net/",
  mount_point = "/mnt/workshop-adlsgen2/gwsroot/",
  extra_configs = adlsConfigs)

# COMMAND ----------

# Display contents
display(dbutils.fs.ls("/mnt/workshop-adlsgen2/gwsroot"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0.2. Validate mount

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Lets try a file upload

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -P /tmp "https://generalworkshopsa.blob.core.windows.net/demo/If-By-Kipling.txt"

# COMMAND ----------

# Copy to mount point
dbutils.fs.cp("file:/tmp/If-By-Kipling.txt","/mnt/workshop-adlsgen2/gwsroot/If-By-Kipling.txt")
# Check if already mounted
display(dbutils.fs.ls("/mnt/workshop-adlsgen2/gwsroot/"))