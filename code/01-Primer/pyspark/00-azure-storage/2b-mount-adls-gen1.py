# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Gen1
# MAGIC 
# MAGIC Mounting Azure storage in Azure Databricks allows you to access the cloud storage like they are directories.<BR>
# MAGIC   
# MAGIC ### What's in this exercise?
# MAGIC The scope of this workshop is restricted to access via Service Principal and AAD based pass through authentication is out of scope.
# MAGIC We will mount ADLSGen1 to Databricks in this module.<br>
# MAGIC 
# MAGIC To mount ADLS Gen 1 - we need a few pieces of information<br>
# MAGIC 1.  Service principal - Application ID<br>
# MAGIC 2.  Directory ID (AAD tenant ID)<br>
# MAGIC 3.  Access tokey/key associated with the application ID<br>
# MAGIC 
# MAGIC We also need a directory created, and full access to the root directory and child items to the SPN from #1<br>
# MAGIC The provisioning docs cover the steps - https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/blob/master//docs/1-provisioning-guide/ProvisioningGuide.md

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define ADLS configuration

# COMMAND ----------

# ADLS URI (replace gwsadlsgen1sa with your instance name)
adlsGen1URI = "adl://" + "gwsadlsgen1sa" + ".azuredatalakestore.net/"

# ADLSGen1 credentials
accessToken = dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "access-token")
clientID = dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "client-id")
tenantID = "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "tenant-id") + "/oauth2/token"

# ADLS config for mounting
adlsConfigs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": clientID,
           "dfs.adls.oauth2.credential": accessToken,
           "dfs.adls.oauth2.refresh.url":tenantID}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Mount ADLSGen1

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0.1. Mount manually

# COMMAND ----------

# MAGIC %md
# MAGIC Create a mount point location

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /mnt/workshop-adlsgen1

# COMMAND ----------

# Check if already mounted
display(dbutils.fs.ls("/mnt/workshop-adlsgen1"))

# COMMAND ----------

dbutils.fs.unmount("/mnt/workshop-adlsgen1/gwsroot")

# COMMAND ----------

# Optionally, you can add <your-directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://gwsadlsgen1sa.azuredatalakestore.net/gwsroot",
  mount_point = "/mnt/workshop-adlsgen1/gwsroot",
  extra_configs = adlsConfigs)

# COMMAND ----------

# Display directories
display(dbutils.fs.ls("/mnt/workshop-adlsgen1"))

# COMMAND ----------

# Unmount - we will mount using a function
dbutils.fs.unmount("/mnt/workshop-adlsgen1/gwsroot")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0.2. Mount using a function

# COMMAND ----------

# This is a function to mount ADLSGen1
def mountStorage(directoryToBeMounted, mountPoint):
  try:
    print("Mounting {0} to {1}:".format(directoryToBeMounted, mountPoint))
    # Unmount if already mounted
    dbutils.fs.unmount(mountPoint)
    
  except Exception as e:
    # If this errors, safe to assume that the container is not mounted
    print("....Not mounted; Attempting mounting now..")
    
  # Mount ADLSGen2 directory
  mountStatus = dbutils.fs.mount(
                    source = adlsGen1URI + directoryToBeMounted,
                    mount_point = mountPoint,
                    extra_configs = adlsConfigs)

  print("....Status of mount is: " + str(mountStatus))
  print() # Provide a blank line between mounts

# COMMAND ----------

# Mount using the function defined
mountStorage("gwsroot","/mnt/workshop-adlsgen1/gwsroot")

# COMMAND ----------

# Display directories
display(dbutils.fs.ls("/mnt/workshop-adlsgen1/gwsroot/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Refresh mount points

# COMMAND ----------

# Refresh mounts if applicable
#  dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. How to unmount

# COMMAND ----------

# dbutils.fs.unmount(<yourMountPoint>)