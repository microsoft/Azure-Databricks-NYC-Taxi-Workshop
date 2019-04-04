// Databricks notebook source
// MAGIC %md
// MAGIC # Mount ADLS Gen1
// MAGIC 
// MAGIC Mounting Azure storage in Azure Databricks allows you to access the cloud storage like they are directories.<BR>
// MAGIC   
// MAGIC ### What's in this exercise?
// MAGIC The scope of this workshop is restricted to access via Service Principal and AAD based pass through authentication is out of scope.
// MAGIC We will mount ADLSGen1 to Databricks in this module.<br>
// MAGIC 
// MAGIC To mount ADLS Gen 1 - we need a few pieces of information<br>
// MAGIC 1.  Service principal - Application ID<br>
// MAGIC 2.  Directory ID (AAD tenant ID)<br>
// MAGIC 3.  Access tokey/key associated with the application ID<br>
// MAGIC 
// MAGIC We also need a directory created, and full access to the root directory and child items to the SPN from #1<br>
// MAGIC The provisioning docs cover the steps - https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/blob/master//docs/1-provisioning-guide/ProvisioningGuide.md

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define ADLS configuration

// COMMAND ----------

//ADLS URI (replace gwsadlsgen1sa with your instance name)
val adlsGen1URI = "adl://" + "gwsadlsgen1sa" + ".azuredatalakestore.net/"

// ADLSGen1 credentials
val accessToken = dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "access-token")
val clientID = dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "client-id")
val tenantID = "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = "gws-adlsgen1-storage", key = "tenant-id") + "/oauth2/token"

// ADLS config for mounting
val adlsConfigs = Map("dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
           "dfs.adls.oauth2.client.id" -> clientID,
           "dfs.adls.oauth2.credential" -> accessToken,
           "dfs.adls.oauth2.refresh.url" -> tenantID)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Mount ADLSGen1

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.1. Mount manually

// COMMAND ----------

// MAGIC %md
// MAGIC Create a mount point location

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /mnt/workshop-adlsgen1

// COMMAND ----------

// Check if already mounted
display(dbutils.fs.ls("/mnt/workshop-adlsgen1"))

// COMMAND ----------

// Unmount in case you are running a second time
dbutils.fs.unmount("/mnt/workshop-adlsgen1/gwsroot")

// COMMAND ----------

// Sample for mounting gwsroot directory
dbutils.fs.mount(
  source = "adl://gwsadlsgen1sa.azuredatalakestore.net/gwsroot",
  mountPoint = "/mnt/workshop-adlsgen1/gwsroot",
  extraConfigs = adlsConfigs)

// COMMAND ----------

//Display directories
display(dbutils.fs.ls("/mnt/workshop-adlsgen1"))

// COMMAND ----------

// Unmount - we will mount using a function
dbutils.fs.unmount("/mnt/workshop-adlsgen1/gwsroot")

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.2. Mount using a function

// COMMAND ----------

//This is a function to mount a directory
def mountStorage(directoryToBeMounted: String, mountPoint: String)
{
   try {
     
     println(s"Mounting ${directoryToBeMounted} to ${mountPoint}:")
    // Unmount the directory if already mounted
    dbutils.fs.unmount(mountPoint)

  } catch { 
    //If this errors, the directory is not mounted
    case e: Throwable => println(s"....Directory is not mounted; Attempting mounting now..")

  } finally {
    // Mount the directory
    val mountStatus = dbutils.fs.mount(
    source = adlsGen1URI + directoryToBeMounted,
    mountPoint = mountPoint,
    extraConfigs = adlsConfigs)
  
    println("...Status of mount is: " + mountStatus)
  }
}

// COMMAND ----------

//Mount using the function defined
mountStorage("gwsroot","/mnt/workshop-adlsgen1/gwsroot")

// COMMAND ----------

//Display directories
display(dbutils.fs.ls("/mnt/workshop-adlsgen1/gwsroot/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Refresh mount points

// COMMAND ----------

//Refresh mounts if applicable
//dbutils.fs.refreshMounts()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. How to unmount

// COMMAND ----------

//dbutils.fs.unmount(<yourMountPoint>)
