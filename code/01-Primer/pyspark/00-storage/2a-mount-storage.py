# Databricks notebook source
# MAGIC %md
# MAGIC # Mount blob storage
# MAGIC
# MAGIC We assume /mnt has already been mounted as s3 storage.
# MAGIC
# MAGIC This notebooks needs only to be run by the workshop organizer 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Make root

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /mnt/workshop

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Mount blob storage

# COMMAND ----------

# Check if already mounted
display(dbutils.fs.ls("/mnt/workshop"))
'''
# Unmount if already mounted - as needed
dbutils.fs.unmount("/mnt/workshop/consumption/")
dbutils.fs.unmount("/mnt/workshop/curated/")
dbutils.fs.unmount("/mnt/workshop/demo/")
dbutils.fs.unmount("/mnt/workshop/raw/")
dbutils.fs.unmount("/mnt/workshop/staging/")
dbutils.fs.unmount("/mnt/workshop/scratch/")
'''

# COMMAND ----------

# Sample to mount blob storage
'''
dbutils.fs.mount(
  source = "wasbs://scratch@gwsblobsa.blob.core.windows.net/",
  mount_point = "/mnt/workshop/scratch",
  extra_configs = {"fs.azure.account.key." + storageAccountName + ".blob.core.windows.net": storageAccountAccessKey})
'''

# COMMAND ----------

# # This is a function to mount a storage container
# def mountStorageContainer(storageAccount, storageAccountKey, storageContainer, blobMountPoint):
#   try:
#     print("Mounting {0} to {1}:".format(storageContainer, blobMountPoint))
#     # Unmount the storage container if already mounted
#     dbutils.fs.unmount(blobMountPoint)
#   except Exception as e:
#     # If this errors, safe to assume that the container is not mounted
#     print("....Container is not mounted; Attempting mounting now..")
    
#   # Mount the storage container
#   mountStatus = dbutils.fs.mount(
#                   source = "wasbs://{0}@{1}.blob.core.windows.net/".format(storageContainer, storageAccount),
#                   mount_point = blobMountPoint,
#                   extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storageAccount): storageAccountKey})

#   print("....Status of mount is: " + str(mountStatus))
#   print() # Provide a blank line between mounts

# COMMAND ----------

# # Mount the various storage containers created
# mountStorageContainer(storageAccountName,storageAccountAccessKey,"scratch","/mnt/workshop/scratch")
# mountStorageContainer(storageAccountName,storageAccountAccessKey,"staging","/mnt/workshop/staging")
# mountStorageContainer(storageAccountName,storageAccountAccessKey,"raw","/mnt/workshop/raw")
# mountStorageContainer(storageAccountName,storageAccountAccessKey,"curated","/mnt/workshop/curated")
# mountStorageContainer(storageAccountName,storageAccountAccessKey,"consumption","/mnt/workshop/consumption")

# COMMAND ----------

#Display directories
display(dbutils.fs.ls("/mnt/workshop"))

# COMMAND ----------

# MAGIC %fs ls /mnt/workshop/raw/nyctaxi/transactions/green-taxi/trip_year=2017/trip_month=01/

# COMMAND ----------

# MAGIC  %md
# MAGIC ### 3. Refresh mount points

# COMMAND ----------

# Refresh mounts if applicable
# dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. How to unmount

# COMMAND ----------

# dbutils.fs.unmount(<yourMountPoint>)
