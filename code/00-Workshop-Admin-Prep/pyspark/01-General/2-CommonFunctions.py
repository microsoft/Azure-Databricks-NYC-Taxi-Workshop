# Databricks notebook source
import os
import math
import glob
import re

# COMMAND ----------

prqShrinkageFactor = 0.19 #We found a saving in space of 81% with Parquet

# COMMAND ----------

def analyzeTables(databaseAndTable):
  try:
    print("Table: " + databaseAndTable)
    print("....refresh table")
    sql("REFRESH TABLE " + databaseAndTable)
    print("....analyze table")
    sql("ANALYZE TABLE " + databaseAndTable + " COMPUTE STATISTICS")
    print("....done")
  except Exception as e:
    return e 

# COMMAND ----------

def calcOutputFileCountTxtToPrq(srcDataFile, targetedFileSizeMB):
  try:
    estFileCount = int(math.floor((os.path.getsize(srcDataFile) * prqShrinkageFactor) / (targetedFileSizeMB * 1024 * 1024)))
    if(estFileCount == 0):
      return 1 
    else:
      return estFileCount
  except Exception as e:
    return e                     

# COMMAND ----------

#Delete residual files from job operation (_SUCCESS, _start*, _committed*)
#Should be called with '/dbfs/mnt/...'
def recursivelyDeleteSparkJobFlagFiles(directoryPath):
  try:
    files = glob.glob(directoryPath + '/**/*', recursive=True)
    for file in files:
      if not os.path.basename(file).endswith('parquet') and os.path.isfile(file):
        fileReplaced = re.sub('/dbfs', 'dbfs:',file)
        print("Deleting...." +  fileReplaced)
        dbutils.fs.rm(fileReplaced)
  except Exception as e:
    return e 