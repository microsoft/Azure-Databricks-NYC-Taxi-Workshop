# Databricks notebook source


# COMMAND ----------

# import os
# import sys
# pwd = os.getcwd()
# repo = "/nyc-workshop"
# repo_root = pwd.split(repo)[0] + repo
# sys.path.append(repo_root)

# COMMAND ----------

# print (sys.path)


# COMMAND ----------

from libs.tblname import tblname
tblname(tbl="taxi_db")

# COMMAND ----------


