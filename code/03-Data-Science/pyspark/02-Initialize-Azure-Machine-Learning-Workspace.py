# Databricks notebook source
# MAGIC %md
# MAGIC # Initializing Connection to Azure Machine Learning Services
# MAGIC 
# MAGIC As you'll see in subsequent notebooks, we need to instantiate a 'Workspace' object when we want to use Azure Machine Learning Services.
# MAGIC 
# MAGIC In this notebook, we'll test our connection to the workspace, and, if needed, create a workspace to use.
# MAGIC 
# MAGIC However, we'll first need to install the `azureml-sdk` pip library. To do that, you will create a new library, choose PyPI, and then enter `azureml-sdk[databricks]` for the Repository name.

# COMMAND ----------

# MAGIC %run "./99-Shared-Functions-and-Settings"

# COMMAND ----------

if AZURE_ML_CONF['subscription_id'] is None:
  raise KeyError("subscription_id, resource_group, and workspace_name must be filled out in notebook 99-Shared-Functions-and-Settings")

# COMMAND ----------

from azureml.core import Workspace
from azureml.core.authentication import InteractiveLoginAuthentication

# Please note - with other examples you will find online, you don't need to do the following lines iwth the up object
# However, when working with multiple users in the same Databricks cluster, you should follow these three steps to 
# avoid interfering with other users' authentication

# Note - it won't let you "log in" as another user, it'll just make you reauthenticate every time you interact with the AML Workspace

up = InteractiveLoginAuthentication()
up.get_authentication_header()

ws = Workspace(**AZURE_ML_CONF, auth=up)

# In non-Databricks world, this would look like:
# ws = Workspace(**AZURE_ML_CONF)

# COMMAND ----------

# If you need to create a new Azure Machine Learning workspace, you can do that via the SDK.

# ws = Workspace.create(**AZURE_ML_CONF, location=AZURE_REGION, exist_ok=True)

# COMMAND ----------

