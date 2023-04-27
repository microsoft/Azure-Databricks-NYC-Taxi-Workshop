# Databricks notebook source
# MAGIC %md 
# MAGIC # Leveraging Azure Machine Learning AutoML
# MAGIC ### Use case: Trip duration prediction 
# MAGIC ##### Model training using Azure ML AutoML
# MAGIC 
# MAGIC Automated ML in Azure Machine learning uses [Probabilistic Matrix Factorization](https://arxiv.org/pdf/1705.05355v2.pdf) - based on work in Microsoft Research. PMF maps model performance metrics to a high-dimensional space and can predict different ML Pipelines to try based on model performance.
# MAGIC 
# MAGIC It's important to note that the PMF function ___doesn't get access to the training data___. Instead, PMF uses model performance metrics such as RMSE, R2, and others to predict the best perfor
# MAGIC 
# MAGIC The goal of this tutorial is build on the data engineering lab and learn how to -
# MAGIC 1. Prepare the data for AutoML
# MAGIC 1. Leverage the AutoML feature of Azure ML service to test performance on several different models

# COMMAND ----------

# MAGIC %run ./99-Shared-Functions-and-Settings

# COMMAND ----------

from azureml.core import Workspace, Experiment, Run
from azureml.core.authentication import InteractiveLoginAuthentication

up = InteractiveLoginAuthentication()
up.get_authentication_header()

ws = Workspace(**AZURE_ML_CONF, auth=up)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Data for AutoML
# MAGIC 
# MAGIC While Automated ML will run on a Databricks cluster, it doesn't leverage Spark Dataframes. Therefore, we're going to put the data in a format that Automated ML can leverage.
# MAGIC 
# MAGIC We'll start by converting the data to a Pandas dataframe - be careful with data volumes when doing this as all data is collected to the driver node. 

# COMMAND ----------

trainDF, validDF, testDF = get_train_test_valid_data()

trainDFpd = trainDF.toPandas()
validDFpd = validDF.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll save these Pandas DataFrames as CSV files. This will allow us to use Azure ML Dataprep (which is required when running AutoML in a Databricks environment)

# COMMAND ----------

import os
import sys

# Make a temporary project folder
project_folder = os.path.join('/dbfs/tmp/', user_name, 'auto_ml')
os.makedirs(project_folder, exist_ok=True)

temp_train_csv_path = os.path.join(project_folder, 'train.csv')
temp_valid_csv_path = os.path.join(project_folder, 'valid.csv')
                                                         
trainDFpd.to_csv(temp_train_csv_path, index=False)
validDFpd.to_csv(temp_valid_csv_path, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create 'Dataflows'

# COMMAND ----------

def prepare_dataflows(csv_file_path, label_column='duration_minutes'):
  import azureml.dataprep as dprep

  dataflow_schema = {'taxi_type': dprep.FieldType.STRING,
                      'store_and_fwd_flag': dprep.FieldType.BOOLEAN,
                      'passenger_count': dprep.FieldType.INTEGER,
                      'trip_distance': dprep.FieldType.DECIMAL,
                      'vendor_abbreviation': dprep.FieldType.STRING,
                      'rate_code_description': dprep.FieldType.STRING,
                      'pickup_borough': dprep.FieldType.STRING,
                      'pickup_zone': dprep.FieldType.STRING,
                      'pickup_service_zone': dprep.FieldType.STRING,
                      'dropoff_borough': dprep.FieldType.STRING,
                      'dropoff_zone': dprep.FieldType.STRING,
                      'dropoff_service_zone': dprep.FieldType.STRING,
                      'pickup_year': dprep.FieldType.INTEGER,
                      'pickup_month': dprep.FieldType.INTEGER,
                      'pickup_day': dprep.FieldType.INTEGER,
                      'pickup_hour': dprep.FieldType.INTEGER,
                      'is_rush_hour_flag': dprep.FieldType.BOOLEAN,                                       
                      'is_weekend_flag': dprep.FieldType.BOOLEAN,
                      'duration_minutes': dprep.FieldType.DECIMAL,
                      }

  dataflow = dprep.read_csv(csv_file_path)
  dataflow = dataflow.set_column_types(dataflow_schema)

  return dataflow.keep_columns([label_column]), dataflow.drop_columns([label_column])

# COMMAND ----------

# Prepare the train and test dataflows 
train_Y, train_X = prepare_dataflows(temp_train_csv_path)
valid_Y ,valid_X = prepare_dataflows(temp_valid_csv_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure AutoML Task
# MAGIC 
# MAGIC Next, we'll configure the AutoML task to be performed. Here we specifiy settings like: 
# MAGIC * what type of ML task it is (`regression`, `classification`, or `forecasting`)
# MAGIC * `X` and `Y` variables (features and labels)
# MAGIC * if we want to `preprocess` the data
# MAGIC * where we want the compute to happen (in this case, in our `spark_context`)
# MAGIC * how many `iterations` we want to attempt
# MAGIC 
# MAGIC The full list of parameters can be found [in the AutoMLConfig class documentation](https://docs.microsoft.com/en-us/python/api/azureml-train-automl/azureml.train.automl.automlconfig.automlconfig?view=azure-ml-py)

# COMMAND ----------

from azureml.train.automl import AutoMLConfig
import logging

automl_config = AutoMLConfig(task = 'regression',
                             debug_log = 'automl_errors.log',
                             primary_metric = 'r2_score',
                             iteration_timeout_minutes = 10,
                             iterations = 4,
                             max_concurrent_iterations = 4, #change it based on number of worker nodes
                             verbosity = logging.INFO,
                             spark_context=sc,
                             enable_cache=True,
                             path = project_folder,
                             preprocess=True,
                             X=train_X,
                             y=train_Y,
                             X_valid=valid_X,
                             y_valid=valid_Y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the AutoML Tasks
# MAGIC Finally, we'll instantiate and Experiment and submit the `automl_config`. This will run for the specified number of iterations.

# COMMAND ----------

# Create AML Experiment - use the name from ./99-Shared-Functions-and-Settings notebook
experiment = Experiment(ws, automl_experiment_name)

# Submit AutoML Run
run = experiment.submit(automl_config)
run.wait_for_completion(show_output=True)