# Databricks notebook source
# MAGIC %md
# MAGIC ## Operationalize (o16n) our PySpark Model using Azure Machine Learning to deploy
# MAGIC 
# MAGIC AML services can be used to deploy a model as a REST API with very little coding required on the part of the data scientist.
# MAGIC 
# MAGIC In this lab we'll using Spark and AML services to:
# MAGIC 1. Pull the details of the runs to find the best performing model
# MAGIC 1. Download our saved model file and extract locally
# MAGIC 1. Register a Model object with AML
# MAGIC 1. Write a `score_model.py` script
# MAGIC 1. Create a Docker container to score the PySpark model
# MAGIC 1. Deploy the Docker container as a Azure Container Instance (ACI) container

# COMMAND ----------

# MAGIC %run ./99-Shared-Functions-and-Settings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instantiate the Workspace and Experiment objects

# COMMAND ----------

from azureml.core import Workspace, Experiment, Run
from azureml.core.authentication import InteractiveLoginAuthentication

up = InteractiveLoginAuthentication()
up.get_authentication_header()

ws = Workspace(**AZURE_ML_CONF, auth=up)
experiment = Experiment(ws, pyspark_experiment_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find best performing run
# MAGIC To find the best performing model run from our experiment, we have several options.
# MAGIC 
# MAGIC We can:
# MAGIC 1. Use the Azure Portal to compare runs
# MAGIC 1. Use Python to compare runs

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Azure Portal
# MAGIC The `Experiment` object has the `get_portal_url()` method that will auto populate the URL.
# MAGIC 
# MAGIC We can use Databricks' `displayHTML` function to render a hyperlink.

# COMMAND ----------

# To find the best performing model, we have several options - we can retrieve the metrics from within Python or we review the Azure portal
displayHTML('<a href="{url}" target="_blank">{url}</a>'.format(url=experiment.get_portal_url()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Python to Compare Runs
# MAGIC 
# MAGIC Each `Run` object has a `get_metrics()` method that will retrieve our stored metrics. We can leverage the `get_runs()` method of the `Experiment` object to retrieve the run objects.
# MAGIC 
# MAGIC We will then render a table to compare model performance.

# COMMAND ----------

# Download RMSE and R2 from AML Service
import pandas as pd

# Use list comprehension to retrieve records from experiment object.
run_results = pd.DataFrame.from_records([{"id": run.id, 
                                          "RMSE": run.get_metrics().get('RMSE'), 
                                          'R2': run.get_metrics().get('R2')} 
                                         for run in experiment.get_runs() 
                                         if run.get_metrics().get('RMSE') is not None])

display(run_results[['id', 'RMSE', 'R2']])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Select Run with Model to deploy
# MAGIC 
# MAGIC Each time we ran the models, we stored a zip file with the trained model in AML. We can now retrieve the trained model of the particular run that we want to deploy. We'll copy the relevant `id` from above and retrieve the Run object.

# COMMAND ----------

best_run_id = '6d670807-6477-4ea6-a98b-84069c888346'
best_run = Run(experiment, best_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Download the model locally and unzip

# COMMAND ----------

# First, let's list the filenames
best_run.get_file_names()

# COMMAND ----------

# In this case, we're looking for the 'outputs/{MODEL_NAME}Model.zip' file
import shutil
import os

saved_model_name = 'model.zip' # Saved model filename in AML Service - leave out the 'outputs/' folder at the beginning
aml_location = 'outputs/{0}'.format(saved_model_name)

user_folder = '/dbfs/tmp/{0}/'.format(user_name)

# Create objects with folder names
temporary_zip_path = os.path.join(user_folder, 'trained_model_zip') # Folder to zipped model file
temporary_model_path = os.path.join(user_folder, 'trained_model')   # Folder for unzipped model files

# Make the temporary directories
os.makedirs(temporary_zip_path, exist_ok = True)
os.makedirs(temporary_model_path, exist_ok = True)

# Download the zip file from AML service to the 'trained_model_zip' folder
best_run.download_file(aml_location, temporary_zip_path)

# Unpack archive to the 'trained_model' folder
shutil.unpack_archive(os.path.join(temporary_zip_path, saved_model_name), temporary_model_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the model to AML service
# MAGIC 
# MAGIC In order to deploy a model in AML - we will 'register' a model with the service. This allows us for model traceability in the service. 
# MAGIC 
# MAGIC There are 3 different pieces of deploying an AML model.
# MAGIC 1. `Model` objects store the model files for scoring and metadata about the model
# MAGIC 1. `Image` objects refer to Docker images that were built with a registered `model`
# MAGIC 1. `Deployment` objects are places where the model is deployed. The include:
# MAGIC   1. `Services` - Web services that are deployes
# MAGIC   1. `IoT Edge Modules` - Modules that are deployed via Azure IoT Edge.
# MAGIC 
# MAGIC Stated another way:
# MAGIC >  A `Model` is deployed in an `Image` to a `Deployment`

# COMMAND ----------

import os
# Change the current working directory to the `user_folder`
os.chdir(user_folder)

from azureml.core.model import Model

registered_model_name = 'duration_prediction_{0}'.format(user_name)

model = Model.register(workspace=ws, 
                       model_path='./trained_model',
                       model_name=registered_model_name,
                       description='Predict NYC Taxi Trip duration given distance and other metrics',
                       tags={'model_framework': 'PySpark'}
                      )

# COMMAND ----------

# MAGIC %md
# MAGIC It's that easy. Our model is now "registered" to our workspace. If we overwrite this model by running that same register command above (using the same name) the service will store and track another model version
# MAGIC 
# MAGIC ### Create a deployment image
# MAGIC 
# MAGIC Next, we'll create a deployment image. An image requires a few different things to deploy:
# MAGIC 1. A Conda dependencies file. We'll use the `CondaDependencies` object to create that.
# MAGIC 1. A scoring file. This file must have 2 methods:
# MAGIC   1. `init()` method takes no arguments and loads the model into memory
# MAGIC   1. `run()` method is passed the JSON object sent to be scored and returns a ___JSON serializable___ object (such as a Python dict)

# COMMAND ----------

# Start with the Conda Dependencies file
from azureml.core.conda_dependencies import CondaDependencies 

conda_file_path = os.path.join(user_folder, 'conda_dependencies.yml')

# For our model, we don't need any other packages, but there are 
# a few listed here as examples
conda_dep = CondaDependencies.create(conda_packages=['pandas']) 

# We can also add pip packages. Again, this isn't needed, but we're
# showing as an example
conda_dep.add_pip_package('utilitime')

with open(conda_file_path,"w") as file_obj:
    file_obj.write(conda_dep.serialize_to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Show Conda File contents

# COMMAND ----------

# Let's look at what is in the Conda File
with open(conda_file_path,"r") as file_obj:
    print(file_obj.read())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Create and test `score_model.py`

# COMMAND ----------

score_file_path = os.path.join(user_folder, 'score_model.py')

score_file = """
# Modified from https://github.com/Azure/MachineLearningNotebooks/blob/master/how-to-use-azureml/azure-databricks/amlsdk/deploy-to-aci-04.ipynb

import json
 
def init():
    # One-time initialization of PySpark and predictive model
    import pyspark
    from azureml.core.model import Model
    from pyspark.ml import PipelineModel
       
    global trainedModel
    global spark
 
    # Instantiate spark object
    spark = pyspark.sql.SparkSession.builder.appName("TripDurationInferenceRunner").getOrCreate()
    
    # Load model from Azure ML Service
    model_name = "{model_name}"
    model_path = Model.get_model_path(model_name)
    trainedModel = PipelineModel.load(model_path)
    
    ###########################################################################################################
    #     MONITORING EXAMPLE                                                                                  #
    #         AML also includes tools to monitor our deployed model by saving inputs, etc.                    #
    ###########################################################################################################
    #  global inputs_dc, prediction_dc                                                                        #
    #                                                                                                         #
    #  from azureml.monitoring import ModelDataCollector                                                      #
    #                                                                                                         #
    #  # this setup will help us save our inputs under the "inputs" path in our Azure Blob                    #
    #  inputs_dc = ModelDataCollector(model_name=model_name, identifier="inputs")                             #
    #                                                                                                         #
    #  # this setup will help us save our predictions under the "predictions" path in our Azure Blob          #
    #  prediction_dc = ModelDataCollector(model_name, identifier="predictions", feature_names=["prediction"]) #
    ###########################################################################################################
    
def run(input_json):
    # If the model failed to rehydrate from some reason, display the issues back to the user calling the model
    if isinstance(trainedModel, Exception):
        return json.dumps({{"trainedModel":str(trainedModel)}})
      
    try:
        # Create sparkContext
        sc = spark.sparkContext
        
        # Transform the data sent to a Spark Dataframe (in order to score)
        input_data = json.loads(input_json)
        
        # In order to read JSON, spark expects a list of JSON objects
        # Therefore, if only 1 row was sent as a JSON object, then convert the row to a list
        if type(input_data) != list:
          input_data = [input_data]
        
        input_rdd = sc.parallelize(input_data)
        input_df = spark.read.json(input_rdd)
              
        # Compute prediction
        prediction = trainedModel.transform(input_df)
        predictions = prediction.collect()
 
        #Get each scored result
        preds = [str(x['prediction']) for x in predictions]
        
        #################################
        #        IF MONITORING          #
        #################################
        #  inputs_dc.collect(input_df)  #
        #  prediction_dc.collect(preds) #
        #################################
        
        if len(preds) == 1:
          preds = preds[0]
          
        return preds
        
    except Exception as e:
        result = str(e)
        return result
    
""".format(model_name=registered_model_name)

# To test our score file, we can execute the Python string and test the run function
exec(score_file)

with open(score_file_path, 'w') as file_obj:
  file_obj.write(score_file)

# COMMAND ----------

# As we did with the Conda File, we can show the contents of the score_model.py file
# with open(score_file_path, 'r') as file_obj:
#   print(file_obj.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Test `run()` function

# COMMAND ----------

from pyspark.ml import PipelineModel
import json

# Load the model into the global variable trainedModel
global trainedModel

# We need to clip off the "/dbfs" at the start of the temporary_model_path
model_dbfs_path = temporary_model_path.lstrip("/dbfs")

trainedModel = PipelineModel.load(model_dbfs_path)

# COMMAND ----------

# Next, let's take a few rows from our test dataset
trainDF, validDF, testDF = get_train_test_valid_data()

# Only these columns are required
required_columns = ['trip_distance', 'is_rush_hour_flag', 'is_weekend_flag', 'pickup_hour', 'taxi_type', 'pickup_borough', 'pickup_zone', 'dropoff_borough', 'dropoff_zone']

# Select 3 rows from our testDF and convert to JSON
json_records = testDF.select(required_columns).limit(3).toJSON().collect()

# Test run() function
print("The result of run() function (locally) using:")
print("     * a single JSON object is: ", run(json_records[0]))
print("     * a list of JSON objects is: ", run(json.dumps([json.loads(rec) 
                                                            for rec 
                                                            in json_records])))


# COMMAND ----------

# MAGIC %md
# MAGIC If our `run()` function was writted correctly, we should get values for both the singleton and batch scoring scenarios tested. Now we're ready to deploy create our image.
# MAGIC ### Creating Docker Image

# COMMAND ----------

from azureml.core.image import ContainerImage

# The service cannot deploy a script file that isn't in the same working directory
os.chdir(user_folder)

image_name = 'taxi-duration-image-{0}'.format(user_name.replace('_', '-'))

image_config = ContainerImage.image_configuration(execution_script='score_model.py',
                                                  runtime='spark-py',
                                                  conda_file=conda_file_path,
                                                  tags={'dataset': 'nyc-taxi', 
                                                        'creator': user_name},
                                                  description="Image to predict duration of taxi trip")

image = ContainerImage.create(workspace=ws,
                              name=image_name,
                              models=[model],
                              image_config=image_config)

image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploying to Azure Container Instance (ACI)
# MAGIC In this workshop, we'll be deploying to ACI. This might not be suitable for production deployments - instead you should consider deploying to [Azure Kubernetes Service (AKS)](https://docs.microsoft.com/en-us/azure/aks/) for production.

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice
service_name = 'taxi-aci-{0}'.format(user_name.replace('_', '-'))

aci_config = AciWebservice.deploy_configuration(cpu_cores=1,
                                                memory_gb=1,
                                                tags={'creator': user_name},
                                                auth_enabled=True)

# COMMAND ----------

aci_service = Webservice.deploy_from_image(workspace=ws,
                                       name=service_name,
                                       image=image,
                                       deployment_config=aci_config)

aci_service.wait_for_deployment(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Updating the image:
# MAGIC If you've already deployed the service once and need to deploy an updated image, you can use:
# MAGIC ```python
# MAGIC aci_service update(image=image)
# MAGIC aci_service.wait_for_deployment(show_output=True)```
# MAGIC 
# MAGIC This will maintain the keys and scoring URI - when allows you to version up your model without changing where the model in being used.

# COMMAND ----------

print("######################",
      "# DEPLOYMENT DETAILS #",
      "######################",
      "",
      "Scoring URI:",
      aci_service.scoring_uri,
      "Bearer Tokens:",
      aci_service.get_keys(),
      sep="\n")

# COMMAND ----------

# Test the service with the same rows we used above
print("The result of run() function (on ACI) using:")
print("     * a single JSON object is: ", aci_service.run(json_records[0]))
print("     * a list of JSON objects is: ", aci_service.run(json.dumps([json.loads(rec)
                                                                        for rec
                                                                        in json_records])))

# COMMAND ----------

# We can also run a test using the Python Requests library
# which will communicate with the REST API directly and not
# through the 
import requests

result_set = requests.post(url=aci_service.scoring_uri, 
                           json=[json.loads(rec)
                                 for rec
                                 in json_records], 
                           headers={'Authorization': "Bearer {0}".format(aci_service.get_keys()[0])})
print(result_set.json())

# COMMAND ----------

# Delete service - this will free up any ACI resources
# aci_service.delete()