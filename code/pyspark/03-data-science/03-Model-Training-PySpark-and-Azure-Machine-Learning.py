# Databricks notebook source
# MAGIC %md 
# MAGIC # Supervised learning with Azure Machine Learning Experiment Tracking 
# MAGIC ### Use case: Trip duration prediction 
# MAGIC ##### Model training with Spark MLlib
# MAGIC The goal of this tutorial is build on the data engineering lab and learn how to -
# MAGIC 1.  Create a Spark ML Pipeline - to consistently apply transformations and models
# MAGIC 1.  Train model with two algorithms - Logistic Regression and Random Forest Regression
# MAGIC 1.  Evaluate model across the two algorithms
# MAGIC 1.  Train Gradient Boosted Regressor model
# MAGIC 1.  Persist model for scoring at a later time in Azure Machine Learning Service

# COMMAND ----------

# MAGIC %run ./99-Shared-Functions-and-Settings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Login to Azure Machine Learning Workspace

# COMMAND ----------

from azureml.core import Workspace, Experiment, Run
from azureml.core.authentication import InteractiveLoginAuthentication

up = InteractiveLoginAuthentication()
up.get_authentication_header()

ws = Workspace(**AZURE_ML_CONF, auth=up)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import shutil
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull in the data from the last notebook and split into training, validation and test datasets.
# MAGIC 
# MAGIC Here, we're selecting a very small percentage of the data - 0.5% for training and 0.1% for validation and test.
# MAGIC 
# MAGIC This is simply due to the amount of data we have and that we're running a workshop. We don't want to wait 4 hours to get results. As we can see - there are still 671,000 rows in our training data!
# MAGIC 
# MAGIC Here we're also setting a random seed - to make the results reproducible. It's not necessary, but it will allow you to compare between two different models much better. 

# COMMAND ----------

trainDF, validDF, testDF = get_train_test_valid_data()

print("There are {:,} rows in our training dataset".format(trainDF.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing Data Transformation Pipelines
# MAGIC PySpark ML models expect to get data in a very specific way:
# MAGIC * All labels should be in a single column 
# MAGIC   * By default models expect the column to be called `labels` 
# MAGIC * All features should be in a single column
# MAGIC   * This is usually a vector column
# MAGIC   * All values must either be a numeric or numeric vector
# MAGIC   * By default models expect the column to be called `features`
# MAGIC   
# MAGIC We'll first start by creating the data transformation pipeline seperate from the machine learning models. This will reduce the time that training different models will take. We'll combine the trained model back with this pipeline later.

# COMMAND ----------

# First we'll pick the columns that we want in our dataset. We'll seperate the columns based on what type of processing they need.
# The list approach used here will allow us to add and subtract columns easier as we're building the model.

continuous_numeric_cols = ['trip_distance', 'is_rush_hour_flag', 'is_weekend_flag']

ohe_numeric_cols = ['pickup_hour']

string_cols = ['taxi_type', 'pickup_borough', 'pickup_zone',
               'dropoff_borough', 'dropoff_zone']


# COMMAND ----------

# MAGIC %md
# MAGIC ##### String Indexing using [pyspark.ml.feature.StringIndexer](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# MAGIC In order to one-hot encode columns, PySpark requires the columns to be "String Indexed." A string indexer will take each value of a string and map it to an integer. The model object retains the index.
# MAGIC 
# MAGIC For example - if we had a list of animals, String Indexing might change it like this:
# MAGIC 
# MAGIC | id | animal | index |  
# MAGIC |----|--------|-------|
# MAGIC | 1  | dog    | 0     |
# MAGIC | 2  | cat    | 1     |
# MAGIC | 3  | dog    | 0     |
# MAGIC | 4  | cow    | 2     |
# MAGIC | 5  | cow    | 2     |

# COMMAND ----------

# Create String Indexer layers 
string_indexers = [StringIndexer(inputCol=col_name, 
                                 outputCol="{0}_index".format(col_name), 
                                 handleInvalid="skip", 
                                 stringOrderType='frequencyDesc') for col_name in string_cols]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### One Hot Encoding using [pyspark.ml.feature.OneHotEncoderEstimator](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoderEstimator)
# MAGIC One hot encoding (also called 'dummy variables') allows you to transform categorical variables to be used in building models.
# MAGIC 
# MAGIC In the example above - there is no _numerical_ relationship between dogs, cats and cows... e.g. a cow isn't worth 2 cats. However, if we pass the index value directly, that's essentially how the model might interpret that data. Instead, we need to one-hot encode the data.
# MAGIC 
# MAGIC Going back to our example earlier, we create 3 new columns - one each for dogs, cats, and cows. Then, we set one of these values to '1' and leave the others set to '0'.
# MAGIC 
# MAGIC | id | animal | index | dog | cat | cow |
# MAGIC |----|--------|-------|-----|-----|-----|
# MAGIC | 1  | dog    | 0     | 1   | 0   | 0   |
# MAGIC | 2  | cat    | 1     | 0   | 1   | 0   |
# MAGIC | 3  | dog    | 0     | 1   | 0   | 0   |
# MAGIC | 4  | cow    | 2     | 0   | 0   | 1   |
# MAGIC | 5  | cow    | 2     | 0   | 0   | 1   |
# MAGIC 
# MAGIC This is the 'theory' behind one-hot encoding. In practice, however, Spark doesn't store all of these 0 values - because that would be a lot of data. Imagine if we had all 5,513 known species of mammals in our dataset. That would mean for each row, we'd store 5,512 '0's and 1 '1'.
# MAGIC 
# MAGIC Instead, Spark uses SparseVectors. Instead of storing all 5,514 possible values, it stores only the indices for the columns where the value is not 0. 

# COMMAND ----------

# Determine the input and output names for one-hot-encoding
ohe_inputCols = ["{0}_index".format(col) for col in string_cols] + ohe_numeric_cols
ohe_outputCols = ["{0}_ohe".format(col) for col in string_cols] + ["{0}_ohe".format(col) for col in ohe_numeric_cols]

ohe = OneHotEncoderEstimator(inputCols=ohe_inputCols, 
                             outputCols=ohe_outputCols)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Vector Assembly using [pyspark.ml.feature.VectorAssembler](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)
# MAGIC 
# MAGIC As noted above, PySpark requires that all features are contained in a single Vector. That's what the 'VectorAssembler' will do for us. Here, we specify all of the feature columns that we're interested in using and then assemble them into a single vector named `features`
# MAGIC 
# MAGIC Once we've done that - we 'fit' our Pipeline. That is - we create the StringIndexer lookup values and one hot encoding. We do this in a pipeline - because we want to consistently apply these transformations each and every time we score the model.

# COMMAND ----------

# Finally, convert these columns to a vector (along with other columns)
feature_cols = continuous_numeric_cols + ohe_outputCols

vectorizer = VectorAssembler(inputCols=feature_cols, outputCol='features')

feature_pipeline = Pipeline(stages=[*string_indexers, ohe, vectorizer])

# Fit the featurizer layers of the pipeline.
# Fitting seperate from the model to streamline 
# execution of many models by skipping these steps
# with future models
feature_model = feature_pipeline.fit(trainDF)

# COMMAND ----------

trainDF_transformed = feature_model.transform(trainDF)

# Capture valid_stddev to calculate normalized RMSE
valid_stddev = validDF.select(stddev_samp(col("duration_minutes")).alias("stddev")).head(1)[0]['stddev']

trainDF_transformed = trainDF_transformed.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Azure Machine Learning Experiment
# MAGIC 
# MAGIC The Experiment object will contain 1-to-many 'Runs'. Each of these Runs can capture track logged data, images, results, and/or trained models. 
# MAGIC 
# MAGIC The Run objects can be a submitted `*.py` script or it can be submitted via an interactive notebook session.
# MAGIC 
# MAGIC We'll also create an evaluator to that will calculate performance metrics of the models.

# COMMAND ----------

# Use the experiment name from the ./99-Shared-Functions-and-Settings notebook
experiment = Experiment(ws, pyspark_experiment_name)

# Create evaluator object to assess model performance
evaluator = RegressionEvaluator(labelCol='duration_minutes')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Linear Regression Model
# MAGIC The first model that we'll try is a Linear Regression model. 

# COMMAND ----------

run = experiment.start_logging()

print("==============================================")
print("                  RUN STATUS                  ")
print("==============================================")

#######################
# SET HYPERPARAMETERS #
#######################

model_name = 'LinearRegression'
maxIters = 10
regParam = 0.5

#######################
# LOG HYPERPARAMETERS #
#######################

run.log("Model Name", model_name)
run.log("Max Iterations", maxIters)
run.log("Regularization Rate", regParam)
run.log_list("Feature Columns", feature_cols)

###############
# TRAIN MODEL #
###############

print("  * Training {0} model".format(model_name))
# Instantiate New LinearRegression Object
lr = LinearRegression(featuresCol='features', labelCol='duration_minutes', maxIter=maxIters, regParam=regParam, solver="auto")

# Train model on transformed training data
lr_model = lr.fit(trainDF_transformed)

lr_full_model = feature_model.copy()
lr_full_model.stages.append(lr_model)

print("  * Model trained, scoring validation data")
# Run the full model (feature steps and trained model)
validation_scored = lr_full_model.transform(validDF)

#####################
# MODEL PERFORMANCE #
#####################

print("  * Calculating performance metrics")
# Calculate Regression Performance
rmse = evaluator.evaluate(validation_scored, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(validation_scored, {evaluator.metricName: "r2"})
nrmse = rmse / valid_stddev

###################
# LOG PERFORMANCE #
###################

print("  * Logging performance metrics")
# Log Performance
run.log('RMSE', rmse)
run.log("R2", r2)
run.log("NRMSE", nrmse)

##########################
# GENERATE RESIDUAL PLOT #
##########################

print("  * Generating residual plot")
# Plot residual and add to Azure ML Experiment
resid_plot = plot_residuals(validation_scored)

print("  * Logging residual plot")
run.log_image("Residual Plot", plot=resid_plot)

######################
# PERSIST MODEL DATA #
######################
print("  * Persisting model to Azure Machine Learning")
model_name = "LinearRegressionModel"
model_zip = model_name + ".zip"
model_local_dbfs = os.path.join("/dbfs/tmp/",  user_name, model_name)

lr_full_model.save(os.path.join("/tmp/", user_name, model_name))
shutil.make_archive(model_name, 'zip', model_local_dbfs)
run.upload_file("outputs/{0}".format(model_zip), model_zip)

# Delete saved models on cluster - since they're in Azure Machine Learning
shutil.rmtree(model_local_dbfs)
os.remove(model_zip)

run.complete()

# Display Information In This Notebook
print()
print("================")
print("    RESULTS     ")
print("================")

print("RMSE: \t {0:.5}".format(rmse),
    "NRMSE: \t {0:.5}".format(nrmse),
    '',
    "R2: \t {0:.5}".format(r2),
    sep='\n')

# COMMAND ----------

display(resid_plot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Forest Regressor

# COMMAND ----------

run = experiment.start_logging()

print("==============================================")
print("                  RUN STATUS                  ")
print("==============================================")

#######################
# SET HYPERPARAMETERS #
#######################

random_seed = 25706

model_name = 'RandomForestRegressor'
maxDepth = 11
maxBins = 45
numTrees = 20
subsamplingRate = 0.3

#######################
# LOG HYPERPARAMETERS #
#######################

run.log("Random Seed", random_seed)
run.log("Model Name", model_name)
run.log("Max Depth", maxDepth)
run.log("Max Bins", maxBins)
run.log("Number of Trees", numTrees)
run.log('Subsampling Rate', subsamplingRate)
run.log_list("Feature Columns", feature_cols)

###############
# TRAIN MODEL #
###############

print("  * Training {0} model".format(model_name))
# Instantiate New RandomForestRegressor Object
rf = RandomForestRegressor(labelCol='duration_minutes', maxDepth=maxDepth, maxBins=maxBins, impurity='variance', 
                           subsamplingRate=1.0, seed=random_seed, numTrees=numTrees, featureSubsetStrategy='auto')

# Train model on transformed training data
rf_model = rf.fit(trainDF_transformed)

rf_full_model = feature_model.copy()
rf_full_model.stages.append(rf_model)

print("  * Model trained, scoring validation data")
# Run the full model (feature steps and trained model)
validation_scored = rf_full_model.transform(validDF)

#####################
# MODEL PERFORMANCE #
#####################

print("  * Calculating performance metrics")
# Calculate Regression Performance
rmse = evaluator.evaluate(validation_scored, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(validation_scored, {evaluator.metricName: "r2"})
nrmse = rmse / valid_stddev

###################
# LOG PERFORMANCE #
###################

print("  * Logging performance metrics")
run.log('RMSE', rmse)
run.log("R2", r2)
run.log("NRMSE", nrmse)

##########################
# GENERATE RESIDUAL PLOT #
##########################

print("  * Generating residual plot")
# Plot residual and add to Azure ML Experiment
resid_plot = plot_residuals(validation_scored)

print("  * Logging residual plot")
run.log_image("Residual Plot", plot=resid_plot)

######################
# PERSIST MODEL DATA #
######################
print("  * Persisting model to Azure Machine Learning")
model_name = "RandomForestRegressionModel"
model_zip = model_name + ".zip"
model_local_dbfs = os.path.join("/dbfs/tmp/",  user_name, model_name)

rf_full_model.save(os.path.join("/tmp/", user_name, model_name))
shutil.make_archive(model_name, 'zip', model_local_dbfs)
run.upload_file("outputs/{0}".format(model_zip), model_zip)

# Delete saved models on cluster - since they're in Azure Machine Learning
shutil.rmtree(model_local_dbfs)
os.remove(model_zip)

run.complete()

# Display Information In This Notebook
print()
print("================")
print("    RESULTS     ")
print("================")

print("RMSE: \t {0:.5}".format(rmse),
      "NRMSE: \t {0:.5}".format(nrmse),
      '',
      "R2: \t {0:.5}".format(r2),
      sep='\n')

# COMMAND ----------

display(resid_plot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gradient Boosted Tree Regressor
# MAGIC 
# MAGIC ___Your turn!___
# MAGIC 
# MAGIC Train a new Gradient Boosted Tree regressor - choose some hyperparameters (HINT: Check out the PySpark API docs for [GBTRegressor](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.GBTRegressor) to know which hyperparamters are supported.)

# COMMAND ----------

run = experiment.start_logging()

random_seed = 25706

model_name = 'GBTRegressor'

###############
##   TODO    ##
###############
run.log_list("Columns", trainDF_transformed.columns)
# Log Hyperparameters

print("Training {0} model".format(model_name))

# Instantiate New GBTRegressor Object


# Train model on transformed training data


###############
## END TODO  ##
###############

print("Model trained, scoring validation data")

###############
##   TODO    ##
###############

# Add trained model to Overall pipeline


# Score model against validation data

validation_scored = 

###############
## END TODO  ##
###############


print("Calculating performance metrics")
# Calculate Regression Performance
rmse = evaluator.evaluate(validation_scored, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(validation_scored, {evaluator.metricName: "r2"})
nrmse = rmse / valid_stddev

print("Logging performance metrics")
# Log Performance

###############
##   TODO    ##
###############

# Log RMSE, R2 and NRMSE to Azure ML Workspace

###############
## END TODO  ##
###############

print("Generating residual plot")
# Plot residual and add to Azure ML Experiment
resid_plot = plot_residuals(validation_scored)

print("Logging residual plot")
run.log_image("Residual Plot", plot=resid_plot)

#Persist
print("  * Persisting model to Azure Machine Learning")
model_name = "GBTRegressionModel"
model_zip = model_name + ".zip"
model_local_dbfs = os.path.join("/dbfs/tmp/",  user_name, model_name)

###############
##   TODO    ##
###############
[ENTER MODEL NAME HERE].save(os.path.join("/tmp/", user_name, model_name))

###############
## END TODO  ##
###############

shutil.make_archive(model_name, 'zip', model_local_dbfs)
run.upload_file("outputs/{0}".format(model_zip), model_zip)

# Delete saved models on cluster - since they're in Azure Machine Learning
shutil.rmtree(model_local_dbfs)
os.remove(model_zip)

run.complete()

# Display Information In This Notebook
print()
print("================")
print("    RESULTS     ")
print("================")

print("RMSE: \t {0:.5}".format(rmse),
      "NRMSE: \t {0:.5}".format(nrmse),
      '',
      "R2: \t {0:.5}".format(r2),
      sep='\n')

# COMMAND ----------

###############
##   TODO    ##
###############

# Display the residual plot

###############
## END TODO  ##
###############