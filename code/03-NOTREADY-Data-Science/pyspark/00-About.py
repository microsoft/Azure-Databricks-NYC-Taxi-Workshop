# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # NB! THIS FOLDER HAS NOT BEEN PREPARED FOR AWS! Needs some work
# MAGIC
# MAGIC ![](https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/raw/master/images/4.png)
# MAGIC
# MAGIC # About this Tutorial #  
# MAGIC The goal of this tutorial us to help you understand the capabilities and features of Azure Databricks Spark MLlib for machine learning (ML) and how to leverage [Azure Machine Learning Service](https://docs.microsoft.com/en-us/azure/machine-learning/service/) with Azure Databricks.  
# MAGIC
# MAGIC ##### We will use the publicly available [NYC Taxi Trip Record](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) from the data engineering portion of the workshop. 
# MAGIC
# MAGIC The end-to-end process for building and refining a machine learning model can be quite involved. The major steps inlcude the following:
# MAGIC - Acquiring the data from a data source.
# MAGIC - Preprocessing the data
# MAGIC - Explore the data to see if there is any correlation between the data attributes 
# MAGIC - Transforming the data (Feature Transformation) so it is optimized for consumption by Spark MLlib for model training. Note that transformation can itself involve multiple steps, with each step transformation different parts of the data in different ways
# MAGIC - Selecting an algorithm to train the model.
# MAGIC - Evaluating the efficiency (sometimes called performance of the model)
# MAGIC - Refining the model for better performance. Model refinement can be done in multiple ways. You can change the parameters that define the model or change the algorithm altogether.
# MAGIC
# MAGIC Building high-performance models is often an iterative, trial-and-error process. Oftenn times all of the above steps may have to be repeated multiple times before you arrive at the optimal model.
# MAGIC
# MAGIC ##### In this tutorial you will be performing the following tasks: #####
# MAGIC
# MAGIC  
# MAGIC 0. Pre-process the data. 
# MAGIC 0. Use the built-in Correlation function to find correlation between columns of the dataset.
# MAGIC 0. Perform the following transformations:
# MAGIC    - Indexing
# MAGIC    - Encoding
# MAGIC    - Assembling into vectors
# MAGIC    - Normalizing the vectors
# MAGIC    
# MAGIC    
# MAGIC    You will see how the data is being trasformed after each step.
# MAGIC 0. Build a model using ** Liner Regression ** The model will predict the ** the duration of a trip** based on pickup time, pickup location and dropoff location.
# MAGIC 0. ** Evalaute ** the efficiency of the model using the built ** Root Mean Squared Error ** evaluation function.
# MAGIC 0. Try to build a better model using the ** Random Forest ** algorithm.
# MAGIC 0. Use the ** K-means ** algorithm to see if there are any natural ** clusters ** in ** duration and trip distance ** 
# MAGIC 0. Visualize the clusters 

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Machine Learning Services
# MAGIC In this lab, we'll be using Azure Machine Learning Service to track experiments and deploy our model as a Rest API via [Azure Container Instances](https://docs.microsoft.com/en-us/azure/container-instances/) for test purposes, but in production you would likely use [Azure Kubernetes Service (AKS)](https://docs.microsoft.com/en-us/azure/aks/).
# MAGIC
# MAGIC ![](https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/raw/master/images/8-machine-learning/1-aml-overview.png)
# MAGIC
# MAGIC Azure Machine Learning service consists of a cloud-based Workspace and can include attached compute and other resources.
# MAGIC
# MAGIC
# MAGIC ![](https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/raw/master/images/8-machine-learning/2-azure-machine-learning-taxonomy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario
# MAGIC
# MAGIC One of the largest taxi companies in New York is looking to predict a trips duration at the time of hail or when a rider calls in to order taxi service. They've looked into other solutions - such as mapping and traffic based APIs, but since this is a Spark and Machine Learning workshop they'll be experimenting using machine learning to predict a trip's duration. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Finding your Azure 'Subscription ID'
# MAGIC In the 99-Shared-Functions-and-Settings notebook, you're asked to enter you Azure Subscription ID (among other things.)
# MAGIC
# MAGIC You can see how to find your subscription id here:
# MAGIC ![](https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/raw/master/images/8-machine-learning/3-find-azure-subscription.gif)
