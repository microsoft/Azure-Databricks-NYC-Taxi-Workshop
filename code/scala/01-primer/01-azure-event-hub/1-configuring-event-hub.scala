// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Event Hub: Configuration
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC 1.  Create an event hub
// MAGIC 2.  Create a consumer group in the event hub
// MAGIC 3.  Create a SAS policy for access
// MAGIC 4.  Capture connnection string for securing
// MAGIC 5.  Attach the Spark-EventHub connector to the cluster

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.0. Azure Event Hub creation and configuration
// MAGIC ##### Prerequisite:
// MAGIC Azure Event Hub service should be provisioned<br>
// MAGIC https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create<br>
// MAGIC 
// MAGIC #### 1.0.1. Create an Azure Event Hub called crimes_aeh
// MAGIC Refer the provisioning documentation listed under pre-requisite, or follow your instructor
// MAGIC 
// MAGIC #### 1.0.2. Create an Azure Event Hub consumer group 
// MAGIC Create an event hub instance for the event hub from 1.0.1 called crimes_chicago_cg
// MAGIC 
// MAGIC #### 1.0.3. Create a SAS policy for the event hub with all checkboxes selected
// MAGIC This is covered in the link for provisioning in the prequisite, or follow your instructor.
// MAGIC 
// MAGIC #### 1.0.4. From the portal, capture the connection string for the event hub
// MAGIC We will need this to write/read off of the event hub

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.0. Spark-Event Hub connector configuration
// MAGIC Refer documentation below to complete this step or follow your instructor-<br>
// MAGIC https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html#requirements