# 01. Azure Event Hub - Lab instructions

In this lab module - we will learn to publish/consume events from Azure Event Hub with Spark Structured Streaming.  The source is the curated crimes dataset in DBFS, and the sink is DBFS in Delta format.<br>

![1-aeh-preview](../../../images/2-aeh/preview.png)

<br>
<hr>
<br>

## Unit 1. Provisioning and configuring
### 1.1. Provision Event Hub

![1-aeh](../../../images/2-aeh/1.png)
<br>
<hr>
<br>

![3-aeh](../../../images/2-aeh/3.png)
<br>
<hr>
<br>

![2-aeh](../../../images/2-aeh/2.png)
<br>
<hr>
<br>

### 1.2. Create consumer group within event hub

![4-aeh](../../../images/2-aeh/4.png)
<br>
<hr>
<br>

![5-aeh](../../../images/2-aeh/5.png)
<br>
<hr>
<br>


### 1.3. Create SAS policy for accessing from Spark

![11-aeh](../../../images/2-aeh/11.png)
<br>
<hr>
<br>


![12-aeh](../../../images/2-aeh/12.png)
<br>
<hr>
<br>


![13-aeh](../../../images/2-aeh/13.png)
<br>
<hr>
<br>

![14-aeh](../../../images/2-aeh/14.png)
<br>
<hr>
<br>


![15-aeh](../../../images/2-aeh/15.png)
<br>
<hr>
<br>
Capture the connection string.

### 1.4. Attach Spark connector library hosted in Maven
This step is performaned on the Databricks cluster.
<br>
The maven coordinates are-<br>
com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.6<br>
Be sure to get the latest from here- https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html#requirements<br>

![16-aeh](../../../images/2-aeh/16.png)
<br>
<hr>
<br>

![17-aeh](../../../images/2-aeh/17.png)
<br>
<hr>
<br>

![18-aeh](../../../images/2-aeh/18.png)
<br>
<hr>
<br>


## Unit 2. Secure credentials
Refer the notebook for instructions.

## Unit 3. Readstream crime data from DBFS and publish events to Azure Event Hub with Spark Structured Streaming
We will read the curated Chicago crimes dataset in DBFS as a stream and pubish to Azure Event Hub using Structured Streaming.
Follow instructions in the notebook and execute step by step.

## Unit 4. Consume events from Azure Event Hub
We will consume events from Azure Event Hub using Structured Streaming and sink to Databricks Delta.  Follow instructions in the notebook and execute step by step.

## Unit 5. Query streaming events
We will create an external table on the streaming events and run queries on it.  Follow instructions in the notebook and execute step by step.



