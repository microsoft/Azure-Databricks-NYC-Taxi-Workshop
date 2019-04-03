# 04. Azure SQL Datawarehouse - Lab instructions

In this lab module - we will learn to integrate with Azure SQL datawarehouse from Spark - batch and with Spark Structured Streaming.  
- **Batch mode:**<br>
The source is the curated crimes dataset in DBFS, and the target is Azure SQL datawarehouse<br>

<img src="../../../images/5-sql-dw/4.png" width="600" height="600">
<br>

- **Streaming mode:**<br>
In the structured streaming lab, the source is Azure Event Hub, and the sink is Azure SQL datawarehouse.<br>

![5-sql-dw](../../../images/5-sql-dw/5.png)
<br>


## A) Configuring the database server
### A1. Firewall settings 
Configure firewall settings as needed; Not required if not needed or completed already in Azure SQL database lab.

### A2. Capture credentials

![1-sql-dw](../../../images/5-sql-dw/1.png)
<br>
<hr>
<br>

![2-sql-dw](../../../images/5-sql-dw/2.png)
<br>
<hr>
<br>

![3-sql-dw](../../../images/5-sql-dw/3.png)
<br>
<hr>
<br>


## B) Lab

### Unit 1. Secure credentials
Refer the notebook for instructions.

### Unit 2. Read/write in batch mode 
In this unit, we will read data in DBFS and write to Azure SQL datawarehouse over JDBC.<br>
We will learn to write in parallel, and read in parallel, AND auto-create table and write, append and overwrite table.

### Unit 3. Publish to Azure Event Hub
We will re-run the event publisher from the event hub module.

### Unit 4. Consume from Azure Event Hub, sink to Azure SQL database
We will leverage structured streaming to read stream from Azure Event Hub, and sink to Azure SQL datawarehouse.

