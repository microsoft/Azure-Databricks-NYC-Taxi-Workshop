
# Lab guide

## Module 1: Azure Data Services - Integration Primer
[1.  Azure Storage](module-1/00-Azure-Storage-Lab.md)<br>
[2.  Azure Event Hub](module-1/01-Azure-Event-Hub-Lab.md)<br>
3.  Azure HDInsight Kafka<br>
[4.  Azure SQL Database](module-1/03-Azure-SQL-Database-Lab.md)<br> 
[5.  Azure SQL Datawarehouse](module-1/04-Azure-SQL-DW-Lab.md)<br> 
[6.  Azure Cosmos DB](module-1/05-Azure-Cosmos-DB-Lab.md)<br> 
[7.  Azure Data Factory v2](module-1/06-Azure-Data-Factory-v2-Lab.md)<br>
[8.  Azure Key Vault](module-1/07-Azure-Key-Vault-Lab.md)<br> 

## Module 2: Data Engineering - Primer

This module covers a simple data engineering batch pipeline in Spark Scala.
### 2.1. Data copy:
We will use Azure Data Factory v2 to copy data to Azure Blob Storage - staging directory. The instructions are [here.](../3-data-copy-guide/README.md)

### 2.2. Load/parse/persist raw data:

#### 2.2.a. Load reference data:
We will read raw CSV reference datasets (6 of them) in the staging directory (blob storage) and persist to Parquet in the curated information zone.
#### 2.2.b. Load transactional yellow taxi trip data:
We will read raw CSV data in the staging directory (blob storage) and persist to Delta format in the raw information zone.  We will dedupe the data, add some additional columns as a precursor to homogenizing schema across yellow and green taxi trips and across years. 
#### 2.2.c. Load transactional green taxi trip data:
We will read raw CSV data in the staging directory (blob storage) and persist to Delta format in the raw information zone.  We will dedupe the data, add some additional columns as a precursor to homogenizing schema across yellow and green taxi trips and across years. 
<br><br>
2.2.a/b/c can be run in parallel.




