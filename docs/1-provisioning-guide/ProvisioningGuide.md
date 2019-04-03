
# Provisioning Guide

For the workshop, we will need to provision multiple resources/services.  Many of these are for the primer only as labeled below.  The following is a step-by-step provisioning guide.<br>
1. Azure Resource Group
1. Azure Virtual network
1. Azure Blob Storage
1. Azure Databricks
1. Azure Data Lake Storage Gen1 (for the primer only)
1. Azure Data Lake Storage Gen2 (for the primer only)
1. Azure Event Hub (for the primer only)
1. Azure HDInsight Kafka (for the primer only)
1. Azure SQL Database
1. Azure SQL Data Warehouse (for the primer only)
1. Azure Cosmos DB (for the primer only)
1. Azure Data Factory v2 (for the primer only)
1. Azure Key Vault (for the primer only)
1. A Linux VM to use Databricks CLI

**Note**: All resources shoud be **provisioned in the same datacenter**.<br>

## 1. Provision a resource group
Create a resource group called "gws-rg" into which we will provision all other Azure resources.<br>
https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-portal

## 2.  Provision a virtual network (Vnet)
Provision an Azure Vnet in #1. <br>
https://docs.microsoft.com/en-us/azure/virtual-network/quick-create-portal#create-a-virtual-network

## 3.  Provision a blob storage account
Provision an Azure Blob Storage account (gen1) in #1<br>
https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?toc=%2fazure%2fstorage%2fblobs%2ftoc.json

## 4.  Provision Azure Data Lake Store Gen1
(i) Provision an Azure Lake Store Gen 1 account in #1 <br>
https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-portal#create-a-data-lake-storage-gen1-account
<br>
(ii) Create a service principal via app registration<br>
This step is crucial for ADLSGen1, ADLSGen2 and Azure Key Vault<br>
https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal<br>
Capture the Application ID, also referred to as Client ID
<br>
(iii) Go to the app registration/service principal, click on settings and create a key/access token, capture the value - its exposed only temporarily; We will refer to this as access token<br>
(iv) Create a new directory in ADLS Gen1 called gwsroot from the portal<br>
(v) Grant the service principal (SPN) from (ii), super user access to the directory, and child items.<br>
We will mount the ADLS Gen 1 in Databricks with this SPN's credentials.

## 5.  Provision Azure Data Lake Store Gen2
Provision an Azure Lake Store Gen 2 account in #1 - with Hierarchical Name Space (HNS) enabled.  <br>
https://docs.microsoft.com/en-us/azure/storage/data-lake-storage/quickstart-create-account<br>
Lets grant the same service principal name, blob contributor role based access to ADLS Gen 2

## 6.  Provision Azure Databricks
Provision an Azure Databricks premium tier workspace in the Vnet we created in #2, the same resource group as in #1, and in the same region as #1.  Then peer the Dataricks service provisioned Vnet with the Vnet from #2 for access if you are trying out Kafka - otherwise, not required.  We will discuss Vnet injection in the classroom.<br>
[Provision a workspace & cluster](https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal#create-an-azure-databricks-workspace)<br>
[Peering networks](https://docs.azuredatabricks.net/administration-guide/cloud-configurations/azure/vnet-peering.html)<br>
The peering aspect is specific to Kafka and assumed no Vnet injection with Databricks. NOT required unless working with Kafka.

## 7.  Provision Azure Event Hub
Provision Azure Event Hub (standard tier, three throughput units) in #1, and a consumer group.  Set up SAS poliies for access, and capture the credentials required for access from Spark.<br>
https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create

## 8.  Provision Azure HDInsight Kafka
Provision a Kafka cluster in the resource group and Vnet from #2.  Enable Kafka to advertise private IPs, and configure it to listen on all network interfaces. <br>
Provisioning: https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-get-started#create-an-apache-kafka-cluster<br>
Broadcast IP addresses, configure listener: https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising

## 9.  Provision Azure SQL Database
Provision a logical database (Standard S0 - 10 DTUs) server in #1, and an Azure SQL Database within the server.  Configure the firewall to allow our machine to access, and also enable access to the Dataricks Vnet.  Capture credentials for access from Databricks.<br>
https://docs.microsoft.com/en-us/azure/sql-database/sql-database-get-started-portal#create-a-sql-database<br>
We will work on the firewall aspect in the classroom.

## 10.  Provision Azure SQL Datawarehouse
Provision an Azure SQL datawarehouse (gen1, 100 DWU), in #1 in the same database server created in #8.<br>
https://docs.microsoft.com/en-us/azure/sql-data-warehouse/create-data-warehouse-portal#create-a-data-warehouse

## 11.  Provision Azure Cosmos DB
Provision a Cosmos DB account in #1, a database and 3 collections - one for batch load, one for streaming. Provision specs are below.<br>
Database: gws_db (dont provision throughput)<br>
Within gws_db, the following collections:
1.  Name: chicago_crimes_curated_batch; Partition key: /case_id; Throughput: 1000<br>
2.  Name: chicago_crimes_curated_stream; Partition key: /case_id; Throughput: 1000<br>
3.  Name: chicago_crimes_curated_stream_aggr; Partition key: /case_type; Throughput: 1000<br>
Complete step 1, 2 and 3 from the link - https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-get-started

## 12.  Provision Azure Data Factory v2
Provision Azure Data Factory v2 instance in #1.<br>
Complete steps 1-9 from the link - https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal#create-a-data-factory

## 13.  Provision Azure Key Vault
Provision Azure Key Vault instance in #1.<br>
https://docs.microsoft.com/en-us/azure/key-vault/quick-create-portal<br>

## Installation of the Databricks CLI
Provision a Linux VM (Ubuntu Server 18.10) on Azure from the portal.<br>
We will install the Databricks CLI on this VM - due to the ephemeral nature of the cloud bash shell.<br>
