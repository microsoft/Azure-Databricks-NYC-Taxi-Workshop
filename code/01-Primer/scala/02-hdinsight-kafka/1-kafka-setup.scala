// Databricks notebook source
// MAGIC %md 
// MAGIC # Kafka setup for the workshop

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.  Setup to allow Databricks and HDInsight Kafka to communicate

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.1.  Peer the HDInsight Kafka and Databricks Vnets

// COMMAND ----------

// MAGIC %md
// MAGIC 1.  Go to the Databricks workspace, and to the link for peerings, and peer the Databricks workers Vnet to the HDInsight Kafka Vnet.  Capture the resource ID for the workers Vnet onto the clipboard<br>
// MAGIC 2.  Go to the HDInsight Kafka vnet and click on peerings, and peer the Kafka Vnet to the workers Vnet from above

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.2.  Configure Kafka to advertise private IP and configure listener

// COMMAND ----------

// MAGIC %md
// MAGIC Following instructions detailed here-<br>
// MAGIC https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/blob/master/docs/1-provisioning-guide/Provisioning-Kafka.md

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Create Kafka topic

// COMMAND ----------

// MAGIC %md
// MAGIC 1) SSH to the Kafka cluster<br>
// MAGIC Replace with your cluster details.
// MAGIC ```
// MAGIC ssh sshuser@gws-kafka-ssh.azurehdinsight.net
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC 2) Install jq<br>
// MAGIC ```
// MAGIC sudo apt -y install jq
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC 3) Write the cluster name to a variable<br>
// MAGIC ```
// MAGIC read -p "Enter the Kafka on HDInsight cluster name: " CLUSTERNAME<br>
// MAGIC Enter the Kafka on HDInsight cluster name: gws-kafka<br>
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC 4) Capture Zookeeper server list into a variable<br>
// MAGIC ```
// MAGIC export KAFKAZKHOSTS=`curl -sS -u admin -G https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/ZOOKEEPER/components/ZOOKEEPER_SERVER | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2`<br>
// MAGIC Enter host password for user 'admin':
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC 5) Create a Kafka topic<br>
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 3 --topic crimes_topic --zookeeper $KAFKAZKHOSTS
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.  Capture details you need to connect from Spark to Kafka

// COMMAND ----------

// MAGIC %md
// MAGIC From Ambari, navigate to the hosts page and capture the broker IP addresses - the broker names start with wn*<br>
// MAGIC This is detailed here-<br>
// MAGIC https://github.com/Microsoft/Azure-Databricks-NYC-Taxi-Workshop/blob/master/docs/1-provisioning-guide/Provisioning-Kafka.md
// MAGIC 
// MAGIC In the author's case - the broker list, with port (9092) was:<br>
// MAGIC 10.7.0.5:9092,10.7.0.6:9092,10.7.0.10:9092