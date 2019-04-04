# Provisioning Guide for Azure HDInsight Kafka

This section covers provisioning of Azure HDInsight Kafka and configuration required.

From the portal navigate to the resource group you created.

## Provision HDInsight Kafka 
Follow the instructions below to provision Kafka.

![HDI-1](../../images/9-iot/CreateHDI-1.png)


![HDI-2](../../images/9-iot/CreateHDI-2.png)


![HDI-3](../../images/9-iot/CreateHDI-3.png)


![HDI-4](../../images/9-iot/CreateHDI-4.png)


![HDI-5](../../images/9-iot/CreateHDI-5.png)


![HDI-6](../../images/9-iot/CreateHDI-6.png)


![HDI-7](../../images/9-iot/CreateHDI-7.png)


![HDI-8](../../images/9-iot/CreateHDI-8.png)



## Configure Kafka for IP advertising
By default, Zookeeper returns the FQDN of the Kafka brokers to clients - not resolvable by entities outside the cluster.  Follow the steps below to configure IP advertising/to broadcast IP addresses - from Ambari.

![kafka-1](../../images/9-iot/kafka-1.png)

![kafka-2](../../images/9-iot/kafka-2.png)

![kafka-3](../../images/9-iot/kafka-3.png)

![kafka-4](../../images/9-iot/kafka-4.png)

Paste this at the bottom of the kafka-env comfiguration
```sh
# Configure Kafka to advertise IP addresses instead of FQDN
IP_ADDRESS=$(hostname -i)
echo advertised.listeners=$IP_ADDRESS
sed -i.bak -e '/advertised/{/advertised@/!d;}' /usr/hdp/current/kafka-broker/conf/server.properties
echo "advertised.listeners=PLAINTEXT://$IP_ADDRESS:9092" >> /usr/hdp/current/kafka-broker/conf/server.properties
```
![kafka-5](../../images/9-iot/kafka-5.png)


## Configure Kafka to listen on all network interfaces
By default, Zookeeper returns the domain name of the Kafka brokers to clients - not resolvable by entities outside the cluster.  Follow the steps below to configure IP advertising.
```PLAINTEXT://0.0.0.0:9092```
![kafka-6](../../images/9-iot/kafka-6.png)


## Restart the Kafka service from Ambari
![kafka-7](../../images/9-iot/kafka-7.png)

![kafka-8](../../images/9-iot/kafka-8.png)

![kafka-9](../../images/9-iot/kafka-9.png)



## Capture the IP addresses of the brokers
The brokers have names starting with wn*.  Capture their private IP addresses.
![kafka-10](../../images/9-iot/kafka-10.png)


In the author's case, the broker port list is:
10.1.0.7:9092,10.1.0.9:9092,10.1.0.10:9092,10.1.0.14:9092