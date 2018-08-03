GridDB Sample Application for Apache Kafka

# Project Overview
This sample application provides an introduction on how to use GridDB with Kafka. The main idea is to connect a GridDB database to a Kafka cluster so that messages or records received from a Kafka cluster can be received, processed, and eventually stored into GridDB. This project also uses MQTT server (ex. Mosquitto) to receive and send messages. In this application, the MQTT messages represent sensor data being sent to the Kafka cluster to be stored in GridDB. The purpose and advantage of using these messaging brokers is that they allow different programs and processes to communicate and send data to each other.

For this application, an MQTT client program generates random data which can be used to represent sensor readings in a real-world Internet of Things (IoT) system; some examples are volt readings from an electricity sensor or light readings from a light sensor. The MQTT library is used to publish this data to a machine's local MQTT server and from there a MQTT-Kafka connector passes that data to the Kafka cluster where a GridDBSinkConnector and GridDBSinkTask will read and parse that record into a GridDB row to be inserted into a container. A client app that uses the MQTT API will publish APIs that will be received by the MQTT server, which then publishes it to listening MQTT subscribers. The Kafka Source connector is a MQTT subscriber, and publishes the message to a Kafka server. Kafka then publishes the message to its subscribers, one of which is the GridDB Sink connector which then inserts the message into GridDB. Finally, the DataViewer reads the message from GridDB. This project will also show you how to configure the MQTT-Kafka connector to allow for multiple MQTT clients from multiple machines to send data to a single, remote GridDB database through Kafka.

A diagram of the communication flow for an MQTT client on a single machine can be viewed in the image below.

 ![multipleMqttClient](http://griddb.github.io/repositories/samples/images/multipleMqttClient.png "multipleMqttClient")

Once data is successfully being inserted into GridDB, you can use the Data Viewer program to view statistics of all the sensors that are reporting to GridDB. You can view averages, standard deviations, and other aggregations from individual sensors and average readings of certain sensor types in general. In this application, there are 3 sensor types.
- Light Sensor: Records and measures Light and Sound Data
- Watt/Power Sensor: Records and measures Power and Heat Data
- Electricity Sensors: Records and measures Voltage and Amperage Data

To setup this application you will need to have the following components installed and configured on your system:
- GridDB server and Java client
- MQTT server
- Kafka messaging broker and server

When you are ready to test the applications with one or more MQTT clients, you can follow the instructions in the Build and Run Components section. It will teach you how to fully build and run the application and see the power and utility offered of using GridDB with messaging brokers.
# Project Components
This repository includes two comopents.
## GridDB Connector
We developed it based on the FileStream Connector within KafkaÅfs source code. The location of the source code of these components can be found in the src folder. 
## GridDB Data Viewer
The GridDB Data Viewer outputs the data recorded by the GridDB Kafka Sink. The location of the source code of these components can be found in the DataViewer folder. 

## Community

  * Issues  
    Use the GitHub issue function if you have any requests, questions, or bug reports. 
  * PullRequest  
    Use the GitHub pull request function if you want to contribute code.
    You'll need to agree GridDB Contributor License Agreement(CLA_rev1.1.pdf).
    By using the GitHub pull request function, you shall be deemed to have agreed to GridDB Contributor License Agreement.

## License
  
  This GridDB Sample Application source is licensed under the Apache License, version 2.0.
  
## Trademarks
  
  Apache Kafka, Kafka are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
