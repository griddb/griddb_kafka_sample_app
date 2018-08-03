/*
 * Copyright (c) 2017 TOSHIBA Digital Solutions Corporation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * This file is modified by TOSHIBA Digital Solutions Corporation.
 *
 * This file is based on the file FileStreamSinkTask.java
 * in https://github.com/apache/kafka/archive/0.10.2.1.zip
 * (Apache Kafka 0.10.2.1).
 */

package net.griddb.connect.griddb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;

import java.util.EnumSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Date;
import net.arnx.jsonic.JSON;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * GridDBSinkTask writes records to a GridDB TimeSeries Container
 */
public class GridDBSinkTask extends SinkTask {
	private static final Logger log = LoggerFactory.getLogger(GridDBSinkTask.class);

	private GridStore store;
	private static final String CONTAINER_NAME = "sample";
	private static final String SENSOR_TYPE_NAME = "Sensor_Types"; // Default name of sensor-type container

	// This class represents the schema for the sensor-type container which records all the sensor Ids and types
	// of sensor devices that have made and submitted readings to GridDB through Kafka

	// The columns are time-inserted/modified a @RowKey of the sensor-id and its type (i.e. volts, light etc)
	static class SensorType {
		@RowKey String id;
		Date time;
		String type;

		public void setId(String id){
			this.id = id;
		}

		public void setType(String type){
			this.type = type;
		}

		public void setTime(Date time){
			this.time = time;
		}

		public String toString(){
			return "Timestamp: " + time + " Sensor-Type: " + type + " Sensor-id: " + id;
		}
	}


	// The following three classes represent different schemas for Data-log containers
	// These data-log containers are named or identified by their sensor-id  
	// and have Timestamp RowKeys

	// Measures light and sound
	static class LightSensor{
		@RowKey Date time;
		double light;
		double sound;
		public void setTime(Date time){
			this.time = time;
		}
		public void setLight(double light){
			this.light = light;
		}
		public void setSound(double sound){
			this.sound = sound;
		}
		public String toString() {
			return "Timestamp: "+time +" Light: "+ light +" Sound: "+sound;
		}
	}
	
	// Measures power (watts) and heat
	static class WattSensor {
		@RowKey Date time;
		double watts;
		double heat;
		public void setTime(Date time){
			this.time = time;
		}
		public void setWatts(double watts){
			this.watts = watts;
		}
		public void setHeat(double heat){
			this.heat = heat;
		}
		public String toString() {
			return "Timestamp: "+time +" Watts: "+watts + " Heat: " + heat;
		}
	}

	// Measures volts and amps
	static class ElectricitySensor {
		@RowKey Date time;
		double volts;
		double amps;
		public void setTime(Date time){
			this.time = time;
		}
		public void setVolts(double volts){
			this.volts = volts;
		}
		public void setAmps(double amps){
			this.amps = amps;
		}
		public String toString(){
			return "Timestamp: "+time +" Volts: " + volts + " Amps: " + amps;
		}
	}
	public GridDBSinkTask() {
	}

	@Override public String version() {
		return new GridDBSinkConnector().version();
	}

	@Override public void start(Map<String, String> props) {
		System.out.println("Starting Task....");
		Properties gsprops = new Properties();

		gsprops.setProperty("notificationAddress", props.get(GridDBSinkConnector.HOST_CONFIG));
		gsprops.setProperty("notificationPort", props.get(GridDBSinkConnector.PORT_CONFIG));
		gsprops.setProperty("clusterName", props.get(GridDBSinkConnector.CLUSTERNAME_CONFIG));
		gsprops.setProperty("user", props.get(GridDBSinkConnector.USER_CONFIG));
		gsprops.setProperty("password", props.get(GridDBSinkConnector.PASSWORD_CONFIG));

		try {
			store=GridStoreFactory.getInstance().getGridStore(gsprops);
		} catch (Exception ex) {
			System.out.println("Failed to get GS instance\n" + ex);
		}

	}

	@Override public void put(Collection<SinkRecord> sinkRecords) {
		
		for (SinkRecord record : sinkRecords) {
			// Fault tolerance: Do not parse any record message that does not have a HashMap value
			if (record.value().getClass() == HashMap.class) {
			//HashMap simpleMap = (HashMap) ((HashMap)record.value()).get("payload");
				try {
					// Connect to sensor-type container
					Container<String,SensorType> typeContainer = store.putCollection(SENSOR_TYPE_NAME,SensorType.class); 
					// Attempt to retrieve and decode message content ("payload")
					String payload = (String) ((HashMap)record.value()).get("payload");
					// Create a sensor-type record that will correspond to the "id" and "type" fields of the JSON message
					SensorType sensorType = JSON.decode(payload,SensorType.class);

					// To determine which kind of schema the sensor-data should fall into and what kind of object the
					// rest of the message should be parsed we check "type" field of sensor type.
					if(payload.contains("light")){
						// For a record of type : light
						// Decode the last two fields of the JSON message into a LightSensor object
						LightSensor sensor = JSON.decode(payload, LightSensor.class);
						// Add the records timestamp as the time field
						sensor.time = new Date(record.timestamp());
						System.out.println(sensor.light);
						System.out.println("Putting "+ sensor + " into " + sensorType.id);
						// Insert into a container with a name that matches its corresponding sensor id
						TimeSeries<LightSensor> ts = store.putTimeSeries(sensorType.id, LightSensor.class);
						ts.put(sensor);

					} else if(payload.contains("watts")) {
						// For a record of type : watts
						// Decode the last two fields of the JSON message into a WattSensor object
					 	WattSensor sensor = JSON.decode(payload, WattSensor.class);
					 	// Add the records timestamp as the time field
						sensor.time = new Date(record.timestamp());
						System.out.println("Putting " + sensor + " into " + sensorType.id);
						// Insert into a container with a name that matches its corresponding sensor id
						TimeSeries<WattSensor> ts = store.putTimeSeries(sensorType.id, WattSensor.class);
						ts.put(sensor);

					} else {
						// For a record of type : volts
						// Decode the last two fields of the JSON message into a ElectricitySensor object
						ElectricitySensor sensor = JSON.decode(payload,ElectricitySensor.class);
						// Add the records timestamp as the time field
						sensor.time = new Date(record.timestamp());
						System.out.println("Putting "+ sensor + " into " + sensorType.id);
						// Insert into a container with a name that matches its corresponding sensor id
						TimeSeries<ElectricitySensor> ts = store.putTimeSeries(sensorType.id, ElectricitySensor.class);
						ts.put(sensor); 

					}

					// If sensor-type container does not have an record of a certain sensor-id
					if(typeContainer.get(sensorType.id) == null){
						System.out.println("Need to insert into container");
						System.out.println("Putting sensor "  + sensorType + " into " + SENSOR_TYPE_NAME);
						// Add the record time
						sensorType.time = new Date(record.timestamp());
						// Insert the sensor-type record into container, with the sensor-id as the Key
						typeContainer.put(sensorType.id,sensorType);

					} else {
						System.out.println("Ignore this duplicate");
					}
					
				} catch (Exception ex) {
					System.out.println("Failed to append TSRecord");
				}
			}
		}
	}

	@Override public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		log.trace("Flushing output stream");
	}

	@Override public void stop() {
		try {
			store.close();
		} catch (Exception ex) {
		}

	}

}
