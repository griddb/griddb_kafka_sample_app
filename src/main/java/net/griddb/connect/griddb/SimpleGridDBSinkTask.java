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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.TimeSeries;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Date;
import net.arnx.jsonic.JSON;

/**
 * SimpleGridDBSinkTask writes records to a GridDB TimeSeries Container
 */
public class SimpleGridDBSinkTask extends SinkTask {
	private static final Logger log = LoggerFactory.getLogger(SimpleGridDBSinkTask.class);

	private GridStore store;
	private static final String CONTAINER_NAME = "sample";

	static class Sensor{
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


	public SimpleGridDBSinkTask() {
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
			System.out.println("Recieved a "+ record.value().getClass());
			String payload = (String)((HashMap)record.value()).get("payload");
			Sensor sensor = JSON.decode(payload, Sensor.class);
			sensor.time = new Date(record.timestamp());
			System.out.println("Putting "+ sensor + " into " + record.topic());

			try {
				TimeSeries<Sensor> ts = store.putTimeSeries(record.topic(), Sensor.class);
				ts.put(sensor);
			} catch (Exception ex) {
				System.out.println("Failed to append TSRecord");
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
