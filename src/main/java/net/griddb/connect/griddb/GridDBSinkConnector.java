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
 * This file is based on the file FileStreamSinkConnector.java
 * in https://github.com/apache/kafka/archive/0.10.2.1.zip
 * (Apache Kafka 0.10.2.1).
 */

package net.griddb.connect.griddb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// This class sets up a connection to be used to stream data from Kafka into a GridDB database 
// So that certain fields or content from that message can be formatted and inserted into GridDB 
// as timestamp records
// It is through this class when used in combination with griddb-sink.properties file that a connection
// between Kafka and GridDB is made
// To decide which connector that Kafka should use as data-sink as well as what GridDB cluster the GridDBSinkConnector and
// GridDBSink task should connect and insert records into simply edit the "topics, clustername, host, port, user and password"
// in griddb-sink.properties file
// To run this connect in your command line run
// $ connect-standalone.sh connect-standalone.properties griddb-sink.properties
public class GridDBSinkConnector extends SinkConnector {


    // Setup Kafka ConfigDef with necessary GridDB variables.
    // These configurations like the GridDB Host IP, port, Clustername and credentials  
    // will be extracted from the griddb-sink.properties file 
    public static final String HOST_CONFIG = "host";
    public static final String PORT_CONFIG = "port";
    public static final String CLUSTERNAME_CONFIG = "clusterName";
    public static final String USER_CONFIG = "user";
    public static final String PASSWORD_CONFIG = "password";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(HOST_CONFIG, Type.STRING, Importance.HIGH, "GridDB Host") 
    .define(PORT_CONFIG, Type.STRING, Importance.HIGH, "GridDB Port")
    .define(CLUSTERNAME_CONFIG, Type.STRING, Importance.HIGH, "GridDB clusterName")
    .define(USER_CONFIG, Type.STRING, Importance.HIGH, "GridDB Username")
    .define(PASSWORD_CONFIG, Type.STRING, Importance.HIGH, "GridDB Password");

    String host, port, clusterName, database, user, password;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    // Start connection from a properties object representing the configuration fields to connect to a GridDB instance
    public void start(Map<String, String> props) {
        host = props.get(HOST_CONFIG);
        port = props.get(PORT_CONFIG);
        clusterName = props.get(CLUSTERNAME_CONFIG);
        user = props.get(USER_CONFIG);
        password = props.get(PASSWORD_CONFIG);
        System.out.println("Connector starting");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GridDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (host != null)
                config.put(HOST_CONFIG, host);
            if (port != null)
                config.put(PORT_CONFIG, port);
            if (clusterName != null)
                config.put(CLUSTERNAME_CONFIG, clusterName);
            if (user != null)
                config.put(USER_CONFIG, user);
            if (password != null)
                config.put(PASSWORD_CONFIG, password);
            configs.add(config);
        }

        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since GridDBSinkConnector has no background monitoring.
    }
    // Return configuration information
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
