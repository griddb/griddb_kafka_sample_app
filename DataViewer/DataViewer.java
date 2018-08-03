package DataViewer;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;
import com.toshiba.mwcloud.gs.TimestampUtils;

import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.GSType;

import java.util.Date;
import java.util.EnumSet;

public class DataViewer {

    private static final String[] TYPE_NAMES = {"light", "volts", "watts"}; // Different sensor-types
	private static final String SENSOR_TYPE_NAME = "Sensor_Types"; // Sensor-type container name
	private  static GridStore store = null;

	private  static Map<String,Map<String,RowKeyPredicate<?>>> typeMap; 
	private  static Container<String,SensorType> typeContainer; // Container of sensor-types


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

	/*
	// The following three classes represent different schemas for Data-log containers
	// These data-log containers are named or identified by their sensor-id  
	// and have Timestamp RowKeys. All columns or field beside the timestamp are number-types
	*/

	// Represent a light sensor that measures light and sound
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

	// Represent a light sensor that measures power (watts) and heat
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

	// Represent an Electricity sensor that measures volts and amps
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



	public static void main(String[] args) throws GSException, ClassNotFoundException {

		// Get a GridStore instance
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]); // Default for localhost 239.0.0.1 
		props.setProperty("notificationPort", args[1]); // Default port 31999
		props.setProperty("clusterName", args[2]); // Cluster name used for this project was 'defaultCluster'
		props.setProperty("user", args[3]); // Default is 'admin'
		props.setProperty("password", args[4]); // Default is 'admin'
		store = GridStoreFactory.getInstance().getGridStore(props); // Initiate GridDB connection
		Date start = new Date("2018/06/21"); // Set a start date, can set to "null" to start at earliest record
		Date end = new Date("2018/06/27"); // Set an end date, can set to "null" to end at latest record

		// Connect to type containers
		inititateTypeContainer(SENSOR_TYPE_NAME); // Initiate sensor-type container connection
		setTypeMap(TYPE_NAMES); // Initiate type-map for a multi-get query with all the types as keys

		String tql = "SELECT * FROM " + SENSOR_TYPE_NAME; // Get all records from the sensor-type Collection
		// Create and fetch results for the query
		Query<SensorType> query = typeContainer.query(tql,SensorType.class);
		RowSet<SensorType> rowSet = query.fetch(); 

		// Set a multi-get Date-key to query multiple containers for a specified time-range
		RowKeyPredicate<Date> dateKeys = RowKeyPredicate.create(Date.class);
		dateKeys.setStart(start);
		dateKeys.setFinish(end);


		// Initiate the type with pairs of "container-name", "date-range" queries to be use for a multi-get query
		setMultiGetMaps(rowSet, dateKeys);

		// Execute the multiget query
		executeMultiGet(start,end); 
		// Close connection to GridDB
		if(store != null)
			store.close();
	}

	// Retrieve statistical information for all columns for an inputted TimeSeries
	public static HashMap<String,Double> getRowCounts(TimeSeries timeSeries, Date start, Date end, String id,String type) throws GSException {
		ContainerInfo containerInfo = store.getContainerInfo(id); // Get container's schema
		int columnCount = containerInfo.getColumnCount(); // Get container's column count
		AggregationResult count = timeSeries.aggregate(start,end,type,Aggregation.COUNT);
		HashMap<String,Double> sensorAggregationMap = new HashMap<>();
		for(int i = 1; i < columnCount; i++){ //Skip over the timestamp column
			String fieldId = containerInfo.getColumnInfo(i).getName(); // Get column name
			AggregationResult minSet = timeSeries.aggregate(start,end,fieldId,Aggregation.MINIMUM); // Get minimum
			AggregationResult averageSet = timeSeries.aggregate(start,end,fieldId,Aggregation.WEIGHTED_AVERAGE); //Get weighted average
			AggregationResult maxSet = timeSeries.aggregate(start,end,fieldId,Aggregation.MAXIMUM); // Get maximum
			AggregationResult stdDev = timeSeries.aggregate(start,end,fieldId,Aggregation.STANDARD_DEVIATION); // Get standard deviation
			String columnResult = String.format("Avg: %s %f",fieldId,averageSet.getDouble());
			columnResult += String.format(" Min: %s %f",fieldId,minSet.getDouble());
			columnResult += String.format(" Max: %s %f",fieldId,maxSet.getDouble());
			columnResult += String.format(" Std.Dev: %s output is: %f",fieldId,stdDev.getDouble()); // Get individual statistics (average,min,max) on each column
			System.out.print(columnResult + "\t");
			sensorAggregationMap.put(fieldId,averageSet.getDouble() * count.getDouble());
		}
		System.out.println();
		sensorAggregationMap.put("count",count.getDouble());
		timeSeries.close(); // Close container connection and release resources
		return sensorAggregationMap;
	}

	
	// Determine which schema, depending on the sensor_id and sensor-type a sensor falls into and 
	// Get data for the right corresponding container (i.e. volt sensor goes to TimeSeries<ElectricitySensor>)
	public static HashMap<String,Double> getSensorData(String sensorType, String id, Date start, Date end) throws GSException{
		System.out.println("Sensor Readings from " + sensorType + " Sensor with ID: " + id);
		// Get and display all records and statistical information for a container with the LightSensor schema
		TimeSeries<?> ts;
		HashMap<String,Double> sensorAggregationMap = new HashMap<>();
		if(sensorType.equals("light")){
			ts = store.putTimeSeries(id,Sensor.class);
			sensorAggregationMap = getRowCounts(ts,start,end,id,sensorType);
		}

		// Get and display all records and statistical information for a container with the WattSensor schema
		else if (sensorType.equals("watts")){
			ts = store.putTimeSeries(id,WattSensor.class);
			sensorAggregationMap = getRowCounts(ts,start,end,id,sensorType);
		}

		// Get and display all records and statistical information for a container with the ElectricitySensor schema
		else if(sensorType.equals("volts")){
			ts = store.putTimeSeries(id,ElectricitySensor.class);
			sensorAggregationMap = getRowCounts(ts,start,end,id,sensorType);
		}
		return sensorAggregationMap;
	}

	private static void displayAverages(HashMap<String,Double> sensorAggregationMap){
		Double count = sensorAggregationMap.get("count");
		for(Entry<String, Double> averagePair : sensorAggregationMap.entrySet()){
			if(!(averagePair.getKey().equals("count"))){
				String displayedAverages = String.format("Avg %s %f",averagePair.getKey(), averagePair.getValue() / count );
				System.out.print(displayedAverages + "\t");
			}
		}
		System.out.println();
	}


	// Create or connect to a sensor-type collection with the given name
	private static void inititateTypeContainer(String containerName) throws GSException {
		typeContainer = store.putCollection(containerName, SensorType.class);
	}

	// Initiate the typeMap that will can be used for a multi-get query
	// This type map will have all the sensor-types as keys and predicate maps of the sensor-id and rows they can
	// correspond to as values 
	private static void setTypeMap(String[] typeName){
		typeMap = new HashMap<>();
		for(String type : typeName){
			typeMap.put(type,new HashMap<>());
		}
	}

	// Create an entry in a predicate that will correspond to an individual sensor-id  as a key 
	// and a date-range (type RowKeyPredicate<Date>) query as the value
	private static void setMultiGetMaps(RowSet<SensorType> rowSet, RowKeyPredicate<Date> dateKey) throws GSException {
		while(rowSet.hasNext()){
			SensorType rowType = rowSet.next();
			typeMap.get(rowType.type).put(rowType.id,dateKey);
		}
	}

	private static void executeMultiGet(Date start, Date end) throws GSException{
		// Get all rows that correspond to a certain type and get all the records in the cluster that 
		// match that type and fall in the date-range. The multi-get can search through different containers
		// such that the container-names are specified
		
		for( Entry<String, Map<String,RowKeyPredicate<?>>> entry : typeMap.entrySet()){ 
			String type = entry.getKey(); // Get sensor-type
			Map<String, List<Row>> multiGetResults = store.multiGet(entry.getValue()); 
			// Get a map corresponding to the all the rows that match that type and fit the query
			System.out.println(type + " Sensors.");
			// For all sensor-id's that match that type
			HashMap<String,Double> typeAveragesMap = new HashMap<>();
			for(Entry<String, List<Row>> getResult : multiGetResults.entrySet()){
				String id = getResult.getKey(); // Get the sensor-id
				HashMap<String,Double> sensorAverages = getSensorData(type,id,start,end);
				if(typeAveragesMap.isEmpty())
					typeAveragesMap = sensorAverages;
				else {
					for(Entry<String,Double> pair : sensorAverages.entrySet()){
						String key = pair.getKey();
						Double value = pair.getValue();
						typeAveragesMap.put(key,typeAveragesMap.get(key) + value);
					}
				}
			}
			System.out.print("\n\nAll: " + type + "sensors");
			System.out.println();
			displayAverages(typeAveragesMap);
			System.out.println();
			System.out.println();
			 
		}
	}
}
