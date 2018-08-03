package DataViewer;
import java.util.Arrays;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import java.util.Date;

public class SimpleDataViewer {

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

	public static void main(String[] args) throws GSException {

		// Get a GridStore instance
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		TimeSeries<Sensor> logTs = store.putTimeSeries("test", Sensor.class);

		Date start = new Date("2018/01/01");
		Date end = new Date();

		Query<Sensor> query = logTs.query(start, end);
		// fetch row
		RowSet<Sensor> rowSet = query.fetch();
		while (rowSet.hasNext()) {
			Sensor sensor= rowSet.next();
			System.out.println(sensor);
		}

		// Release the resource
		store.close();
	}

}
