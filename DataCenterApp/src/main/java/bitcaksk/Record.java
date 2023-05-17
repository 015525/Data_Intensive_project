package bitcaksk;

import java.io.Serializable;

public class Record implements Serializable {
    long station_id;
    long s_no;
    String battery_status;
    long battery_timeStamp;
    Weather weather;


    public Record(long station_id, long s_no, String battery_status, long battery_timeStamp, Weather weather) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.battery_timeStamp = battery_timeStamp;
        this.weather = weather;
    }

    @Override
    public String toString() {
        return "Record{" +
                "station_id=" + station_id +
                ", s_no=" + s_no +
                ", battery_status='" + battery_status + '\'' +
                ", battery_timeStamp=" + battery_timeStamp +
                ", weather=" + weather.toString() +
                '}';
    }

    public Record() {

    }
}
