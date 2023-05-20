package org.example.bitcask;

import java.io.Serializable;

public class Record implements Serializable {
    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;
    Weather weather;

    public Record(long station_id, long s_no, String battery_status, long battery_timeStamp, Weather weather) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = battery_timeStamp;
        this.weather = weather;
    }

    @Override
    public String toString() {
        return "Record{" +
                "station_id=" + station_id +
                ", s_no=" + s_no +
                ", battery_status='" + battery_status + '\'' +
                ", battery_timeStamp=" + status_timestamp +
                ", weather=" + weather.toString() +
                '}';
    }

    public Record() {

    }
}
