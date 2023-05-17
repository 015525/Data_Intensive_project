package bitcaksk;

import java.io.Serializable;

public class Weather implements Serializable {
    int humidity;
    int temperature;
    int wind_speed;

    public Weather(int humidity, int temperature, int wind_speed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.wind_speed = wind_speed;
    }

    @Override
    public String toString() {
        return "Weather{" +
                "humidity=" + humidity +
                ", temperature=" + temperature +
                ", wind_speed=" + wind_speed +
                '}';
    }
}
