package no.uio.ifi.dmms;


import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class GpxTuple {
    private static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private String lat;
    private String lon;
    private String time;
    private String power;
    private String temp;
    private String heartrate;
    private String cadence;

    public String getLat() {
        return lat;
    }

    public String getLon() {
        return lon;
    }

    public String getTime() {
        return time;
    }

    public String getPower() {
        return power;
    }

    public String getHeartrate() {
        return heartrate;
    }

    public String getCadence() {
        return cadence;
    }

    public String getTemp() {
        return temp;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public void setPower(String power) {
        this.power = power;
    }

    public void setHeartrate(String heartrate) {
        this.heartrate = heartrate;
    }

    public void setCadence(String cadence) {
        this.cadence = cadence;
    }
    public void setTemp(String temp) {
        this.temp = temp;
    }
    public String getPosixString() {
        LocalDateTime time = LocalDateTime.parse(this.time, TIME_FORMATTER);
        ZoneId zoneId = ZoneId.systemDefault();
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()+"";
    }
}