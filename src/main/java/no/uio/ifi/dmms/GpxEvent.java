package no.uio.ifi.dmms;

public class GpxEvent {
    private Long timestamp;
    private Double power;
    private Integer hr;
    private Float lat;
    private Float lon;

    public GpxEvent() {
        this.timestamp = timestamp;
        this.power = 0D;
        this.hr = 0;
        this.lat = 0F;
        this.lon = 0F;
    }
    public GpxEvent(Long timestamp, Double power, Integer hr, Float lat, Float lon) {
        this.timestamp = timestamp;
        this.power = power;
        this.hr = hr;
        this.lat = lat;
        this.lon = lon;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GpxEvent) {
            GpxEvent event = (GpxEvent) obj;
            return (this.timestamp == event.timestamp
            && this.power == event.power
            && this.hr == event.hr
            && this.lat == event.lat
            && this.lon == event.lon);
        }
        else return false;
    }

    @Override
    public int hashCode() {
        return Math.toIntExact(timestamp);
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Double getPower() {
        return power;
    }

    public Integer getHr() {
        return hr;
    }

    public Float getLat() {
        return lat;
    }

    public Float getLon() {
        return lon;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setPower(Double power) {
        this.power = power;
    }

    public void setHr(Integer hr) {
        this.hr = hr;
    }

    public void setLat(Float lat) {
        this.lat = lat;
    }

    public void setLon(Float lon) {
        this.lon = lon;
    }
}
