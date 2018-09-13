package no.uio.ifi.dmms;

public class GpxEventAccumulator {
    private Long timestamp;
    private Double power;
    private Integer hr;
    private Float lat;
    private Float lon;
    private Long count;

    public GpxEventAccumulator(Long timestamp, Double power, Integer hr, Float lat, Float lon, Long count) {
        this.timestamp = timestamp;
        this.power = power;
        this.hr = hr;
        this.lat = lat;
        this.lon = lon;
        this.count = count;
    }
    public GpxEventAccumulator() {
        this.timestamp = 0L;
        this.power = 0D;
        this.hr = 0;
        this.lat = 0F;
        this.lon = 0F;
        this.count = 0L;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GpxEventAccumulator) {
            GpxEventAccumulator event = (GpxEventAccumulator) obj;
            return (this.timestamp == event.timestamp
                    && this.power == event.power
                    && this.hr == event.hr
                    && this.lat == event.lat
                    && this.lon == event.lon
                    && this.count == event.count);
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

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getPower() {
        return power;
    }

    public void setPower(Double power) {
        this.power = power;
    }

    public Integer getHr() {
        return hr;
    }

    public void setHr(Integer hr) {
        this.hr = hr;
    }

    public Float getLat() {
        return lat;
    }

    public void setLat(Float lat) {
        this.lat = lat;
    }

    public Float getLon() {
        return lon;
    }

    public void setLon(Float lon) {
        this.lon = lon;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
