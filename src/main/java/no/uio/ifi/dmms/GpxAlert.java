package no.uio.ifi.dmms;

public class GpxAlert {
    private Long timestamp;
    private Double factor;
    private Float lat;
    private Float lon;

    public GpxAlert(GpxEvent gpxEvent) {
        this.timestamp = gpxEvent.getTimestamp();
        this.factor = Double.valueOf(gpxEvent.getHr());
        this.lat = gpxEvent.getLat();
        this.lon = gpxEvent.getLon();
    }

    @Override
    public String toString() {
        return timestamp+","+factor+","+lat+","+lon;
    }
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getFactor() {
        return factor;
    }

    public void setFactor(Double factor) {
        this.factor = factor;
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
}
