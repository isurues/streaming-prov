package edu.indiana.d2i.flink.utils;

public class ProvEdge {

    private String source;
    private String destination;

    public ProvEdge(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    @Override
    public String toString() {
        return "<" + source + ", " + destination + ">";
    }

    public String toJSONString() {
        return "{\"sourceId\":\"" + source + "\", \"destId\":\"" + destination + "\"}";
    }
}
