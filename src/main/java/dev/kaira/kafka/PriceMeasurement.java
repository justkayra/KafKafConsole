package dev.kaira.kafka;

import java.time.Instant;

public class PriceMeasurement {
    public int itemId;
    public String itemName;
    public Instant timestamp;
    public double value;

    public PriceMeasurement(int stationId, String stationName, Instant timestamp,
                                  double value) {
        this.itemId = stationId;
        this.itemName = stationName;
        this.timestamp = timestamp;
        this.value = value;
    }
}
