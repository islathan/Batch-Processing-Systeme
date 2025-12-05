package org.example.max_temperature;

import org.apache.hadoop.io.Text;

public class Record {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private boolean airTemperatureMalformed;
    private String quality;
    private String stationId;


    public Record(String record) {
        this.stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        this.year = record.substring(15,19);
        this.airTemperatureMalformed = false;

        if (record.charAt(87) == '+') {
            this.airTemperature = Integer.parseInt(record.substring(88,92));
        } else if (record.charAt(87) == '-') {
            this.airTemperature = Integer.parseInt(record.substring(87,92));
        } else {
            this.airTemperatureMalformed = true;
        }
        this.quality = record.substring(92,93);
    }

    public String year() { return year; }

    public boolean isValidTemperature() {
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public int airTemperature() {
        return airTemperature;
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String quality() {
        return quality;
    }

    public Integer yearInt() {
        return Integer.parseInt(year);
    }

    public String stationId() {
        return stationId;
    }
}
