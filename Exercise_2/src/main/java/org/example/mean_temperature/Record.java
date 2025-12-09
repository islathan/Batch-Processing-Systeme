package org.example.mean_temperature;

public class Record {
    private static final int MISSING_TEMPERATURE = 9999;

    private final String year;
    private int temperature;
    private boolean temperatureMalformed;
    private final String quality;

    public Record(String record) {
        year = record.substring(15,19);
        quality = record.substring(92,93);
        temperatureMalformed = false;

        char sign = record.charAt(87);
        if ('+' == sign) {
            temperature = Integer.parseInt(record.substring(88,92));
        } else if ('-' == sign) {
            temperature = Integer.parseInt(record.substring(87,92));
        } else {
            temperatureMalformed = true;
        }
    }
    public Boolean isMalformed() { return temperatureMalformed; }

    public Boolean isMissingTemperature() { return temperature == MISSING_TEMPERATURE; }

    public String quality() {return quality;}

    public String year() { return year; }

    public int airTemperature() { return temperature; }

    public boolean isValidTemperature() {
        return !temperatureMalformed && temperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }
}