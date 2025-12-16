package org.example.median_temperature;

import org.apache.avro.Schema;

import java.io.IOException;

public enum Schemas {
    INSTANCE;

    private Schema weatherRecordSchema;

    Schemas() {
        //Create the parser for the schema
        Schema.Parser parser = new Schema.Parser();

        //Create schema from .avsc file
        try {
            String weatherRecordSchemaFile = "/avro/WeatherRevord.avsc";
            weatherRecordSchema = parser.parse(getClass().getResourceAsStream(weatherRecordSchemaFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Schema weatherRecordSchema() {
        return weatherRecordSchema;
    }
}
