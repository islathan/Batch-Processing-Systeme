package org.example.median_temperature;

import org.apache.avro.Schema;

import java.io.IOException;

public enum Schemas {
    INSTANCE;

    private Schema yearTempKeySchema;

    Schemas() {
        //Create the parser for the schema
        Schema.Parser parser = new Schema.Parser();

        //Create schema from .avsc file
        try {
            String yearTempKeySchemaFile = "/avro/WeatherRevord.avsc";
            yearTempKeySchema = parser.parse(getClass().getResourceAsStream(yearTempKeySchemaFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Schema yearTempKeySchema() {
        return yearTempKeySchema;
    }
}
