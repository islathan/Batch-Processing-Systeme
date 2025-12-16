package org.example.median_temperature;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MedianTemperatureReducer extends Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>, AvroValue<Float>> {
    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        List<Float> temps = new ArrayList<>();

        for (NullWritable ignored : values) {
            temps.add((Float) key.datum().get("temperature"));
        }

        int n = temps.size();
        float median = (n % 2 == 1)
                ? temps.get(n / 2)
                : (temps.get(n / 2 - 1) + temps.get(n / 2)) / 2f;

        context.write(
                new AvroKey<>((Integer) key.datum().get("year")),
                new AvroValue<>(median)
        );
    }
}


