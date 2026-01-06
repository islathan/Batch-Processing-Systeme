package org.example.median_temperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MedianTemperatureReducer
        extends Reducer<YearTemperatureKey, IntWritable,
        IntWritable, FloatWritable> {

    @Override
    protected void reduce(YearTemperatureKey key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        List<Integer> temps = new ArrayList<>();

        for (IntWritable v : values) {
            temps.add(v.get());
        }

        int n = temps.size();
        float median = (n % 2 == 1)
                ? temps.get(n / 2)
                : (temps.get(n / 2 - 1) + temps.get(n / 2)) / 2f;

        context.write(
                new IntWritable(key.getYear()),
                new FloatWritable(median)
        );
    }
}
