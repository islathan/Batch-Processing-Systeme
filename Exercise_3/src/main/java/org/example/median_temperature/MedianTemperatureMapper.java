package org.example.median_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedianTemperatureMapper
        extends Mapper<LongWritable, Text, YearTemperatureKey, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Record record = new Record(value.toString());

        if (record.isValidTemperature()) {
            context.write(
                    new YearTemperatureKey( Integer.parseInt(record.year()), record.airTemperature()),
                    new IntWritable(record.airTemperature())
            );
        }
    }
}
