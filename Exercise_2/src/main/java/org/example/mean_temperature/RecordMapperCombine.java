package org.example.mean_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecordMapperCombine extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static enum FaultyTemperatureCounter {
        MISSING,
        MALFORMED
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Record record = new Record(value.toString());

        if (record.isMissingTemperature()) {
            context.getCounter(RecordMapperNaive.FaultyTemperatureCounter.MISSING).increment(1);
        }
        if(record.isMalformed()) {
            context.getCounter(RecordMapperNaive.FaultyTemperatureCounter.MALFORMED).increment(1);
        }
        //hadoop automatically creates counters with respective names
        context.getCounter("QualityCounter", record.quality()).increment(1);

        if (record.isValidTemperature()) {
            context.write(new Text(record.year()), new IntWritable(record.airTemperature()));
        }
    }
}
