package org.example.mean_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class RecordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static enum CustomCounters {
        MISSING,
        MALFORMED
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Record record = new Record(value.toString());

        if (record.isMissingTemperature()) context.getCounter(CustomCounters.MISSING).increment(1);
        if(record.isMalformed()) context.getCounter(CustomCounters.MALFORMED).increment(1);

        if (record.isValidTemperature()) {
            context.write(new Text(record.year()), new IntWritable(record.airTemperature()));
        }
    }
}
