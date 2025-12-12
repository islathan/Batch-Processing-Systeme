package org.example.mean_temperature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// especially for tests structure must be =>
// input/output: Mapper == Combiner == Reducer Input
public class RecordMapperCombine extends Mapper<LongWritable, Text, Text, SumCountWritable> {
    public static enum FaultyTemperatureCounter {
        MISSING,
        MALFORMED
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Record record = new Record(value.toString());

        if (record.isMissingTemperature()) {
            context.getCounter(RecordMapperCombine.FaultyTemperatureCounter.MISSING).increment(1);
        }
        if(record.isMalformed()) {
            context.getCounter(RecordMapperCombine.FaultyTemperatureCounter.MALFORMED).increment(1);
        }
        //hadoop automatically creates counters with respective names
        context.getCounter("QualityCounter", record.quality()).increment(1);

        if (record.isValidTemperature()) {
            context.write(new Text(record.year()), new SumCountWritable(record.airTemperature(), 1));
        }
    }
}
