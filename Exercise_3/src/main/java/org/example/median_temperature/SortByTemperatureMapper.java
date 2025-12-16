package org.example.median_temperature;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortByTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable outKey = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Record record = new Record(value.toString());

        if (record.isValidTemperature()) {
            outKey.set(record.airTemperature());
            context.write(outKey, value);
        }
    }
}
