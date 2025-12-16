package org.example.median_temperature;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedianTemperatureMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {

    private final GenericRecord keyRecord = new GenericData.Record(Schemas.INSTANCE.yearTempKeySchema());

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Record record = new Record(value.toString());

        if (record.isValidTemperature()) {
            keyRecord.put("year", Integer.parseInt(record.year()));
            keyRecord.put("temperature", record.airTemperature());

            context.write(new AvroKey<>(keyRecord), NullWritable.get());
        }
    }
}
