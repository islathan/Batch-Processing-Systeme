package org.example.median_temperature;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedianTemperatureMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {

    private final GenericRecord weatherRecord = new GenericData.Record(Schemas.INSTANCE.weatherRecordSchema());

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Record record = new Record(value.toString());
        Integer year = Integer.parseInt(record.year());

        if (record.isValidTemperature()) {
            weatherRecord.put("year", year);
            weatherRecord.put("temperature", record.airTemperature());
            context.write(new AvroKey<>(year), new AvroValue<>(weatherRecord));
        }
    }
}
