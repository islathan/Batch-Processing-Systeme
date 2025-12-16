package org.example.median_temperature;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<AvroKey<GenericRecord>, Object> {

    @Override
    public int getPartition(AvroKey<GenericRecord> key, Object value, int numPartitions) {
        Integer year = (Integer) key.datum().get("year");

        return (year.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
