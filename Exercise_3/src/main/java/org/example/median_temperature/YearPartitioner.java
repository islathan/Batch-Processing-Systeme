package org.example.median_temperature;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner
        extends Partitioner<YearTemperatureKey, NullWritable> {

    @Override
    public int getPartition(YearTemperatureKey key, NullWritable value, int numPartitions) {
        return (key.getYear() & Integer.MAX_VALUE) % numPartitions;
    }
}
