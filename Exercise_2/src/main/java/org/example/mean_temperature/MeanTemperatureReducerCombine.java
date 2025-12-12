package org.example.mean_temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanTemperatureReducerCombine extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
        long totalSum = 0;
        long totalCount = 0;

        //The combiner does not guarantee that all data for a given key will be aggregated on a single machine.
        //we need to aggregate again, as it could be that mappers with e.g. same year were executed on different machines
        // => loop below would therefore sum up the aggregated values of combiners for each mapper-machine
        for (SumCountWritable val : values) {
            totalSum += val.getSum();
            totalCount += val.getCount();
        }

        if (totalCount > 0) {
            context.write(key, new DoubleWritable((double) totalSum / totalCount / 10.0));
        }
    }
}