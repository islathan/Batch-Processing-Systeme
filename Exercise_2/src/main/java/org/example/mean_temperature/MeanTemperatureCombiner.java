package org.example.mean_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanTemperatureCombiner extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {

    //Combiner = mini-reducer, aggregation will happen locally on mapper machine
    //The combiner does not guarantee that all data for a given key will be aggregated on a single machine.
    public void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long count = 0;

        for (SumCountWritable v : values) {
            sum += v.getSum();
            count += v.getCount();
        }

        context.write(key, new SumCountWritable(sum, count));
    }
}