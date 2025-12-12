package org.example.mean_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanTemperatureReducerNaive extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        context.write(key, new DoubleWritable(sum / count / 10));
    }
}
