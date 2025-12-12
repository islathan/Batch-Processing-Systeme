package org.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.example.mean_temperature.MeanTemperatureReducerNaive;
import org.example.mean_temperature.RecordMapperNaive;


public class MeanTemperatureDriverNaive extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        // Create a new Job
        Job job = Job.getInstance(conf, "MeanTemperature");
        job.setJarByClass(getClass());

        // Set the input and output file path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set the mapper, reducer and combiner classes
        job.setMapperClass(RecordMapperNaive.class);
        job.setReducerClass(MeanTemperatureReducerNaive.class);

        // Set map output key/value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set final output key/value types (reducer output)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanTemperatureDriverNaive(), args);
        System.exit(exitCode);
    }
}
