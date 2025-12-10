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
import org.example.mean_temperature.MeanTemperatureReducer;
import org.example.mean_temperature.RecordMapper;


public class MeanTemperatureDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        // Create a new Job
        Job job = Job.getInstance(conf, "MaxTemperature");
        job.setJarByClass(getClass());

        // Set the input and output file path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set the mapper, reducer and combiner classes
        job.setMapperClass(RecordMapper.class);
        job.setReducerClass(MeanTemperatureReducer.class);

        // Set map output key/value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set final output key/value types (reducer output)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //Print Counters
        long missing = job.getCounters()
                    .findCounter(RecordMapper.FaultyTemperatureCounter.MISSING)
                    .getValue();
        long malformed = job.getCounters()
                .findCounter(RecordMapper.FaultyTemperatureCounter.MALFORMED)
                .getValue();
        CounterGroup qualityCounters = job.getCounters()
                .getGroup("QualityCounter");
        System.out.println("Distribution of quality codes:");
        for (Counter c : qualityCounters) {
            String qualityCode = c.getName();   // z. B. "1", "A", "R"
            long count = c.getValue();          // Häufigkeit
            System.out.println(qualityCode + " = " + count);
        }
        System.out.println("Amount of missing temperatures: " + missing);
        System.out.println("Amount of malformed temperatures: " + malformed);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
