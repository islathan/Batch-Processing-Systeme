package org.example;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.NullWritable;
import org.apache.avro.mapred.AvroKey;
import org.example.median_temperature.*;

public class MedianTemperatureDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MedianTemperature");
        job.setJarByClass(MedianTemperatureDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapperClass(MedianTemperatureMapper.class);
        job.setReducerClass(MedianTemperatureReducer.class);

        // Map output (secondary sort key)
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        AvroJob.setMapOutputKeySchema( job, Schemas.INSTANCE.yearTempKeySchema() );
        AvroJob.setMapOutputValueSchema( job, Schema.create(Schema.Type.NULL) );

        // Final output: year -> median temperature
        AvroJob.setOutputKeySchema( job, Schema.create(Schema.Type.INT) );
        AvroJob.setOutputValueSchema( job, Schema.create(Schema.Type.FLOAT) );

        // Secondary sort plumbing
        job.setSortComparatorClass(YearTemperatureSortComparator.class);
        job.setGroupingComparatorClass(YearGroupingComparator.class);
        job.setPartitionerClass(YearPartitioner.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MedianTemperatureDriver(), args));
    }
}
