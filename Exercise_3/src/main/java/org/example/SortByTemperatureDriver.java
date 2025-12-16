package org.example;

import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.example.median_temperature.Schemas;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.example.median_temperature.SortByTemperatureMapper;


public class SortByTemperatureDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Temperature Sort");
        job.setJarByClass(SortByTemperatureDriver.class);

        job.setMapperClass(SortByTemperatureMapper.class);
        job.setNumReduceTasks(0); // map-only

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Kompression aktivieren
        FileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(
                job, SequenceFile.CompressionType.BLOCK);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
