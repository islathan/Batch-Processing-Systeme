package org.example.median_temperature;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MedianTemperatureReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

}
