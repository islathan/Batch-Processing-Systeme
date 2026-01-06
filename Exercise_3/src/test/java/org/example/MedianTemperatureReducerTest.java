package org.example;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.example.median_temperature.MedianTemperatureReducer;
import org.example.median_temperature.YearTemperatureKey;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class MedianTemperatureReducerTest {

    private ReduceDriver<YearTemperatureKey, IntWritable,
            IntWritable, FloatWritable> reduceDriver;

    @Before
    public void setup() {
        MedianTemperatureReducer reducer = new MedianTemperatureReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducerMedianOdd() throws Exception {

        // One key per group (year)
        YearTemperatureKey key = new YearTemperatureKey(1901, 80);

        // Values arrive already sorted by temperature
        reduceDriver.withInput(
                key,
                Arrays.asList(
                        new IntWritable(80),
                        new IntWritable(81),
                        new IntWritable(82)
                )
        );

        reduceDriver
                .withOutput(new IntWritable(1901), new FloatWritable(81f))
                .runTest();
    }

    @Test
    public void testReducerMedianEven() throws Exception {

        YearTemperatureKey key = new YearTemperatureKey(1901, 80);

        reduceDriver.withInput(
                key,
                Arrays.asList(
                        new IntWritable(80),
                        new IntWritable(81),
                        new IntWritable(82),
                        new IntWritable(83)
                )
        );

        reduceDriver
                .withOutput(new IntWritable(1901), new FloatWritable(81.5f))
                .runTest();
    }
}
