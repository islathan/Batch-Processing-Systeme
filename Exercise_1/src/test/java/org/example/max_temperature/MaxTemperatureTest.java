package org.example.max_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class MaxTemperatureTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        RecordMapper mapper = new RecordMapper();
        MaxTemperatureReducer reducer = new MaxTemperatureReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException, InterruptedException {
        String line = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        mapDriver.withInput(new LongWritable(0), new Text(line));

        mapDriver.withOutput(new Text("1901"), new IntWritable(-78));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("1901"), Arrays.asList(
                new IntWritable(-78),
                new IntWritable(-10),
                new IntWritable(-55)
        ));

        reduceDriver.withOutput(new Text("1901"), new DoubleWritable(-1.0)); // max = -78 -> -7.8 after /10

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException, InterruptedException {
        String line1 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991902010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+01230+99999102001ADDGF108991999999999999999999";

        mapReduceDriver.withInput(new LongWritable(0), new Text(line1));
        mapReduceDriver.withInput(new LongWritable(1), new Text(line2));

        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-7.8));
        mapReduceDriver.withOutput(new Text("1902"), new DoubleWritable(12.3));

        mapReduceDriver.runTest();
    }

    @Test
    public void testInvalidRecordFiltered() throws IOException, InterruptedException {
        String validLine = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        String invalidLine = "0029029070999991903010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+99999+99999102001ADDGF108991999999999999999999";

        mapReduceDriver.withInput(new LongWritable(0), new Text(validLine));
        mapReduceDriver.withInput(new LongWritable(1), new Text(invalidLine));

        // Only the valid year should appear
        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-7.8));

        mapReduceDriver.runTest();
    }

}
