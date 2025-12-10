package org.example.mean_temperature;

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

public class MeanTemperatureTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        RecordMapper mapper = new RecordMapper();
        MeanTemperatureReducer reducer = new MeanTemperatureReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        String line = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+99999+99999102001ADDGF108991999999999999999999";
        mapDriver.withInput(new LongWritable(0), new Text(line));
        mapDriver.withInput(new LongWritable(1), new Text(line2));

        mapDriver.withOutput(new Text("1901"), new IntWritable(-78));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("1901"), Arrays.asList(
                new IntWritable(120),
                new IntWritable(100),
                new IntWritable(200)
        ));

        reduceDriver.withOutput(new Text("1901"), new DoubleWritable(14.0)); // mean = 14.0 -> 14.0 after /10

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        String line1 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00800+99999102001ADDGF108991999999999999999999";
        String line3 = "0029029070999991902010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+00000+99999102001ADDGF108991999999999999999999";
        String line4 = "0029029070999991902010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+01000+99999102001ADDGF108991999999999999999999";

        mapReduceDriver.withInput(new LongWritable(0), new Text(line1));
        mapReduceDriver.withInput(new LongWritable(1), new Text(line2));
        mapReduceDriver.withInput(new LongWritable(2), new Text(line3));
        mapReduceDriver.withInput(new LongWritable(2), new Text(line4));

        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-7.9));
        mapReduceDriver.withOutput(new Text("1902"), new DoubleWritable(5.0));

        mapReduceDriver.runTest();
    }

    @Test
    public void testInvalidRecordFiltered() throws IOException {
        String validLine = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        String invalidLine = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+99999+99999102001ADDGF108991999999999999999999";

        mapReduceDriver.withInput(new LongWritable(0), new Text(validLine));
        mapReduceDriver.withInput(new LongWritable(1), new Text(invalidLine));

        // Only the valid year should appear
        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-7.8));

        mapReduceDriver.runTest();
    }

    @Test
    public void testQualityCodeCounter() throws Exception {
        String line = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00801+99999102001ADDGF108991999999999999999999";
        String line3 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00801+A9999102001ADDGF108991999999999999999999";
        mapDriver.withInput(new LongWritable(0), new Text(line));
        mapDriver.withInput(new LongWritable(1), new Text(line2));
        mapDriver.withInput(new LongWritable(2), new Text(line3));
        mapDriver.run();

        long countNine = mapDriver.getCounters()
                .findCounter("QualityCodes", "9")
                .getValue();

        long countA = mapDriver.getCounters()
                .findCounter("QualityCodes", "A")
                .getValue();

        long countFour = mapDriver.getCounters()
                .findCounter("QualityCodes", "4")
                .getValue();

        assertEquals(2, countNine);
        assertEquals(1, countA);
        assertEquals(0, countFour);
    }

    @Test
    public void testFaultyTemperatureCounter() throws Exception {
        String line = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+09999+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+09999+99999102001ADDGF108991999999999999999999";
        String line3 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9a00801+A9999102001ADDGF108991999999999999999999";
        mapDriver.withInput(new LongWritable(0), new Text(line));
        mapDriver.withInput(new LongWritable(1), new Text(line2));
        mapDriver.withInput(new LongWritable(2), new Text(line3));
        mapDriver.run();

        long counterMissing = mapDriver.getCounters()
                .findCounter("FaultyTemperatureCounter", "MISSING")
                .getValue();

        long counterMalformed = mapDriver.getCounters()
                .findCounter("FaultyTemperatureCounter", "MALFORMED")
                .getValue();

        assertEquals(2, counterMissing);
        assertEquals(1, counterMalformed);
    }

}