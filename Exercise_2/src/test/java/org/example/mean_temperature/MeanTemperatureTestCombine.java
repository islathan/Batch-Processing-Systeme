package org.example.mean_temperature;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MeanTemperatureTestCombine {

    private MapDriver<LongWritable, Text, Text, SumCountWritable> mapDriver;
    private ReduceDriver<Text, SumCountWritable, Text, DoubleWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, SumCountWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        RecordMapperCombine mapper = new RecordMapperCombine();
        MeanTemperatureReducerCombine reducer = new MeanTemperatureReducerCombine();
        MeanTemperatureCombiner combiner = new MeanTemperatureCombiner();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.withCombiner(combiner);
    }

    @Test
    public void testCombiner() throws IOException {
        ReduceDriver<Text, SumCountWritable, Text, SumCountWritable> combinerDriver =
                ReduceDriver.newReduceDriver(new MeanTemperatureCombiner());

        combinerDriver.withInput(new Text("1901"), Arrays.asList(
                new SumCountWritable(120, 1),
                new SumCountWritable(100, 1),
                new SumCountWritable(200, 1)
        ));

        // Sum is 120 + 100 + 200 = 420. Count is: 3.
        combinerDriver.withOutput(new Text("1901"), new SumCountWritable(420, 3));

        combinerDriver.runTest();
    }

    @Test
    public void testReducerFinal() throws IOException {
        List<SumCountWritable> combinerOutputs = Arrays.asList(
                // Aggregation 1: from Mapper A
                new SumCountWritable(220, 2),
                // Aggregation 2: from Mapper B
                new SumCountWritable(200, 1)
        );

        reduceDriver.withInput(new Text("1901"), combinerOutputs);

        // overall sum: 220 + 200 = 420
        // overall count: 2 + 1 = 3
        // mean: 420 / 3 = 140
        reduceDriver.withOutput(new Text("1901"), new DoubleWritable(14.0)); // considering division /10

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduceWithCombiner() throws IOException {
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
        mapReduceDriver.withInput(new LongWritable(3), new Text(line4));

        // 1901: (-78 + -80) / 2 = -79.0
        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-7.9));
        // 1902: (0 + 100) / 2 = 50.0
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
        String line = "0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9+99999+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9+99999+99999102001ADDGF108991999999999999999999";
        String line3 = "0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9/00801+A9999102001ADDGF108991999999999999999999";
        mapDriver.withInput(new LongWritable(0), new Text(line));
        mapDriver.withInput(new LongWritable(1), new Text(line2));
        mapDriver.withInput(new LongWritable(2), new Text(line3));
        mapDriver.run();

        long counterMissing = mapDriver.getCounters()
                .findCounter("org.example.mean_temperature.RecordMapperCombine$FaultyTemperatureCounter", "MISSING")
                .getValue();

        long counterMalformed = mapDriver.getCounters()
                .findCounter("org.example.mean_temperature.RecordMapperCombine$FaultyTemperatureCounter", "MALFORMED")
                .getValue();

        assertEquals(2, counterMissing);
        assertEquals(1, counterMalformed);
    }

}