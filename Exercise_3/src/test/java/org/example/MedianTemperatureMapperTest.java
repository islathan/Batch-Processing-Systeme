package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.example.median_temperature.MedianTemperatureMapper;
import org.example.median_temperature.YearTemperatureKey;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MedianTemperatureMapperTest {

    private MapDriver<LongWritable, Text,
            YearTemperatureKey, IntWritable> mapDriver;

    @Before
    public void setup() throws IOException {
        MedianTemperatureMapper mapper = new MedianTemperatureMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperValidTemperature() throws Exception {

        String line =
                "0029029070999991901010106004+64333+023450FM-12+" +
                        "000599999V0202701N015919999999N0000001N9+00811+99999102001ADDGF108991999999999999999999";

        mapDriver
                .withInput(new LongWritable(0), new Text(line))
                .withOutput(
                        new YearTemperatureKey(1901, 81),
                        new IntWritable(81)
                )
                .runTest();
    }

    @Test
    public void testMapperIgnoresInvalidTemperature() throws Exception {

        String line =
                "0029029070999991901010106004+64333+023450FM-12+" +
                        "000599999V0202701N015919999999N0000001N9+99991+99999102001ADDGF108991999999999999999999";

        mapDriver
                .withInput(new LongWritable(0), new Text(line))
                .runTest(); // no output expected
    }
}
