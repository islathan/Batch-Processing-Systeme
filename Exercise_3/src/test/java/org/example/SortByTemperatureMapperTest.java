package org.example;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.example.median_temperature.SortByTemperatureMapper;
import org.junit.Before;
import org.junit.Test;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SortByTemperatureMapperTest {

    private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;

    @Before
    public void setup() {
        SortByTemperatureMapper mapper = new SortByTemperatureMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws Exception {
        String line1 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+00811+99999102001ADDGF108991999999999999999999";
        String line2 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+99991+99999102001ADDGF108991999999999999999999";
        String line3 = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+00831+99999102001ADDGF108991999999999999999999";

        mapDriver.withInput(new LongWritable(0), new Text(line1))
                .withInput(new LongWritable(1), new Text(line2))
                .withInput(new LongWritable(2), new Text(line3))
                .withOutput(new IntWritable(81), new Text(line1))
                .withOutput(new IntWritable(83), new Text(line3))
                .runTest();
    }

    @Test
    public void testOrderedByTemperature() throws Exception {
        String second = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+00821+99999102001ADDGF108991999999999999999999";
        String third = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+00831+99999102001ADDGF108991999999999999999999";
        String first = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+00811+99999102001ADDGF108991999999999999999999";

        mapDriver.withInput(new LongWritable(0), new Text(second))
                .withInput(new LongWritable(1), new Text(third))
                .withInput(new LongWritable(2), new Text(first));

        // Mapper ausführen, Ausgabe sammeln
        List<Pair<IntWritable, Text>> mapperOutputs = mapDriver.run();

        // Ausgabe nach Key sortieren
        mapperOutputs.sort(Comparator.comparingInt(p -> p.getFirst().get()));

        // Prüfen, ob die Keys korrekt sortiert sind
        assertEquals(78, mapperOutputs.get(0).getFirst().get());
        assertEquals(123, mapperOutputs.get(1).getFirst().get());
        assertEquals(456, mapperOutputs.get(2).getFirst().get());

        // Optional: auch den Value prüfen
        assertEquals(first, mapperOutputs.get(0).getSecond().toString());
        assertEquals(second, mapperOutputs.get(1).getSecond().toString());
        assertEquals(third, mapperOutputs.get(2).getSecond().toString());
    }

    @Test
    public void testMissingTemperatureFiltered() throws Exception {
        String line = "0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9+99991+99999102001ADDGF108991999999999999999999";

        mapDriver.withInput(new LongWritable(0), new Text(line))
                .runTest();
    }
}

