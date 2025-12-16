package org.example.median_temperature;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.hadoop.io.AvroKeyComparator;

public class YearGroupingComparator extends AvroKeyComparator<GenericRecord> {

    @Override
    public int compare(AvroKey<GenericRecord> k1, AvroKey<GenericRecord> k2) {
        GenericRecord r1 = k1.datum();
        GenericRecord r2 = k2.datum();

        return ((Integer) r1.get("year")) .compareTo((Integer) r2.get("year"));
    }
}
