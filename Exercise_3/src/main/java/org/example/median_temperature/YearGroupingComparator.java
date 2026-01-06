package org.example.median_temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearGroupingComparator extends WritableComparator {

    protected YearGroupingComparator() {
        super(YearTemperatureKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearTemperatureKey k1 = (YearTemperatureKey) a;
        YearTemperatureKey k2 = (YearTemperatureKey) b;
        return Integer.compare(k1.getYear(), k2.getYear());
    }
}

