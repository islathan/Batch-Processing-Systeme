package org.example.median_temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearTemperatureSortComparator extends WritableComparator {

    protected YearTemperatureSortComparator() {
        super(YearTemperatureKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        YearTemperatureKey k1 = (YearTemperatureKey) w1;
        YearTemperatureKey k2 = (YearTemperatureKey) w2;

        int cmp = Integer.compare(k1.getYear(), k2.getYear());
        if (cmp != 0) return cmp;

        return Integer.compare(k1.getTemperature(), k2.getTemperature());
    }
}
