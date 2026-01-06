package org.example.median_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearTemperatureKey implements WritableComparable<YearTemperatureKey> {

    private final IntWritable year = new IntWritable();
    private final IntWritable temperature = new IntWritable();

    public YearTemperatureKey() {}

    public YearTemperatureKey(int year, int temperature) {
        this.year.set(year);
        this.temperature.set(temperature);
    }

    public int getYear() {
        return year.get();
    }

    public int getTemperature() {
        return temperature.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(YearTemperatureKey other) {
        int cmp = year.compareTo(other.year);
        if (cmp != 0) return cmp;
        return temperature.compareTo(other.temperature);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof YearTemperatureKey)) return false;
        YearTemperatureKey that = (YearTemperatureKey) o;
        return year.equals(that.year) &&
                temperature.equals(that.temperature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year.get(), temperature.get());
    }
}