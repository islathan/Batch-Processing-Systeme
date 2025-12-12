package org.example.mean_temperature;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountWritable implements Writable {
    private long sum;
    private long count;

    public SumCountWritable() {
        this.sum = 0;
        this.count = 0;
    }

    public SumCountWritable(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    // Getter-Methoden
    public long getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    // Writable-Methoden
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readLong();
        count = in.readLong();
    }

    //equals and hashCode must be overriden, as otherwise combiner cannot be tested (memory address is checked instead of actual values)
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumCountWritable that = (SumCountWritable) o;
        return sum == that.sum && count == that.count;
    }

    @Override
    public int hashCode() {
        long result = sum;
        result = 31 * result + count;
        return (int)result;
    }
}