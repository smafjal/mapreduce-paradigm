package hadoop.summarization.minmax;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable {
    private int min;
    private int max;
    private int count;

    public MinMaxTuple() {
    }

    public void readFields(DataInput in) throws IOException {
        this.min = in.readInt();
        this.max = in.readInt();
        this.count = in.readInt();
    }

    public int getMin() {
        return this.min;
    }

    public int getMax() {
        return this.max;
    }

    public int getCount() {
        return this.count;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.min);
        out.writeInt(this.max);
        out.writeInt(this.count);
    }

    public String toString() {
        return "Min: " + this.min + " Max: " + this.max + " Count: " + this.count;
    }

    public int hashCode() {
        int result = 31 * +(this.count ^ this.count >>> 30);
        result = 31 * result + this.max;
        result = 31 * result + this.max;
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            MinMaxTuple other = (MinMaxTuple) obj;
            if (this.count != other.count) {
                return false;
            } else if (this.min != other.min) {
                return false;
            } else {
                return this.max == other.max;
            }
        }
    }
}
