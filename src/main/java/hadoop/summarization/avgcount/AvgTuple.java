package hadoop.summarization.avgcount;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AvgTuple implements Writable {
    private float count = 0.0F;
    private float average = 0.0F;

    public AvgTuple() {
    }

    public void readFields(DataInput in) throws IOException {
        this.count = in.readFloat();
        this.average = in.readFloat();
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.count);
        out.writeFloat(this.average);
    }

    public float getCount() {
        return this.count;
    }

    public void setCount(float count) {
        this.count = count;
    }

    public float getAverage() {
        return this.average;
    }

    public void setAverage(float average) {
        this.average = average;
    }

    public String toString() {
        return this.count + "\t" + this.average;
    }

    public int hashCode() {
        float result = 31 * +this.average;
        result = 31 * result + this.count;
        return (int) result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            AvgTuple other = (AvgTuple) obj;
            if (this.average != other.average) {
                return false;
            } else {
                return this.count == other.count;
            }
        }
    }
}

