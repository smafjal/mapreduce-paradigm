package hadoop.summarization.medianstd;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianStdTuple implements Writable {
    float median;
    float std;

    public MedianStdTuple() {
    }

    public float getMedian() {
        return this.median;
    }

    public float getStd() {
        return this.std;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public void setStd(float std) {
        this.std = std;
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.median);
        out.writeFloat(this.std);
    }

    public void readFields(DataInput in) throws IOException {
        this.median = in.readFloat();
        this.std = in.readFloat();
    }

    public String toString() {
        return this.median + " " + this.std;
    }

    public int hashCode() {
        float result = 31 * +this.median;
        result = 31 * result + this.std;
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
            MedianStdTuple other = (MedianStdTuple) obj;
            if (this.median != other.median) {
                return false;
            } else {
                return this.std == other.std;
            }
        }
    }
}
