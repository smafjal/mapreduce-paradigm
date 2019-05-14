package hadoop.advanced.multilinereader;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

public class MultiLineInputFormat extends NLineInputFormat {
    public MultiLineInputFormat() {
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) {
        context.setStatus(genericSplit.toString());
        return new MultiLineInputFormat.MultiLineRecordReader();
    }

    public static class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
        private int NLINESTOPROCESS;
        private LineReader in;
        private LongWritable key;
        private Text value = new Text();
        private long start = 0L;
        private long end = 0L;
        private long pos = 0L;
        private int maxLineLength;

        public MultiLineRecordReader() {
        }

        public void close() throws IOException {
            if (this.in != null) {
                this.in.close();
            }

        }

        public LongWritable getCurrentKey() {
            return this.key;
        }

        public Text getCurrentValue() {
            return this.value;
        }

        public float getProgress() {
            return this.start == this.end ? 0.0F : Math.min(1.0F, (float) (this.pos - this.start) / (float) (this.end - this.start));
        }

        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            this.NLINESTOPROCESS = NLineInputFormat.getNumLinesPerSplit(context);
            FileSplit split = (FileSplit) genericSplit;
            Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            this.maxLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
            FileSystem fs = file.getFileSystem(conf);
            this.start = split.getStart();
            this.end = this.start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());
            if (this.start != 0L) {
                skipFirstLine = true;
                --this.start;
                filein.seek(this.start);
            }

            this.in = new LineReader(filein, conf);
            if (skipFirstLine) {
                this.start += (long) this.in.readLine(new Text(), 0, (int) Math.min(2147483647L, this.end - this.start));
            }

            this.pos = this.start;
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.key == null) {
                this.key = new LongWritable();
            }

            this.key.set(this.pos);
            if (this.value == null) {
                this.value = new Text();
            }

            this.value.clear();
            Text endline = new Text("\n");
            int newSize = 0;

            for (int i = 0; i < this.NLINESTOPROCESS; ++i) {
                Text v = new Text();

                while (this.pos < this.end) {
                    newSize = this.in.readLine(v, this.maxLineLength, Math.max((int) Math.min(2147483647L, this.end - this.pos), this.maxLineLength));
                    this.value.append(v.getBytes(), 0, v.getLength());
                    this.value.append(endline.getBytes(), 0, endline.getLength());
                    if (newSize == 0) {
                        break;
                    }

                    this.pos += (long) newSize;
                    if (newSize < this.maxLineLength) {
                        break;
                    }
                }
            }

            if (newSize == 0) {
                this.key = null;
                this.value = null;
                return false;
            } else {
                return true;
            }
        }
    }
}
