package hadoop.summarization.medianstd;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

public class MedianStdFinderAdvanced extends Configured implements Tool {
    private final static Logger LOG = Logger.getLogger(MedianStdFinderAdvanced.class.getName());

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: MedianStdFinder <int> <out>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "Median Std Finder Advanced");
        job.setJarByClass(MedianStdFinderAdvanced.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MedianStdFinderAdvanced.MedianStdMapper.class);
        job.setCombinerClass(MedianStdFinderAdvanced.MedianStdCombiner.class);
        job.setReducerClass(MedianStdFinderAdvanced.MedianStdReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianStdTuple.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new MedianStdFinderAdvanced(), args);
        System.exit(returnCode);
    }

    public static class MedianStdReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdTuple> {
        private MedianStdTuple result = new MedianStdTuple();
        private TreeMap sortedFreq = new TreeMap();

        public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
            this.sortedFreq.clear();
            this.result.setStd(0.0F);
            this.result.setMedian(0.0F);
            float sum = 0.0F;
            int totalCount = 0;

            for (SortedMapWritable curValue : values) {
                for (Map.Entry<WritableComparable, Writable> val : curValue.entrySet()) {
                    Map.Entry<WritableComparable, Writable> entry = (Map.Entry) val;
                    int freq = ((IntWritable) entry.getKey()).get();
                    int count = ((IntWritable) entry.getValue()).get();
                    totalCount += count;
                    sum += (float) (freq * count);
                    Integer sortedCount = (Integer) this.sortedFreq.get(freq);
                    if (sortedCount == null) {
                        this.sortedFreq.put(freq, count);
                    } else {
                        this.sortedFreq.put(freq, sortedCount + count);
                    }
                }
            }

            int medianIdx = totalCount / 2;
            int prvCount = 0;
            int prvKey = 0;

            Map.Entry entry;
            for (Iterator itr = this.sortedFreq.entrySet().iterator(); itr.hasNext(); prvKey = (Integer) entry.getKey()) {
                entry = (Map.Entry) itr.next();
                int count = prvCount + (Integer) entry.getValue();
                if (prvCount <= medianIdx && medianIdx < count) {
                    if (totalCount % 2 == 0 && prvCount == medianIdx) {
                        this.result.setMedian((float) ((Integer) entry.getKey() + prvKey) / 2.0F);
                    } else {
                        this.result.setMedian((float) (Integer) entry.getKey());
                    }
                    break;
                }
                prvCount = count;
            }

            float mean = sum * 1.0F / (float) totalCount;
            float sumOfSqr = 0.0F;

            for (Iterator itr = this.sortedFreq.entrySet().iterator(); itr.hasNext(); sumOfSqr += ((float) (Integer) entry.getKey() - mean) * ((float) (Integer) entry.getKey() - mean) * (float) (Integer) entry.getValue()) {
                entry = (Map.Entry) itr.next();
            }
            float stdValue = (float) Math.sqrt((double) (sumOfSqr / (float) (totalCount - 1)));
            this.result.setStd(stdValue);
            context.write(key, this.result);
        }
    }

    public static class MedianStdCombiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {

        protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
            SortedMapWritable outValue = new SortedMapWritable();

            for (SortedMapWritable curValue : values) {
                for (Map.Entry<WritableComparable, Writable> val : curValue.entrySet()) {
                    Map.Entry<WritableComparable, Writable> entry = (Map.Entry) val;
                    IntWritable freqValue = (IntWritable) outValue.get(entry.getKey());
                    if (freqValue == null) {
                        outValue.put(entry.getKey(), new IntWritable(((IntWritable) entry.getValue()).get()));
                    } else {
                        freqValue.set(freqValue.get() + ((IntWritable) entry.getValue()).get());
                    }
                }
            }
            context.write(key, outValue);
        }
    }

    public static class MedianStdMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {
        private IntWritable keyItem = new IntWritable();
        private static final IntWritable ONE = new IntWritable(1);
        private IntWritable freqValue = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int id = Integer.parseInt(line.split(" ")[0]);
            int freq = Integer.parseInt(line.split(" ")[1]);
            this.keyItem.set(id);
            this.freqValue.set(freq);
            SortedMapWritable outValue = new SortedMapWritable();
            outValue.put(this.freqValue, ONE);
            context.write(this.keyItem, outValue);
        }
    }
}
