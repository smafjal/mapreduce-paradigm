package hadoop.summarization.medianstd;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MedianStdFinderBasic extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: MedianStdFinder <int> <out>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "Median Std Finder Basic");
        job.setJarByClass(MedianStdFinderBasic.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setMapperClass(MedianStdFinderBasic.MedianStdMapper.class);
        job.setReducerClass(MedianStdFinderBasic.MedianStdReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new MedianStdFinderBasic(), args);
        System.exit(returnCode);
    }

    public static class MedianStdReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdTuple> {
        private ArrayList<Integer> freqList = new ArrayList();
        private MedianStdTuple result = new MedianStdTuple();

        public void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            this.freqList.clear();
            float sum = 0.0F;
            int count = 0;

            for (Iterator itr = value.iterator(); itr.hasNext(); ++count) {
                IntWritable freqValue = (IntWritable) itr.next();
                this.freqList.add(freqValue.get());
                sum += (float) freqValue.get();
            }

            Collections.sort(this.freqList);
            float mean;
            if (count % 2 == 1) {
                mean = (float) (Integer) this.freqList.get(count / 2);
                this.result.setMedian(mean);
            } else {
                mean = (float) (Integer) this.freqList.get(count / 2);
                mean += (float) (Integer) this.freqList.get(count / 2 - 1);
                mean /= 2.0F;
                this.result.setMedian(mean);
            }

            mean = (float) ((double) sum * 1.0D / (double) count);
            float sumOfSqr = 0.0F;

            Integer freq;
            for (Iterator itr = this.freqList.iterator(); itr.hasNext(); sumOfSqr += ((float) freq - mean) * ((float) freq - mean)) {
                freq = (Integer) itr.next();
            }
            float stdValue = (float) Math.sqrt((double) (sumOfSqr / (float) (count - 1)));
            this.result.setStd(stdValue);
            context.write(key, this.result);
        }
    }

    public static class MedianStdMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable keyItem = new IntWritable();
        private IntWritable outValue = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int id = Integer.parseInt(line.split(" ")[0]);
            int freq = Integer.parseInt(line.split(" ")[1]);
            this.keyItem.set(id);
            this.outValue.set(freq);
            context.write(this.keyItem, this.outValue);
        }
    }
}
