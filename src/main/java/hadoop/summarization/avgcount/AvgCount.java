package hadoop.summarization.avgcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

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

public class AvgCount extends Configured implements Tool {
    private final static Logger LOG = Logger.getLogger(AvgCount.class.getName());

    public int run(String[] args) throws Exception {
        LOG.info("Application Name: Average Count!");

        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: AvgCount <in> <out>");
            System.exit(2);
        }

        LOG.info("Input Path: " + otherArgs[0]);
        LOG.info("Output Path: " + otherArgs[1]);

        Job job = Job.getInstance(conf, "Average Count");
        job.setJarByClass(AvgCount.class);

        job.setMapperClass(AvgCount.AvgMapper.class);
        job.setCombinerClass(AvgCount.AvgReducer.class);
        job.setReducerClass(AvgCount.AvgReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AvgTuple.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new AvgCount(), args);
        System.exit(returnCode);
    }

    public static class AvgReducer extends Reducer<IntWritable, AvgTuple, IntWritable, AvgTuple> {
        private AvgTuple result = new AvgTuple();

        public void reduce(IntWritable key, Iterable<AvgTuple> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0F;
            float count = 0.0F;

            AvgTuple avgTuple;
            for (Iterator itr = values.iterator(); itr.hasNext(); count += avgTuple.getCount()) {
                avgTuple = (AvgTuple) itr.next();
                sum += avgTuple.getCount() * avgTuple.getAverage();
            }
            this.result.setCount(count);
            this.result.setAverage(sum / count);
            context.write(key, this.result);
        }
    }

    public static class AvgMapper extends Mapper<Object, Text, IntWritable, AvgTuple> {
        private IntWritable currentIdx = new IntWritable();
        private AvgTuple avgTuple = new AvgTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int idx = Integer.parseInt(line.split("\\s")[0]);
            int freq = Integer.parseInt(line.split("\\s")[1]);

            this.currentIdx.set(idx);
            this.avgTuple.setCount(1.0F);
            this.avgTuple.setAverage((float) freq);
            context.write(this.currentIdx, this.avgTuple);
        }
    }
}
