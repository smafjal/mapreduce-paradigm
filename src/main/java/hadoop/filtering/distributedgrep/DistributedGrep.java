package hadoop.filtering.distributedgrep;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedGrep extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] othrArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (othrArgs.length != 2) {
            System.out.println("Usages: Distinct <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Distributed Grep");
        job.setJarByClass(DistributedGrep.class);
        job.setMapperClass(DistributedGrep.DistributedGrepMapper.class);
        job.setReducerClass(DistributedGrep.DistributedGrepReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(othrArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(othrArgs[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistributedGrep(), args);
        System.exit(res);
    }

    public static class DistributedGrepReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
        private IntWritable keyValue = new IntWritable();


        public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            this.keyValue.set(key.get());
            context.write(this.keyValue, NullWritable.get());
        }
    }

    public static class DistributedGrepMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
        private IntWritable keyValue = new IntWritable();

        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int id = Integer.parseInt(line.split(":")[0]);
            this.keyValue.set(id);
            context.write(this.keyValue, NullWritable.get());
        }
    }
}
