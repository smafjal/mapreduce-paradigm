package hadoop.summarization.minmax;

import java.io.IOException;
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

public class MinMaxFinder extends Configured implements Tool {
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: MinMaxFinder <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MinMax Finder");
        job.setJarByClass(MinMaxFinder.class);
        job.setMapperClass(MinMaxFinder.MinMaxMapper.class);
        job.setCombinerClass(MinMaxFinder.MinMaxReducer.class);
        job.setReducerClass(MinMaxFinder.MinMaxReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MinMaxTuple.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new MinMaxFinder(), args);
        System.exit(returnCode);
    }

    public static class MinMaxReducer extends Reducer<IntWritable, MinMaxTuple, IntWritable, MinMaxTuple> {
        private MinMaxTuple result = new MinMaxTuple();

        public void reduce(IntWritable key, Iterable<MinMaxTuple> value, Context context) throws IOException, InterruptedException {
            int localMin = 1073741824;
            int localMax = -1073741824;
            int sum = 0;

            MinMaxTuple minMaxTuple;
            Iterator valItr = value.iterator();
            while (valItr.hasNext()) {
                minMaxTuple = (MinMaxTuple) valItr.next();
                localMin = Math.min(minMaxTuple.getMin(), localMin);
                localMax = Math.max(minMaxTuple.getMax(), localMax);
                sum += minMaxTuple.getCount();
            }
            this.result.setMin(localMin);
            this.result.setMax(localMax);
            this.result.setCount(sum);
            context.write(key, this.result);
        }
    }

    public static class MinMaxMapper extends Mapper<Object, Text, IntWritable, MinMaxTuple> {
        private IntWritable keyItem = new IntWritable();
        private MinMaxTuple minMaxTuple = new MinMaxTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int id = Integer.parseInt(line.split(" ")[0]);
            int freq = Integer.parseInt(line.split(" ")[1]);
            this.keyItem.set(id);
            this.minMaxTuple.setMax(freq);
            this.minMaxTuple.setMin(freq);
            this.minMaxTuple.setCount(1);
            context.write(this.keyItem, this.minMaxTuple);
        }
    }
}
