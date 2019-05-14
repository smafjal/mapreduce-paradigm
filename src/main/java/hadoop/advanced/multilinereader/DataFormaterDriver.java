package hadoop.advanced.multilinereader;


import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataFormaterDriver extends Configuration implements Tool {
    private static final Logger LOGGER = Logger.getLogger(DataFormaterDriver.class.getName());

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usages: DataFormaterDriver <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Multiline input reader job");
        job.setJarByClass(DataFormaterDriver.class);
        job.setMapperClass(DataFormaterDriver.DataMapper.class);
        job.setReducerClass(DataFormaterDriver.DataReducer.class);
        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(args[1]), true);
        job.setInputFormatClass(MultiLineInputFormat.class);
        MultiLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration conf) {
    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new DataFormaterDriver(), args);
        System.exit(returnCode);
    }

    public static class DataReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Text v;
            for (Iterator valItr = values.iterator(); valItr.hasNext(); sum += Integer.parseInt(v.toString())) {
                v = (Text) valItr.next();
            }
            context.write(key, new Text(Integer.toString(sum)));
        }
    }

    public static class DataMapper extends Mapper<Object, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int sum = 0;
            sum = sum + Integer.parseInt(value.toString().split("\\s")[0]);
            sum += Integer.parseInt(value.toString().split("\\s")[1]);
            context.write(key, new Text(Integer.toString(sum)));
        }
    }
}

