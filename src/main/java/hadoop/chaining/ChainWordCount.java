package hadoop.chaining;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * A simple Mapchainer/ReduceCHainer example. Collected from stacjoverflow
 * url: https://stackoverflow.com/questions/6840922/hadoop-mapreduce-driver-for-chaining-mappers-within-a-mapreduce-job/
 *
 * */

public class ChainWordCount extends Configured implements Tool {
    private static Logger LOG = Logger.getLogger(ChainWordCount.class.getName());

    public static class Tokenizer extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            LOG.info("Line:" + line);
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class UpperCaser extends MapReduceBase implements Mapper<Text, IntWritable, Text, IntWritable> {
        public void map(Text key, IntWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String word = key.toString().toUpperCase();
            LOG.info("UpperCase:" + word);
            output.collect(new Text(word), value);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            LOG.info("Word:" + key.toString() + "\tCount:" + sum);
            output.collect(key, new IntWritable(sum));
        }
    }

    private static int printUsage() {
        System.out.println("ChainWordCount <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), ChainWordCount.class);
        conf.setJobName("chain wordCount");

        if (args.length != 2) {
            System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        JobConf mapAConf = new JobConf(false);
        ChainMapper.addMapper(conf, Tokenizer.class, LongWritable.class, Text.class, Text.class, IntWritable.class, true, mapAConf);

        JobConf mapBConf = new JobConf(false);
        ChainMapper.addMapper(conf, UpperCaser.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, mapBConf);

        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(conf, Reduce.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, reduceConf);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ChainWordCount(), args);
        System.exit(res);
    }
}