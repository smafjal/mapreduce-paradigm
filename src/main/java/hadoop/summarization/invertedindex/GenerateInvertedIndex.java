package hadoop.summarization.invertedindex;


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

import java.io.IOException;

public class GenerateInvertedIndex extends Configured implements Tool {
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: MinMaxFinder <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Inverted Index Finder");
        job.setJarByClass(GenerateInvertedIndex.class);
        job.setMapperClass(GenerateInvertedIndex.InvertedMapper.class);
        job.setReducerClass(GenerateInvertedIndex.InverterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new GenerateInvertedIndex(), args);
        System.exit(returnCode);
    }

    public static class InverterReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder line = new StringBuilder(":");
            for (IntWritable docID : values) {
                line.append(" ").append(docID);
            }
            line = new StringBuilder(line.toString().trim());
            context.write(key, new Text(line.toString()));
        }
    }

    public static class InvertedMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable outvalue = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int id = Integer.parseInt(line.split(":")[0]);
            String[] docs = line.split(":")[1].trim().split(" ");
            this.outvalue.set(id);

            for (String word : docs) {
                context.write(new Text(word.trim()), this.outvalue);
            }
        }
    }
}
