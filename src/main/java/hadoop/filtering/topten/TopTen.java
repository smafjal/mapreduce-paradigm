package hadoop.filtering.topten;

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
import java.util.TreeMap;

public class TopTen extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] othrArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (othrArgs.length != 2) {
            System.out.println("Usages: TopTen <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Top Ten");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TopTen.TopTenMapper.class);
        job.setReducerClass(TopTen.TopTenReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(othrArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(othrArgs[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopTen(), args);
        System.exit(res);
    }

    public static class TopTenReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private TreeMap<Integer, String> topTen;

        public void setup(Reducer<IntWritable, Text, IntWritable, Text>.Context context) {
            this.topTen = new TreeMap<Integer, String>();
        }

        public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) {
            for (Text value : values) {
                this.topTen.put(key.get(), value.toString());
                if (this.topTen.size() > 10) {
                    this.topTen.remove(this.topTen.firstKey());
                }
            }
        }

        public void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            for (Integer key : this.topTen.keySet()) {
                context.write(new IntWritable(key), new Text(this.topTen.get(key)));
            }
        }
    }

    public static class TopTenMapper extends Mapper<Object, Text, IntWritable, Text> {
        TreeMap<Integer, String> topTen;

        public void setup(Mapper<Object, Text, IntWritable, Text>.Context context) {
            this.topTen = new TreeMap<Integer, String>();
        }

        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) {
            String line = value.toString();
            int id = Integer.parseInt(line.split(":")[0]);
            String lineValue = line.split(":")[1];
            this.topTen.put(id, lineValue);
            if (this.topTen.size() > 10) {
                this.topTen.remove(this.topTen.firstKey());
            }

        }

        public void cleanup(Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            for (Integer key : this.topTen.keySet()) {
                context.write(new IntWritable(key), new Text((String) this.topTen.get(key)));
            }
        }
    }
}
