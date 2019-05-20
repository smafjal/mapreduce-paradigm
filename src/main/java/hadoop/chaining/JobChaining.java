package hadoop.chaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Job Chaining is an important design to glue multiple map-reduce.
 * Problem: Given users list is which is like: [id,firstName,lastName,reputation]. Given comments list which is like: [id, comments]
 */

public class JobChaining extends Configuration implements Tool {

    private final static Logger LOGGER = Logger.getLogger(JobChaining.class.getName());

    public static final String AVERAGE_CALC_GROUP = "average.calc.group";
    public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.user";
    public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "multiplebelow";
    public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "multipleabove";
    public static final String RECORDS_COUNTER_NAME = "Records";
    public static final String USERS_COUNTER_NAME = "Users";


    public static class UserIdBinningMapper extends Mapper<Object, Text, Text, Text> {
        private double average = 0.0;
        private MultipleOutputs<Text, Text> mos = null;
        private Text outkey = new Text();
        private Text outvalue = new Text();
        private HashMap<String, String> userIdToReputation = null;

        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            average = Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
            userIdToReputation = new HashMap<String, String>();
            mos = new MultipleOutputs<Text, Text>(context);

            URI[] localCacheFiles = context.getCacheFiles();
            for (URI p : localCacheFiles) {
                LOGGER.info("Setup---> localCacheFile: " + p.toString());
                BufferedReader br = new BufferedReader(new FileReader(p.toString()));
                String line;
                while ((line = br.readLine()) != null) {
                    // line format:
                    // 4 firstName: aooul lastName: mrham email: aahqc@gmail.com reputation: 29
                    String[] lineSegments = line.split("\\s");
                    String userId = lineSegments[0];
                    String reputation = lineSegments[lineSegments.length - 1];
                    userIdToReputation.put(userId, reputation);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            String userId = tokens[0];
            int posts = Integer.parseInt(tokens[1]);

            outkey.set(userId);
            outvalue.set((long) posts + "\t" + userIdToReputation.get(userId));

            if ((double) posts < average) {
                mos.write(MULTIPLE_OUTPUTS_BELOW_NAME, outkey, outvalue, MULTIPLE_OUTPUTS_BELOW_NAME + "/part");
            } else {
                mos.write(MULTIPLE_OUTPUTS_ABOVE_NAME, outkey, outvalue, MULTIPLE_OUTPUTS_ABOVE_NAME + "/part");
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static class UserIdCountMapper extends Mapper<Object, Text, Text, LongWritable> {
        private Text outkey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String userId = line.split("\\s")[0];
            if (userId != null) {
                outkey.set(userId);
                context.write(outkey, new LongWritable(1));
                context.getCounter(AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME).increment(1);
            }
        }
    }

    public static class UserIdSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable outvalue = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME).increment(1);
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            outvalue.set(sum);
            context.write(key, outvalue);
        }
    }

    private void printUsage() {
        System.err.println("Usage: JobChaining <comments_in> <user_in> <out>");
        ToolRunner.printGenericCommandUsage(System.err);
        System.exit(2);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            printUsage();
        }

        Path postInput = new Path(otherArgs[0]);
        Path userInput = new Path(otherArgs[1]);
        Path outputDirIntermediate = new Path(otherArgs[2] + "_int");
        Path outputDir = new Path(otherArgs[2]);

        LOGGER.info("PostInput Path: " + postInput.toString() +
                "\nUserInput Path: " + userInput.toString() +
                "\nOutputDir: " + outputDir.toString());

        Job countingJob = Job.getInstance(conf, "JobChaining-Counting");
        countingJob.setJarByClass(JobChaining.class);

        // we can use the API long sum reducer for a combiner!
        countingJob.setMapperClass(UserIdCountMapper.class);
        countingJob.setCombinerClass(LongSumReducer.class);
        countingJob.setReducerClass(UserIdSumReducer.class);

        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(LongWritable.class);

        countingJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(countingJob, postInput);

        countingJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);

        int code = countingJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            double numRecords = (double) countingJob.getCounters()
                    .findCounter(AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME)
                    .getValue();

            double numUsers = (double) countingJob.getCounters()
                    .findCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME)
                    .getValue();

            double averagePostsPerUser = numRecords / numUsers;
            LOGGER.info("averagePostsPerUser: " + averagePostsPerUser);

            Configuration confBinning = new Configuration();
            confBinning.set(AVERAGE_POSTS_PER_USER, Double.toString(averagePostsPerUser));
            Job binningJob = Job.getInstance(confBinning, "JobChaining-Binning");
            binningJob.setJarByClass(JobChaining.class);

            binningJob.setMapperClass(UserIdBinningMapper.class);
            binningJob.setNumReduceTasks(0);

            binningJob.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(binningJob, outputDirIntermediate);

            MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class, Text.class, Text.class);

            MultipleOutputs.setCountersEnabled(binningJob, true);
            TextOutputFormat.setOutputPath(binningJob, outputDir);

            binningJob.addCacheFile(new URI(userInput.toString()));
            code = binningJob.waitForCompletion(true) ? 0 : 1;
        }
        FileSystem.get(conf).delete(outputDirIntermediate, true);
        return code;
    }

    public void setConf(Configuration configuration) {
    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new Configuration(), new JobChaining(), args);
        System.exit(returnCode);
    }
}
