package hadoop.iopattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class RandomDataGenerator extends Configuration implements Tool {
    private static final Logger LOGGER = Logger.getLogger(RandomDataGenerator.class.getName());

    private static final String NUM_MAP_TASKS = "random.generator.map.tasks";
    private static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";

    private void printUsage() {
        System.err.println("Usage: RandomDataGenerator <wordlist> <out> ");
        ToolRunner.printGenericCommandUsage(System.err);
        System.exit(2);
    }

    public int run(String[] strings) throws Exception {
        LOGGER.info("Start random data generation job!");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, strings).getRemainingArgs();
        if (otherArgs.length != 2) {
            printUsage();
        }

        int numMapTasks = 8;
        int numRecordsPerTask = 2;
        Path wordList = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "RandomDataGenerationDriver");
        job.setJarByClass(RandomDataGenerator.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(RandomStackOverflowInputFormat.class);
        RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
        RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
        RandomStackOverflowInputFormat.setRandomWordList(job, wordList);

        TextOutputFormat.setOutputPath(job, outputDir);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RandomDataGenerator(), args);
        System.exit(res);
    }


    public static class RandomStackOverflowInputFormat extends InputFormat<Text, NullWritable> {

        public static void setNumMapTasks(Job job, int numMapTasks) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, numMapTasks);
        }

        public static void setNumRecordPerTask(Job job, int numRecordsPerTask) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, numRecordsPerTask);
        }

        public static void setRandomWordList(Job job, Path wordList) {
            job.addCacheFile(wordList.toUri());
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) {

            // Other way around, give as much FakeInputSplit as you need map
            int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < numSplits; i++) {
                splits.add(new FakeInputSplit());
            }
            return splits;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // Create a new RandomStackOverflowRecordReader and initialize it
            RandomStackOverflowRecordReader rr = new RandomStackOverflowRecordReader();
            rr.initialize(split, context);
            return rr;
        }
    }

    public static class RandomStackOverflowRecordReader extends RecordReader<Text, NullWritable> {
        private int numRecordsToCreate = 0;
        private int createdRecords = 0;
        private Text key = new Text();
        private NullWritable value = NullWritable.get();
        private Random rndm = new Random();
        private ArrayList<String> randomWords = new ArrayList<String>();

        // This object will format the creation date string into a Date object
        private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            // Get the number of records to create from the configuration
            this.numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
            // Get the list of random words from the DistributedCache
            String wordListPath = context.getCacheFiles()[0].toString();

            // Read the list of random words into a list
            BufferedReader rdr = new BufferedReader(new FileReader(wordListPath));
            String line;
            while ((line = rdr.readLine()) != null) {
                randomWords.add(line);
            }
            rdr.close();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            // If we still have records to create
            if (createdRecords < numRecordsToCreate) {
                // Generate random data
                int score = Math.abs(rndm.nextInt()) % 15000;
                int rowId = Math.abs(rndm.nextInt()) % 1000000000;
                int postId = Math.abs(rndm.nextInt()) % 100000000;
                int userId = Math.abs(rndm.nextInt()) % 1000000;
                String creationDate = frmt.format(Math.abs(rndm.nextLong()));
                // Create a string of text from the random words
                String text = getRandomText();
                String randomRecord = "<row Id=\"" + rowId + "\" PostId=\""
                        + postId + "\" Score=\"" + score + "\" Text=\"" + text
                        + "\" CreationDate=\"" + creationDate + "\" UserId\"="
                        + userId + "\" />";
                key.set(randomRecord);
                ++createdRecords;
                return true;
            } else {
                // We are done creating records
                return false;
            }
        }

        @Override
        public Text getCurrentKey() {
            return key;
        }

        @Override
        public NullWritable getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return (float) createdRecords / (float) numRecordsToCreate;
        }

        @Override
        public void close() {
        }

        private String getRandomText() {
            StringBuilder bldr = new StringBuilder();
            int numWords = Math.abs(rndm.nextInt()) % 30 + 1;
            for (int i = 0; i < numWords; ++i) {
                bldr.append(randomWords.get(Math.abs(rndm.nextInt()) % randomWords.size()) + " ");
            }
            return bldr.toString();
        }
    }

    public static class FakeInputSplit extends InputSplit implements Writable {

        @Override
        public void readFields(DataInput arg0) throws IOException {
            // Useless here
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            // Useless here
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            // Fake location
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            // Fake locations
            return new String[0];
        }
    }
}

