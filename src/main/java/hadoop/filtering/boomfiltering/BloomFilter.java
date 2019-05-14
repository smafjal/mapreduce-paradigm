package hadoop.filtering.boomfiltering;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BloomFilter extends Configured implements Tool {
    private final static Logger LOG = Logger.getLogger(BloomFilter.class.getName());

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.out.println("Usages: BloomFilter <bloom_filter_file> <int> <out>");
            System.exit(2);
        }
        LOG.info("Job Name: Bloom Filter");

        Job job = Job.getInstance(conf, "Bloom Filter");
        job.setJarByClass(BloomFilter.class);

        job.addCacheFile(new URI(otherArgs[0]));
        job.setMapperClass(BloomFilter.BloomFilterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int response = ToolRunner.run(new Configuration(), new BloomFilter(), args);
        System.exit(response);
    }

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
        private static HashMap<String, Boolean> bloomFilterCandidate;

        public void setup(Context context) throws IOException {
            URI[] localCachePaths = context.getCacheFiles();
            String filePath = localCachePaths[0].toString();
            LOG.info("Bloom filter file path: " + filePath);

            bloomFilterCandidate = new HashMap<String, Boolean>();
            BufferedReader br = new BufferedReader(new FileReader(filePath));

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("Bloom candidate: " + line);
                if (line.trim().length() != 0) {
                    bloomFilterCandidate.put(line.trim(), true);
                }
            }
            LOG.info("Bloom filter size: " + bloomFilterCandidate.size());
            br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] docs = line.split(":")[1].split("\\s");
            for (String curChar : docs) {
                if (bloomFilterCandidate.containsKey(curChar.trim())) {
                    context.write(value, NullWritable.get());
                    break;
                }
            }
        }
    }
}
