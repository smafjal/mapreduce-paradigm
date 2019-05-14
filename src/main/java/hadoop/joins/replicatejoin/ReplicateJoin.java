package hadoop.joins.replicatejoin;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReplicateJoin extends Configuration implements Tool {
    private static Logger LOGGER = Logger.getLogger(ReplicateJoin.class.getName());

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 4) {
            this.printUsage();
        }

        System.out.println("User Path: " + otherArgs[0]);
        System.out.println("Comments Path: " + otherArgs[1]);
        LOGGER.info("UserPath: " + otherArgs[0]);
        LOGGER.info("CommentsPath: " + otherArgs[1]);
        Job job = Job.getInstance(conf, "Replicate Join (Mapper Side Join)");
        job.setJarByClass(ReplicateJoin.class);

        job.getConfiguration().set("join.type", otherArgs[3]);
        job.setMapperClass(ReplicateJoin.ReplicateMapper.class);
        job.setNumReduceTasks(0);
        job.addCacheFile(new URI(otherArgs[0]));
        TextInputFormat.setInputPaths(job, new Path[]{new Path(otherArgs[1])});
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
    }

    public Configuration getConf() {
        return null;
    }

    private void printUsage() {
        System.err.println("Usage: ReplicateJoin <user_in> <comments_in> <out> <inner|leftouter>");
        ToolRunner.printGenericCommandUsage(System.err);
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new ReplicateJoin(), args);
        System.exit(returnCode);
    }

    public static class ReplicateMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<Integer, String> userMap = new HashMap();
        private String joinType = null;
        private Text EMPTY_TEXT = new Text("emptyText");

        public void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException {
            URI[] localCacheFiles = context.getCacheFiles();
            String userPath = localCacheFiles[0].toString();
            BufferedReader br = new BufferedReader(new FileReader(userPath));

            String line;
            while ((line = br.readLine()) != null) {
                int userId = Integer.parseInt(line.split("\\s+")[0]);
                this.userMap.put(userId, line);
            }

            this.joinType = context.getConfiguration().get("join.type");
        }

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int commentsId = Integer.parseInt(line.split("\\s+")[0]);
            if (this.userMap.containsKey(commentsId)) {
                context.write(new Text(line), new Text((String) this.userMap.get(commentsId)));
            } else if (this.joinType.equalsIgnoreCase("leftouter")) {
                context.write(new Text(this.userMap.get(commentsId)), this.EMPTY_TEXT);
            }
        }
    }
}
