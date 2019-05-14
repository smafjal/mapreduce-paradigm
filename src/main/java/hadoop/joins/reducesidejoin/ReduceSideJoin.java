package hadoop.joins.reducesidejoin;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configuration implements Tool {
    public void setConf(Configuration configuration) {
    }

    public Configuration getConf() {
        return null;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] othrArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (othrArgs.length != 4) {
            this.printUsage();
        }

        Job job = Job.getInstance(conf, "Reduce Side Join");
        job.setJarByClass(ReduceSideJoin.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReduceSideJoin.UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReduceSideJoin.CommentsMapper.class);
        job.getConfiguration().set("join.type", args[2]);
        job.setReducerClass(ReduceSideJoin.ReduceSideReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 2;
    }

    private void printUsage() {
        System.err.println("Usage: ReduceSideJoin <user_in> <comments_in> <join_type> <out>");
        ToolRunner.printGenericCommandUsage(System.err);
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new ReduceSideJoin(), args);
        System.exit(returnCode);
    }

    public static class ReduceSideReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Text EMPTY_TEXT = new Text("EmptyText");
        private Text tmp = new Text();
        private ArrayList<Text> userList = new ArrayList();
        private ArrayList<Text> commentsList = new ArrayList();
        private String joinType;

        public void setup(Reducer<IntWritable, Text, Text, Text>.Context context) {
            this.joinType = context.getConfiguration().get("join.type");
            System.out.println("Join Type: " + this.joinType);
        }

        public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            this.userList.clear();
            this.commentsList.clear();

            while (values.iterator().hasNext()) {
                this.tmp = (Text) values.iterator().next();
                if (this.tmp.charAt(0) == 65) {
                    this.userList.add(new Text(this.tmp.toString().substring(1)));
                } else if (this.tmp.charAt(0) == 66) {
                    this.commentsList.add(new Text(this.tmp.toString().substring(1)));
                }
            }
            this.excuteJoinLogic(context);
        }

        private void excuteJoinLogic(Context context) throws IOException, InterruptedException {
            Iterator itr1, itr2;
            Text a, b;
            if (this.joinType.equalsIgnoreCase("inner")) {
                if (!this.userList.isEmpty() && !this.commentsList.isEmpty()) {
                    itr1 = this.userList.iterator();

                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        itr2 = this.commentsList.iterator();

                        while (itr2.hasNext()) {
                            b = (Text) itr2.next();
                            context.write(a, b);
                        }
                    }
                }
            } else if (this.joinType.equalsIgnoreCase("leftouter")) {
                itr1 = this.userList.iterator();

                while (true) {
                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        if (this.commentsList.isEmpty()) {
                            context.write(a, this.EMPTY_TEXT);
                        } else {
                            itr2 = this.commentsList.iterator();

                            while (itr2.hasNext()) {
                                b = (Text) itr2.next();
                                context.write(a, b);
                            }
                        }
                    }

                    return;
                }
            } else if (this.joinType.equalsIgnoreCase("rightouter")) {
                if (this.userList.isEmpty()) {
                    itr1 = this.commentsList.iterator();

                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        context.write(this.EMPTY_TEXT, a);
                    }
                } else {
                    itr1 = this.userList.iterator();

                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        itr2 = this.commentsList.iterator();

                        while (itr2.hasNext()) {
                            b = (Text) itr2.next();
                            context.write(a, b);
                        }
                    }
                }
            } else if (this.joinType.equalsIgnoreCase("fullouter")) {
                if (this.userList.isEmpty()) {
                    itr1 = this.commentsList.iterator();

                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        context.write(this.EMPTY_TEXT, a);
                    }
                } else if (this.commentsList.isEmpty()) {
                    itr1 = this.userList.iterator();

                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        context.write(a, this.EMPTY_TEXT);
                    }
                } else {
                    itr1 = this.userList.iterator();

                    while (itr1.hasNext()) {
                        a = (Text) itr1.next();
                        itr2 = this.commentsList.iterator();

                        while (itr2.hasNext()) {
                            b = (Text) itr2.next();
                            context.write(a, b);
                        }
                    }
                }
            } else if (this.joinType.equalsIgnoreCase("anti") && this.userList.isEmpty() ^ this.commentsList.isEmpty()) {
                itr1 = this.userList.iterator();
                while (itr1.hasNext()) {
                    a = (Text) itr1.next();
                    context.write(a, this.EMPTY_TEXT);
                }
                itr1 = this.commentsList.iterator();
                while (itr1.hasNext()) {
                    a = (Text) itr1.next();
                    context.write(this.EMPTY_TEXT, a);
                }
            }

        }
    }

    public static class CommentsMapper extends Mapper<Object, Text, IntWritable, Text> {
        IntWritable outkey = new IntWritable();
        Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int userId = Integer.parseInt(line.split("\\s+")[0]);
            this.outkey.set(userId);
            this.outValue.set('B' + line);
            context.write(this.outkey, this.outValue);
        }
    }

    public static class UserMapper extends Mapper<Object, Text, IntWritable, Text> {
        IntWritable outkey = new IntWritable();
        Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int userId = Integer.parseInt(line.split("\\s+")[0]);
            this.outkey.set(userId);
            this.outValue.set('A' + line);
            context.write(this.outkey, this.outValue);
        }
    }
}
