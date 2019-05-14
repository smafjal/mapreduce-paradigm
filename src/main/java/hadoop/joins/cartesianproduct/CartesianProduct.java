package hadoop.joins.cartesianproduct;


import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CartesianProduct extends Configured implements Tool {
    private static final Logger LOGGER = Logger.getLogger(CartesianProduct.class.getName());

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CartesianProduct <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(2);
        }

        JobConf conf = new JobConf("Cartesian Product Job");
        conf.setJarByClass(CartesianProduct.class);
        conf.setMapperClass(CartesianProduct.CartesianMapper.class);
        conf.setNumReduceTasks(0);
        conf.setInputFormat(CartesianProduct.CartesianInputFormat.class);
        CartesianProduct.CartesianInputFormat.setLeftInputInfo(conf, TextInputFormat.class, args[0]);
        CartesianProduct.CartesianInputFormat.setRightInputInfo(conf, TextInputFormat.class, args[0]);
        TextOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        RunningJob job = JobClient.runJob(conf);

        while (!job.isComplete()) {
            Thread.sleep(1000L);
        }

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CartesianProduct(), args);
        System.exit(res);
    }

    public static class CartesianMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            CartesianProduct.LOGGER.info("Key: " + key.toString());
            CartesianProduct.LOGGER.info("Value: " + value.toString());
            output.collect(key, value);
        }
    }

    public static class CartesianRecordReader<K1, V1, K2, V2> implements RecordReader<Text, Text> {
        private RecordReader leftRR = null;
        private RecordReader rightRR = null;
        private FileInputFormat rightFIF;
        private JobConf rightConf;
        private InputSplit rightIS;
        private Reporter rightReporter;
        private K1 lkey;
        private V1 lvalue;
        private K2 rkey;
        private V2 rvalue;
        private boolean goToNextLeft = true;
        private boolean alldone = false;

        public CartesianRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter) throws Exception {
            this.rightConf = conf;
            this.rightIS = split.get(1);
            this.rightReporter = reporter;
            FileInputFormat leftFIF = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(conf.get("cart.left.inputformat")), conf);
            this.leftRR = leftFIF.getRecordReader(split.get(0), conf, reporter);
            this.rightFIF = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(conf.get("cart.right.inputformat")), conf);
            this.rightRR = this.rightFIF.getRecordReader(this.rightIS, this.rightConf, this.rightReporter);
            this.lkey = (K1) this.leftRR.createKey();
            this.lvalue = (V1) this.leftRR.createValue();
            this.rkey = (K2) this.rightRR.createKey();
            this.rvalue = (V2) this.rightRR.createValue();
        }

        public boolean next(Text key, Text value) throws IOException {
            do {
                if (this.goToNextLeft) {
                    if (!this.leftRR.next(this.lkey, this.lvalue)) {
                        this.alldone = true;
                        break;
                    }

                    key.set(this.lvalue.toString());
                    this.goToNextLeft = this.alldone = false;
                    this.rightRR = this.rightFIF.getRecordReader(this.rightIS, this.rightConf, this.rightReporter);
                }

                if (this.rightRR.next(this.rkey, this.rvalue)) {
                    value.set(this.rvalue.toString());
                } else {
                    this.goToNextLeft = true;
                }
            } while (this.goToNextLeft);

            return !this.alldone;
        }

        public Text createKey() {
            return new Text();
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return this.leftRR.getPos();
        }

        public void close() throws IOException {
            this.leftRR.close();
            this.rightRR.close();
        }

        public float getProgress() throws IOException {
            return this.leftRR.getProgress();
        }
    }

    public static class CartesianInputFormat extends FileInputFormat {
        private static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
        private static final String LEFT_INPUT_PATH = "cart.left.path";
        private static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
        private static final String RIGHT_INPUT_PATH = "cart.right.path";

        private static void setLeftInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
            job.set("cart.left.inputformat", inputFormat.getCanonicalName());
            job.set("cart.left.path", inputPath);
            CartesianProduct.LOGGER.info("Left InputaPath: " + inputPath);
        }

        private static void setRightInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
            job.set("cart.right.inputformat", inputFormat.getCanonicalName());
            job.set("cart.right.path", inputPath);
            CartesianProduct.LOGGER.info("Right InputPath: " + inputPath);
        }

        public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
            InputSplit[] leftSplits = null;
            InputSplit[] rightSplits = null;

            try {
                leftSplits = this.getInputSplits(conf, conf.get("cart.left.inputformat"), conf.get("cart.left.path"), numSplits);
                rightSplits = this.getInputSplits(conf, conf.get("cart.right.inputformat"), conf.get("cart.right.path"), numSplits);
            } catch (ClassNotFoundException var15) {
                var15.printStackTrace();
            }

            CartesianProduct.LOGGER.info("Split Size:---> L = " + leftSplits.length + " R = " + rightSplits.length);
            CompositeInputSplit[] returnSplits = new CompositeInputSplit[leftSplits.length * rightSplits.length];
            int i = 0;
            InputSplit[] var7 = leftSplits;
            int var8 = leftSplits.length;

            for (int var9 = 0; var9 < var8; ++var9) {
                InputSplit left = var7[var9];
                InputSplit[] var11 = rightSplits;
                int var12 = rightSplits.length;

                for (int var13 = 0; var13 < var12; ++var13) {
                    InputSplit right = var11[var13];
                    returnSplits[i] = new CompositeInputSplit(2);
                    returnSplits[i].add(left);
                    returnSplits[i].add(right);
                    ++i;
                }
            }

            return returnSplits;
        }

        public RecordReader getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
            try {
                return new CartesianProduct.CartesianRecordReader((CompositeInputSplit) split, conf, reporter);
            } catch (Exception var5) {
                var5.printStackTrace();
                return null;
            }
        }

        private InputSplit[] getInputSplits(JobConf conf, String inputFormatClass, String inputPath, int numSplits) throws ClassNotFoundException, IOException {
            FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(inputFormatClass), conf);
            FileInputFormat.setInputPaths(conf, inputPath);
            return inputFormat.getSplits(conf, numSplits);
        }
    }
}
