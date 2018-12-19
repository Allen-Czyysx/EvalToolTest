package DSPPTest.hadoop;

import DSPPCode.hadoop.WordCount;
import DSPPTest.TestTemplate;
import DSPPTest.util.KVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.BeforeClass;
import org.junit.Test;

import static DSPPTest.util.FileOperator.*;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTest extends TestTemplate {

    private static String outputFolder = "/home/chenzh/output/Hadoop";

    @BeforeClass
    public static void deleteOutput() {
        deleteFolder(outputFolder);
    }

    @Test
    public void test() throws Exception {
        String inputFile = "/home/chenzh_e/input/word_count";
        String outputFile = outputFolder + "/part-r-00000";
        String answerFile = "/home/chenzh_e/checkout/word_count";

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));

        job.waitForCompletion(false);

        verifyKV(readFile2String(outputFile), readFile2String(answerFile), new KVParser("\t"));
        points += 2.6D;
    }

}
