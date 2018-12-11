package DSPPTest.hadoop;

import DSPPCode.hadoop.WordCount;
import DSPPTest.TestTemplate;
import DSPPTest.util.FileOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class WordCountTest extends TestTemplate {

    private static String outputPath = "/home/chenzh/output/Hadoop";

    @BeforeClass
    public static void deleteOutput() {
        FileOperator.deleteLocalDirectory(outputPath);
    }

    @Test
    public void test() throws Exception {
        String inputPath = "/home/chenzh_e/input";
        String answerPath = "/home/chenzh_e/checkout/Hadoop/word_count.out";

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        assertTrue(FileOperator.existLocal(outputPath + "/_SUCCESS"));
        String output = FileOperator.readLocal2String(outputPath + "/part-r-00000");
        String answer = FileOperator.readLocal2String(answerPath);
        assertEquals(answer, output);
        points += 2.6D;
    }

}
