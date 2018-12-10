package DSPPTest.hadoop;

import DSPPCode.hadoop.WordCount;
import DSPPTest.util.FileOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class WordCountTest {

    static private double points = 0.0D;

    static private Configuration conf = new Configuration();

    static private FileSystem fs;

    @BeforeClass
    public static void init() throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        fs = FileSystem.get(conf);
        fs.delete(new Path("output"), true);
    }

    @AfterClass
    public static void summarize() {
        System.out.println("");
        System.out.println("Group achieved " + points + " points.");
        System.out.println("");
    }

    @Test
    public void test() throws Exception {
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.waitForCompletion(false);

        assertTrue(fs.exists(new Path("output/_SUCCESS")) &&
                fs.exists(new Path("output/part-r-00000")));
        String output = FileOperator.readHDFS2String(fs, "output/part-r-00000");
        String answer = FileOperator.readFile2String("/home/chenzh_e/checkout/word_count.out");
        assertEquals(output, answer);
        points += 2.6D;
    }

}
