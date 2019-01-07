package DSPPTest.hadoop;

import DSPPCode.hadoop.WordCount;
import DSPPTest.util.KVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import static DSPPTest.util.FileOperator.*;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTest {

    @Test
    public void test() throws Exception {
        // 设置路径
        String rootPath = System.getProperty("user.dir") + "/src/test/resources";
        String inputFile = rootPath + "/input";
        String outputFolder = "/tmp/dspp_output/hadoop";
        String outputFile = outputFolder + "/part-r-00000";
        String answerFile = rootPath + "/answer";

        // 删除旧输出
        deleteFolder(outputFolder);

        // 执行
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        job.waitForCompletion(false);

        // 检验结果
        verifyKV(readFile2String(outputFile), readFile2String(answerFile), new KVParser("\t"));
        System.out.println("通过~");
    }

}
