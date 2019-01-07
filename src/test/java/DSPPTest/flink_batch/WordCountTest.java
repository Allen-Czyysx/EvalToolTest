package DSPPTest.flink_batch;

import DSPPCode.flink_batch.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.FileOperator.readFolder2String;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTest {

    @Test
    public void batchTest() throws Exception {
        // 设置路径
        String rootPath = System.getProperty("user.dir") + "/src/test/resources";
        String inputFile = rootPath + "/input";
        String outputFolder = "/tmp/dspp_output/flink_batch";
        String answerFile = rootPath + "/answer";

        // 删除旧输出
        deleteFolder(outputFolder);

        // 执行
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(inputFile);
        DataSet<Tuple2<String, Integer>> wordCount = WordCount.wordCount(text);
        wordCount.writeAsCsv(outputFolder);
        wordCount.collect();

        // 检验结果
        verifyKV(readFolder2String(outputFolder), readFile2String(answerFile));
        System.out.println("通过~");
    }

}
