package DSPPTest.flink_stream;

import DSPPCode.flink_stream.WordCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.FileOperator.readFolder2String;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTest {

    @Test
    public void streamTest() throws Exception {
        // 设置路径
        String rootPath = System.getProperty("user.dir") + "/src/test/resources";
        String inputFile = rootPath + "/input";
        String outputFolder = "/tmp/dspp_output/flink_stream";
        String answerFile = rootPath + "/answer";

        // 删除旧输出
        deleteFolder(outputFolder);

        // 执行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.addSource(new Source(inputFile, 1));
        WordCount.wordCount(text).writeAsCsv(outputFolder);
        env.execute();

        // 检验结果
        verifyKV(readFolder2String(outputFolder), readFile2String(answerFile));
        System.out.println("通过~");
    }

}
