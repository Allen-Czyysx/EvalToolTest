package DSPPTest.flink;

import DSPPCode.flink.StreamWordCount;
import DSPPTest.TestTemplate;
import DSPPCode.flink.BatchWordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.FileOperator.readFolder2String;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTest extends TestTemplate {

    private static String inputFile = "/home/chenzh_e/input/word_count";

    private static String outputFolder = "/home/chenzh/output/Flink";

    private static String answerFile = "/home/chenzh_e/checkout/word_count";

    private static void deleteOutput() {
        deleteFolder(outputFolder);
    }

    @Test
    public void batchTest() throws Exception {
        deleteOutput();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(inputFile);

        DataSet<Tuple2<String, Integer>> wordCount = BatchWordCount.wordCount(text);
        wordCount.writeAsCsv(outputFolder);
        wordCount.collect();

        verifyKV(readFolder2String(outputFolder), readFile2String(answerFile));
        points += 2.6D;
    }

    @Test
    public void streamTest() throws Exception {
        deleteOutput();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.addSource(new Source(inputFile, 1));

        DataStream<Tuple2<String, Integer>> windowCounts = StreamWordCount.wordCount(text);

        windowCounts.writeAsCsv(outputFolder);
        env.execute();

        verifyKV(readFolder2String(outputFolder), readFile2String(answerFile));
        points += 2.6D;
    }

}
