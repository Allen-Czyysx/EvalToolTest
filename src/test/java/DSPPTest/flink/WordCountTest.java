package DSPPTest.flink;

import DSPPTest.TestTemplate;
import DSPPTest.util.FileOperator;
import DSPPCode.flink.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordCountTest extends TestTemplate {

    @Test
    public void test() throws Exception {
        String inputPath = "/home/chenzh_e/input";
        String answerPath = "/home/chenzh_e/checkout/Flink/word_count.out";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(inputPath);
        AggregateOperator counts = text.flatMap(new WordCount()).groupBy(0).sum(1);
        List list = counts.collect();

        StringBuilder output = new StringBuilder();
        for (Object tuple : list) {
            output.append(tuple.toString());
        }

        String answer = FileOperator.readLocal2String(answerPath);
        assertEquals(answer, output.toString());
        points += 2.6D;
    }

}
