package DSPPTest.util;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Verifier {

    public static void verifyKV(String output, String answer) throws Exception {
        KVParser parser = new KVParser();
        verifyKV(output, answer, parser, parser);
    }

    public static void verifyKV(String output, String answer, KVParser parser) throws Exception {
        verifyKV(output, answer, parser, parser);
    }

    public static void verifyKV(String output, String answer, KVParser outputParser, KVParser answerParser) throws Exception {
        Map<String, String> outputMap = outputParser.parse(output);
        Map<String, String> answerMap = answerParser.parse(answer);
        for (String key : answerMap.keySet()) {
            assertEquals(answerMap.get(key), outputMap.get(key));
        }
        assertEquals(answerMap.size(), outputMap.size());
    }

}
