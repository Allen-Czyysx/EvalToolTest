package DSPPTest.util;

import java.util.HashMap;
import java.util.Map;

public class KVParser {

    private String delimiter = ",";

    private String recordDelimiter = "\n|\r\n";

    public KVParser() {
    }

    public KVParser(String delimiter) {
        this.delimiter = delimiter;
    }

    public KVParser(String delimiter, String recordDelimiter) {
        this.delimiter = delimiter;
        this.recordDelimiter = recordDelimiter;
    }

    public Map<String, String> parse(String str) throws Exception {
        Map<String, String> ret = new HashMap<String, String>();
        String[] records = str.split(recordDelimiter);
        for (String record : records) {
            String[] kv = record.split(delimiter);
            if (ret.containsKey(kv[0])) {
                throw new Exception("Duplicated key: " + kv[0]);
            }
            ret.put(kv[0], kv[1]);
        }
        return ret;
    }

}
