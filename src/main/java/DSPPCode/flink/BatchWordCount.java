package DSPPCode.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {

    public static DataSet<Tuple2<String, Integer>> wordCount(DataSet<String> text) {
        return text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                for (String token : value.split("\\W+")) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);
    }

}
