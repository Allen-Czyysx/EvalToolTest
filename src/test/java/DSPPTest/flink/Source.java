package DSPPTest.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static DSPPTest.util.FileOperator.readFile2String;
import static java.lang.Thread.sleep;

public class Source implements SourceFunction<String> {

    private String inputPath;

    private int runningSeconds;

    public Source(String inputPath, int runningSeconds) {
        this.inputPath = inputPath;
        this.runningSeconds = runningSeconds;
    }

    public void run(SourceContext<String> sourceContext) throws Exception {
        for (int i = 0; i < runningSeconds; i++) {
            sourceContext.collect(readFile2String(inputPath));
            sleep(1000);
        }
    }

    public void cancel() {
    }

}
