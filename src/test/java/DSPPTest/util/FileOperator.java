package DSPPTest.util;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

public class FileOperator {

    static private short DEFAULT_BUFFER_SIZE = 1024;

    // encode in UTF-8
    static public String readFile2String(String filePath)
            throws java.io.IOException {
        File file = new File(filePath);
        if (file.length() == 0) {
            return "";
        }

        InputStreamReader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
        StringWriter writer = new StringWriter();
        char[] buffer = new char[DEFAULT_BUFFER_SIZE];
        int n;

        while (-1 != (n = reader.read(buffer))) {
            writer.write(buffer, 0, n);
        }
        reader.close();

        return writer.toString();
    }

    // encode in UTF-8
    static public String readHDFS2String(FileSystem fileSystem, String filePath)
            throws java.io.IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        String out= IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        fileSystem.close();

        return out;
    }

}
