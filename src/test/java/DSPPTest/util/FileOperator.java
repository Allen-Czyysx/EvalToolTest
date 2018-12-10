package DSPPTest.util;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

public class FileOperator {

    private static short DEFAULT_BUFFER_SIZE = 1024;

    // encode in UTF-8
    public static String readLocal2String(String filePath)
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
    public static String readHDFS2String(FileSystem fileSystem, String filePath)
            throws java.io.IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        String out = IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        fileSystem.close();

        return out;
    }

    public static boolean existLocal(String filePath) {
        return new File(filePath).exists();
    }

    public static boolean deleteLocalDirectory(String filePath) {
        return FileUtils.deleteQuietly(new File(filePath).getParentFile());
    }

}
