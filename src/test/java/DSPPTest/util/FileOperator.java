package DSPPTest.util;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileOperator {

    // encode in UTF-8
    public static String readFile2String(String filePath) {
        try {
            short DEFAULT_BUFFER_SIZE = 1024;
            File file = new File(filePath);
            InputStreamReader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
            StringWriter writer = new StringWriter();
            char[] buffer = new char[DEFAULT_BUFFER_SIZE];
            int n;

            while (-1 != (n = reader.read(buffer))) {
                writer.write(buffer, 0, n);
            }
            reader.close();

            return writer.toString();
        } catch (Exception e) {
            return "";
        }
    }

    // encode in UTF-8
    public static String readHDFS2String(FileSystem fileSystem, String filePath) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        String out = IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        fileSystem.close();

        return out;
    }

    public static String readFolder2String(String folderPath) {
        try {
            StringBuilder ret = new StringBuilder();
            File folder = new File(folderPath);
            File[] files = folder.listFiles();
            for (File file : files) {
                ret.append(readFile2String(file.getAbsolutePath()));
            }
            return ret.toString();
        } catch (Exception e) {
            return "";
        }
    }

    public static boolean existFile(String filePath) {
        return new File(filePath).exists();
    }

    public static boolean deleteFolder(String filePath) {
        return FileUtils.deleteQuietly(new File(filePath));
    }

}
