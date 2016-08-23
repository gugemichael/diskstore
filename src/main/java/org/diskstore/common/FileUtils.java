package org.diskstore.common;

public class FileUtils {

    public static int getFileNumber(String fileName) {
        return Integer.parseInt(fileName.split("\\.")[2]);
    }
}
