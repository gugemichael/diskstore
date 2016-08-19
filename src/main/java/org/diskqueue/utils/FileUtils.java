package org.diskqueue.utils;

public class FileUtils {

    public static int getFileNumber(String fileName) {
        return Integer.parseInt(fileName.split("\\.")[2]);
    }
}
