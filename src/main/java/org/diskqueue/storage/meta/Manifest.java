package org.diskqueue.storage.meta;

import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.manager.FileManager;

import java.io.*;
import java.util.Date;
import java.util.IllegalFormatException;

public class Manifest {
    // Data file permanent size
    public int DataFileSize = DataFile.DATA_FILE_LENGTH;
    // Data file count
    public int DataFileCount;
    // last created file full name
    public String LastCreatedDataFile;
    // last deleted file full name
    public String LastDeletedDataFile;
    // last this file synced time
    public String LastSyncTime;

    private final FileManager fileManager;
    private final File self;

    public Manifest(FileManager fileManager, File fileName) {
        this.fileManager = fileManager;
        this.self = fileName;
        if (!this.self.exists()) {
            try {
                this.self.createNewFile();
                // write an empty manifest file
                this.sync();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean load() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(self));
            String line = null;
            // for DataFileSize
            line = reader.readLine();
            if (!line.contains("="))
                throw new IllegalStateException("manifest DataFileSize field error");
            DataFileSize = Integer.parseInt(line.split("=")[1].trim());
            // for DataFileCount
            line = reader.readLine();
            if (!line.contains("="))
                throw new IllegalStateException("manifest DataFileCount field error");
            DataFileCount = Integer.parseInt(line.split("=")[1].trim());
            // for LastCreatedDataFile
            line = reader.readLine();
            if (!line.contains("="))
                throw new IllegalStateException("manifest LastCreatedDataFilet field error");
            LastCreatedDataFile = line.split("=")[1].trim();
            // for LastDeletedDataFile
            line = reader.readLine();
            if (!line.contains("="))
                throw new IllegalStateException("manifest LastDeletedDataFilet field error");
            LastDeletedDataFile = line.split("=")[1].trim();
            // for LastSyncTime
            line = reader.readLine();
            if (!line.contains("="))
                throw new IllegalStateException("manifest LastSyncTime field error");
            LastSyncTime = line.split("=")[1].trim();
        } catch (IllegalFormatException | IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public void sync() {
        BufferedWriter writer = null;
        try {
            LastSyncTime = new Date().toString();
            writer = new BufferedWriter(new FileWriter(self));
            writer.write(String.format("DataFileSize=%d", DataFileSize));
            writer.newLine();
            writer.write(String.format("DataFileCount=%d", DataFileCount));
            writer.newLine();
            writer.write(String.format("LastCreatedDataFile=%s", LastCreatedDataFile));
            writer.newLine();
            writer.write(String.format("LastDeletedDataFile=%s", LastDeletedDataFile));
            writer.newLine();
            writer.write(String.format("LastSyncTime=%s", LastSyncTime));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null)
                    writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
