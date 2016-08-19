package org.diskqueue.storage.meta;

import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.store.DiskFile;
import org.diskqueue.storage.store.FileStatus;

import java.io.*;
import java.util.Date;

public class Manifest extends DiskFile {
    // Data file permanent size
    public final int DataFileSize = DataFile.DATA_BLOCK_FILE_SIZE;
    // Data file count
    public int DataFileCount;
    // last created file full name
    public String LastCreatedDataFile;
    // last read file full name
    public String LastReadDataFile;
    // last deleted file full name
    public String LastDeletedDataFile;
    // last this file synced time
    public String LastSyncTime;

    private final FileManager fileManager;
    private final ManifestFileMapper mapper;

    public Manifest(FileManager fileManager, File file) {
        super(file, FileStatus.WRITE);
        this.fileManager = fileManager;
        this.mapper = new ManifestFileMapper(file);
    }

    public void load() throws IOException {
        mapper.load();
    }

    @Override
    public void sync() {
        mapper.sync();
    }

    /**
     * Helper tools for manifest reading and flushing
     */
    class ManifestFileMapper {
        private final File self;

        ManifestFileMapper(File file) {
            this.self = file;
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

        boolean load() throws IOException {
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(self));
            String line = null;
            // for DataFileSize
            line = reader.readLine();
            // for DataFileCount
            line = reader.readLine();
            if (line == null || !line.contains("="))
                throw new IOException("manifest DataFileCount field error");
            DataFileCount = Integer.parseInt(line.split("=")[1].trim());
            // for LastCreatedDataFile
            line = reader.readLine();
            if (line == null || !line.contains("="))
                throw new IOException("manifest LastCreatedDataFilet field error");
            LastCreatedDataFile = line.split("=")[1].trim();
            // for LastReadDataFile
            line = reader.readLine();
            if (line == null || !line.contains("="))
                throw new IOException("manifest LastReadDataFile field error");
            LastReadDataFile = line.split("=")[1].trim();
            // for LastDeletedDataFile
            line = reader.readLine();
            if (line == null || !line.contains("="))
                throw new IOException("manifest LastDeletedDataFilet field error");
            LastDeletedDataFile = line.split("=")[1].trim();
            // for LastSyncTime
            line = reader.readLine();
            if (line == null || !line.contains("="))
                throw new IOException("manifest LastSyncTime field error");
            LastSyncTime = line.split("=")[1].trim();
            reader.close();
            return true;
        }

        void sync() {
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
                writer.write(String.format("LastReadDataFile=%s", LastReadDataFile));
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
}
