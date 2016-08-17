package org.diskqueue.storage.manager;

import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.meta.Manifest;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager {
    // manifest file name
    public static final String MANIFEST_FILE_NAME = "manifest";
    // file auto increament sequence number
    private final AtomicInteger fileSequenceNumber = new AtomicInteger(0);


    // meta data file
    private Manifest manifest;
    // block file array
    private ConcurrentSkipListMap<String, DataFile> blockFileHandleMap;

    public static FileManager build(File path, File folder) throws IOException {
        return build(path, folder, true);
    }

    public static FileManager build(File path, File folder, boolean recovery) throws IOException {
        FileManager fileManager = new FileManager();
        File meta = new File(String.format("%s/%s.%s", folder.getAbsolutePath(), folder, MANIFEST_FILE_NAME));
        // install manifest
        fileManager.manifest = new Manifest(fileManager, meta);
        fileManager.manifest.load();
        // install Datafiles

        return fileManager;
    }

    public DataFile getLatestDataFile() {
        if (blockFileHandleMap.isEmpty())
            return null;
        return blockFileHandleMap.get(blockFileHandleMap.lastKey());
    }

    private void newDataFile() {
    }

    public void putback(DataFile fileHandle) {

    }


    class FileName {
        String Name;
        int Sequence;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FileName fileName = (FileName) o;

            if (Sequence != fileName.Sequence) return false;
            return Name != null ? Name.equals(fileName.Name) : fileName.Name == null;

        }

        @Override
        public int hashCode() {
            int result = Name != null ? Name.hashCode() : 0;
            result = 31 * result + Sequence;
            return result;
        }
    }

    /**
     * could only created by load()
     */
    private FileManager() {
    }
}
