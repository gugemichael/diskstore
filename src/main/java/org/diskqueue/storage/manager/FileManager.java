package org.diskqueue.storage.manager;

import org.diskqueue.storage.FileHandle;
import org.diskqueue.storage.meta.StorageMetadata;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager {
    // file auto increament sequence number
    private final AtomicInteger fileSequenceNumber = new AtomicInteger(0);

    // meta data file
    private StorageMetadata metadata;
    // block file array
    private ConcurrentSkipListMap<String, FileHandle> blockFileHandleMap;

    /**
     * could only created by load()
     */
    private FileManager() {
    }

    public static FileManager load(boolean forceCleanup) {
        FileManager fileManager = new FileManager();

        return fileManager;
    }

    public FileHandle getLatestFileHandle() {
        if (blockFileHandleMap.isEmpty())
            return null;
        return blockFileHandleMap.get(blockFileHandleMap.lastKey());
    }

    public void newFileHandle() {
    }

    public void giveback(FileHandle fileHandle) {

    }
}
