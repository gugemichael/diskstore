package org.diskqueue.storage;

import org.diskqueue.storage.block.Block;
import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.block.Slice;
import org.diskqueue.storage.manager.FileManager;

import java.io.Flushable;
import java.io.IOException;


public class FileHandle implements Flushable {
    // parent store
    private final FileManager fileManager;

    /**
     * data file and write-read block. producer is writting
     * while consume is reading.
     */
    private DataFile producer;
    private DataFile consume;
    private volatile Block writeBlock;
    private volatile Block readBlock;

    public FileHandle(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public void write(Slice slice) {
        if (writeBlock == null) {
            // no block space to be written. we create a new one

        }
    }

    @Override
    public void flush() throws IOException {
        producer.flushAll();
    }
}
