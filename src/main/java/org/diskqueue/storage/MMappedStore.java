package org.diskqueue.storage;

import org.diskqueue.option.Syncer;
import org.diskqueue.storage.block.Slice;
import org.diskqueue.storage.manager.FileManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * MMaped store for handling mmap file I/O
 */
public class MMappedStore implements Store {
    // for write and read
    private final MMappedMode mode = MMappedMode.READABLE_AND_WRITEABLE;

    // file manager
    private FileManager fileManager;

    // file handle
    private FileHandle fileIO;
    // flush caller. change with Syncer
    private Flusher flusher;
    // byte order
    private volatile ByteOrder order;


    @Override
    public void writeSlice(Slice slice) {
        try {
            fileIO.write(slice);
            flusher.flushAll();
        } catch (IOException ignored) {
            ignored.printStackTrace();
        }
    }

    @Override
    public Slice readSlice() {
        return null;
    }


    private enum MMappedMode {
        WRITEABLE, READABLE, READABLE_AND_WRITEABLE
    }

    public MMappedStore byteOrder(ByteOrder byteOrder) {
        this.order = order;
        return this;
    }

    public MMappedStore syncer(Syncer syncer) {
        this.flusher = Flusher.policy(this.fileIO, syncer);
        return this;
    }

    public MMappedStore initialize(String pathName, String name) {
        try {
            File path = new File(pathName);
            if (!path.exists() || !path.canWrite())
                throw new IOException("storage folder path isn't valid");

            File folder = new File(String.format("%s/%s", pathName, name));
            if (!folder.exists())
                folder.createNewFile();

            // build folder management
            this.fileManager = FileManager.build(path, folder);
            this.fileIO = new FileHandle(this.fileManager);

            return this;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
