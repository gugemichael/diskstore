package org.diskqueue.storage.store;

import org.diskqueue.controller.Configure;
import org.diskqueue.option.Option;
import org.diskqueue.option.Syncer;
import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.Storage;
import org.diskqueue.storage.block.Slice;

import java.io.File;
import java.io.IOException;

/**
 * MMaped store for handling New file I/O
 */
public class MappedAppendOnlyStore implements AppendOnlyStore {
    // parent storage
    private final Storage storage;
    private boolean recovery;
    // all files manager
    private FileManager fileManager;

    private final Configure configure;

    // file handle
    private FileHandle fileIO;
    // flush caller. change with Syncer
    private Flusher flusher;
    private Syncer syncer;

    public MappedAppendOnlyStore(Storage storage, Configure configure) {
        this.storage = storage;
        this.recovery = configure.get(Option.RECOVERY);
        this.syncer = configure.get(Option.SYNC);
        this.configure = configure;
        initialize(configure.get(Option.DATA_PATH), configure.get(Option.NAME));
    }

    @Override
    public void writeAppend(Slice slice) {
        try {
            fileIO.append(slice);
            flusher.flushAll();
        } catch (IOException ignored) {
            ignored.printStackTrace();
        }
    }

    @Override
    public Slice fetch() {
        return fileIO.fetch();
    }

    public MappedAppendOnlyStore initialize(String pathName, String name) {
        try {
            File fullpath = new File(String.format("%s/%s", pathName, name));
            if (!fullpath.exists())
                fullpath.mkdirs();
            if (!fullpath.isDirectory() || !fullpath.canWrite())
                throw new IOException("storage folder path isn't valid");

            // build file management from this full path foler
            this.fileManager = FileManager.build(fullpath, recovery);
            this.fileIO = new FileHandle(fileManager, configure);
            this.flusher = Flusher.policy(fileIO, syncer);

            return this;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
