package org.diskstore.engine.storage.store;

import org.diskstore.engine.Configuration;
import org.diskstore.engine.Store;
import org.diskstore.engine.option.Option;
import org.diskstore.engine.option.Syncer;
import org.diskstore.engine.storage.FileManager;
import org.diskstore.engine.storage.block.Slice;

import java.io.File;
import java.io.IOException;

/**
 * MMaped store for handling New file I/O
 */
public class MMappedStore implements Store {
    // all files manager
    private FileManager fileManager;

    private boolean recovery;

    // options
    private final Configuration configuration;

    // file handle
    private FileHandle fileIO;
    // flush caller. change with Syncer
    private Flusher flusher;
    private Syncer syncer;

    public MMappedStore(Configuration configuration) {
        this.recovery = configuration.get(Option.RECOVERY);
        this.syncer = configuration.get(Option.SYNC);
        this.configuration = configuration;
        initialize(configuration.get(Option.DATA_PATH), configuration.get(Option.NAME));
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
    public Slice readNext() {
        return fileIO.fetch();
    }

    public MMappedStore initialize(String pathName, String name) {
        try {
            File fullpath = new File(String.format("%s/%s", pathName, name));
            if (!fullpath.exists())
                fullpath.mkdirs();
            if (!fullpath.isDirectory() || !fullpath.canWrite())
                throw new IOException("storage folder path isn't valid");

            // build file management from this full path foler
            this.fileManager = FileManager.build(configuration, fullpath, recovery);
            this.fileIO = new FileHandle(fileManager);
            this.flusher = Flusher.policy(fileIO, syncer);

            return this;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
