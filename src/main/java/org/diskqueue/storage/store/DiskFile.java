package org.diskqueue.storage.store;

import java.io.File;

/**
 * Class of abstraction all diskqueue file on disk
 */
public abstract class DiskFile {
    protected final File thisFile;

    public DiskFile(File file) {
        this.thisFile = file;
    }

    public File getGenericFile() {
        return thisFile;
    }

    public abstract void sync();

    public abstract boolean checkout();
}
