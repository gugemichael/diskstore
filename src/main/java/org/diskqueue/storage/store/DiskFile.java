package org.diskqueue.storage.store;

import java.io.File;

/**
 * Class of abstraction all diskqueue file on disk
 */
public abstract class DiskFile {
    protected final File thisFile;
    protected final FileStatus fileStatus;

    public DiskFile(File file, FileStatus fileStatus) {
        this.thisFile = file;
        this.fileStatus = fileStatus;
    }

    public File getGenericFile() {
        return thisFile;
    }

    public abstract void sync();
}
