package org.diskqueue.storage;

import org.diskqueue.storage.manager.FileManager;
import org.diskqueue.utils.RefCount;

import java.io.File;

public class FileHandle extends RefCount {

    // belonged FileManager. used to recyle itself
    private FileManager fileManager;
    // java file struct
    private File target;

    public FileHandle(FileManager fileManager, String fileName) {
        this.fileManager = fileManager;
        this.target = new File(fileName);
    }

    @Override
    public void recyle() {
        fileManager.giveback(this);
    }
}
