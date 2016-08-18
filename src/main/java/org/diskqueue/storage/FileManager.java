package org.diskqueue.storage;

import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.meta.Manifest;
import org.diskqueue.utils.RefCounter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager {
    // manifest and data file name pattern
    public static final String MANIFEST_FILE_NAME = "%s.manifest";
    public static final String DATA_FILE_NAME = "%s.block.%d";
    // file auto increament sequence number
    private final AtomicInteger nextSequenceFileNumber = new AtomicInteger(0);

    // current directory
    private File folder;
    private String namespace;

    // manifest metadata file
    private Manifest manifest;
    // data block files
    private Deque<RefCounter<DataFile>> dataBlockFileList = new ConcurrentLinkedDeque<>();

    // disk file deleter thread who is responsibility for clean up
    // consumed (refcount equals zero) files
    private final FileCleanupDeleter deleter = new FileCleanupDeleter();
    private final AsyncIOFlusher flusher = new AsyncIOFlusher();

    public static FileManager build(File folder) throws IOException {
        return build(folder, true);
    }

    public static FileManager build(File folder, boolean recovery) throws IOException {
        // new instance from current path
        FileManager fileManager = new FileManager();

        fileManager.deleter.start();

        /**
         * namespace of these files. include follow names suppose our folder called "diskqueue" :
         *
         *      diskqueue.lock
         *      diskqueue.manifest
         *      diskqueue.block.$n, diskqueue.block.$n+1, diskqueue.block.$n+2 ....
         *
         *      n is the logical numberic that start from 0 and is sequential increase
         */
        final String namespace = folder.getName();
        fileManager.folder = folder;
        fileManager.namespace = namespace;

        // remove all files include data file and manifest if ${recovery} is configured to false
        if (!recovery) {
            fileManager.deleter.asyncDelete(folder.listFiles(), true);
        }

        // analyze and recovery manifest
        File meta = new File(String.format("%s/%s", folder.getAbsolutePath(), String.format(MANIFEST_FILE_NAME, namespace)));
        fileManager.manifest = new Manifest(fileManager, meta);
        fileManager.manifest.checkout();

        // load data block files
        File[] dataBlockList = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(String.format("%s.block", namespace));
            }
        });

        if (dataBlockList != null && dataBlockList.length != 0) {
            // do recovery
            DataFile[] sortedDataFiles = new DataFile[dataBlockList.length];
            for (int i = 0; i != dataBlockList.length; i++)
                sortedDataFiles[i] = fileManager.__internalNew(dataBlockList[i]);

            // sort by file number in the suffix of file name
            Arrays.sort(sortedDataFiles, 0, sortedDataFiles.length, new Comparator<DataFile>() {
                @Override
                public int compare(DataFile first, DataFile second) {
                    // I don't care numberic overflow
                    return first.getFileNumber() - second.getFileNumber();
                }
            });

            // add them to deque
            for (DataFile file : sortedDataFiles) {
                System.out.println(file.getGenericFile().getName());
                fileManager.dataBlockFileList.offer(new RefCounter<DataFile>(file));
            }

            fileManager.nextSequenceFileNumber.set(fileManager.dataBlockFileList.getLast().getInstance().getFileNumber() + 1);
        } else {
            // we create at lease one DataFile
            fileManager.newDataFile();
        }

        // fileManager.sync();

        return fileManager;
    }

    public synchronized DataFile newDataFile() throws IOException {
        DataFile datafile = __internalNew(
                new File(String.format("%s/%s.block.%d", folder.getAbsolutePath(), namespace, nextSequenceFileNumber.intValue())));
        dataBlockFileList.offer(new RefCounter<DataFile>(datafile));
        // auto increment
        nextSequenceFileNumber.incrementAndGet();

        return datafile;
    }

    private DataFile __internalNew(File data) throws IOException {
        DataFile datafile = new DataFile(this, data);
        if (!datafile.checkout())
            throw new IOException(String.format("new data block file failed : %s", datafile.getGenericFile().getName()));

        // change meta data.
        manifest.LastCreatedDataFile = datafile.getGenericFile().getName();

        sync();
        return datafile;
    }

    public DataFile getNewestDataFile() {
        return dataBlockFileList.getLast().getInstance();
    }

    public DataFile getLatestDataFile() {
        return dataBlockFileList.getFirst().getInstance();
    }

    public FileCleanupDeleter getDeleter() {
        return deleter;
    }

    public AsyncIOFlusher getFlusher() {
        return flusher;
    }

    public void release(DataFile fileHandle) {

    }

    public void sync() {
        manifest.DataFileCount = dataBlockFileList.size();
        manifest.LastSyncTime = new Date().toString();
        manifest.sync();
    }
}
