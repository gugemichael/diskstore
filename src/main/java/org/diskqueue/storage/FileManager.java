package org.diskqueue.storage;

import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.meta.Manifest;
import org.diskqueue.utils.FileUtils;
import org.diskqueue.utils.RefCounter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.diskqueue.storage.block.DataFile.New;
import static org.diskqueue.storage.block.DataFile.load;

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
    private ConcurrentSkipListMap<DataFile, RefCounter<DataFile>> writeDataFileSet = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<DataFile, RefCounter<DataFile>> readDataFileSet = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<DataFile, RefCounter<DataFile>> releaseMap = new ConcurrentSkipListMap<>();

    // disk file deleter thread who is responsibility for clean up
    // consumed (refcount equals zero) files
    private final FileCleanupDeleter deleter = new FileCleanupDeleter();

    public static FileManager build(File folder) throws IOException {
        return build(folder, true);
    }

    public static FileManager build(File folder, boolean recovery) throws IOException {
        // new instance from current path
        final FileManager fileManager = new FileManager();

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
        fileManager.manifest.load();

        // only load data block files
        File[] dataBlockList = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(String.format("%s.block.", namespace)) &&
                        !name.endsWith("deleted");
            }
        });

        if (dataBlockList != null && dataBlockList.length != 0) {
            // do recovery and load persistence file into file cache
            // and sort data file by number
            Arrays.sort(dataBlockList, new Comparator<File>() {
                @Override
                public int compare(File first, File second) {
                    return FileUtils.getFileNumber(first.getName()) - FileUtils.getFileNumber(second.getName());
                }
            });
            int i = 0;
            for (; i < dataBlockList.length - 1; i++) {
                DataFile read = load(fileManager, dataBlockList[i]);
                fileManager.readDataFileSet.put(read, new RefCounter<DataFile>(read));
            }
            // make the latest one writeable
            DataFile write = DataFile.New(fileManager, dataBlockList[i]);
            DataFile read = load(fileManager, dataBlockList[i]);
            fileManager.writeDataFileSet.put(write, new RefCounter<DataFile>(write));
            fileManager.readDataFileSet.put(read, new RefCounter<DataFile>(read));

            // we are manpulating the newest one. since we hold its reference
            fileManager.writeDataFileSet.lastEntry().getValue().incrRef();
            // fix next number and global block number
            fileManager.nextSequenceFileNumber.set(fileManager.writeDataFileSet.lastEntry().getKey().getFileNumber() + 1);
        } else {
            // we create at lease one DataFile
            fileManager.createDataFile();
        }

        // may be sync twice ! but it's OK
        fileManager.syncMeta();

        for (RefCounter<DataFile> ref : fileManager.writeDataFileSet.values())
            System.out.println("write set : " + ref.getInstance().getGenericFile().getName() + ", " + ref.getRefCount());
        for (RefCounter<DataFile> ref : fileManager.readDataFileSet.values())
            System.out.println("read set : " + ref.getInstance().getGenericFile().getName() + ", " + ref.getRefCount());

        return fileManager;
    }

    public synchronized DataFile createDataFile() throws IOException {
        File physical = new File(String.format("%s/%s.block.%d", folder.getAbsolutePath(), namespace, nextSequenceFileNumber.intValue()));
        DataFile write = New(this, physical);
        DataFile read = load(this, physical);
        // add to deque and wait to be read
        writeDataFileSet.put(write, new RefCounter<DataFile>(write).incrRef());
        readDataFileSet.put(read, new RefCounter<DataFile>(read));
        // auto increment
        nextSequenceFileNumber.incrementAndGet();
        // fill the last created file
        manifest.LastCreatedDataFile = write.getGenericFile().getName();

        write.sync();
        syncMeta();
        return write;
    }

    public synchronized DataFile getEarliestDataFile() {
        if (readDataFileSet.isEmpty())
            return null;

        // pop the earliest data file
        RefCounter<DataFile> reference = readDataFileSet.firstEntry().getValue();
        reference.incrRef();
        readDataFileSet.remove(readDataFileSet.firstEntry().getKey());
        releaseMap.put(reference.getInstance(), reference);
        DataFile datafile = reference.getInstance();
        manifest.LastReadDataFile = datafile.getGenericFile().getName();

        syncMeta();
        return datafile;
    }

    public DataFile getNewestDataFile() {
        return writeDataFileSet.lastEntry().getValue().getInstance();
    }


    public FileCleanupDeleter getDeleter() {
        return deleter;
    }

    public void release(DataFile datafile) {
        releaseMap.get(datafile).decrRef();
    }

    public void syncMeta() {
        manifest.DataFileCount = readDataFileSet.size();
        manifest.LastSyncTime = new Date().toString();
        manifest.sync();
    }
}
