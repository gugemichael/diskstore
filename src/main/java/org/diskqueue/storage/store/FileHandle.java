package org.diskqueue.storage.store;

import org.diskqueue.controller.Configure;
import org.diskqueue.option.Option;
import org.diskqueue.option.Syncer;
import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.block.Block;
import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.block.Slice;

import java.io.Flushable;
import java.io.IOException;


public class FileHandle implements Flushable {
    // parent store
    private final FileManager fileManager;
    // global configuration
    private final Configure configure;

    /**
     * data file and write-read block. newest is for writting
     * like lastest is for reading.
     */
    private volatile DataFile newest;
    private volatile DataFile earliest;

    // newest or earliest target object
    private volatile Block writeBlock;
    private volatile Block readBlock;

    FileHandle(FileManager fileManager, Configure configure) {
        this.fileManager = fileManager;
        this.newest = fileManager.getNewestDataFile();
        this.earliest = fileManager.getEarliestDataFile();
        this.configure = configure;
    }

    public void append(Slice slice) {
        // avoid "The End Of The World" .....
        int retry = 8;

        while (ensureWriteableBlock() && writeBlock != null && retry-- != 0) {
            // return false if this Block is full. Since we froze it that make
            // the Block in read only mode
            if (writeBlock.write(slice))
                break;

            writeBlock.froze();
        }
    }

    private boolean ensureWriteableBlock() {
        // no block space to be written. first of all we try
        // to create one new Block. if still null returned we
        // create a new DataFile (data file is incessant)
        if (writeBlock == null || writeBlock.isFrozen()) {
            if ((writeBlock = newest.nextBlock()) == null) {
                try {
                    // allocate new data file because of there has no more space
                    newest = fileManager.createDataFile();
                    writeBlock = newest.nextBlock();
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            // we successfully get the next Block. next we write the previous
            // one to disk and flush the dirty page
            if (configure.get(Option.SYNC) == Syncer.BLOCK)
                newest.sync();
        }

        return true;
    }

    public Slice fetch() {
        return null;        // nothing here
    }

    private Block findAvailableBlock() {
        if (readBlock != null && readBlock.remain())
            return readBlock;

        return readBlock = earliest.readBlock();
    }


    @Override
    public void flush() throws IOException {
        System.out.println("============================ flush");
        newest.sync();
    }
}
