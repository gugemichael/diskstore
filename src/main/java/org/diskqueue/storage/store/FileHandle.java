package org.diskqueue.storage.store;

import org.diskqueue.controller.Configure;
import org.diskqueue.option.Option;
import org.diskqueue.option.Syncer;
import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.block.Block;
import org.diskqueue.storage.block.BlockHeader;
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

    FileHandle(FileManager fileManager) {
        this.fileManager = fileManager;
        this.configure = fileManager.getConfigure();
        this.newest = fileManager.getNewestDataFile();
        this.earliest = fileManager.getEarliestDataFile();
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

            assert (writeBlock.getBlockHeader().getFrozen() == BlockHeader.BLOCK_FROZEN);

            // we successfully get the next Block. next we write the previous
            // one to disk and flush the dirty page
            if (configure.get(Option.SYNC) == Syncer.BLOCK) {
                newest.sync();
            }
        }

        assert (retry == 7);
    }

    private boolean ensureWriteableBlock() {
        // no block space to be written. first of all we try
        // to create one new Block. if still null returned we
        // create a new DataFile (data file is incessant)
        if (writeBlock == null || writeBlock.isFrozen()) {
            if ((writeBlock = newest.nextWriteBlock()) == null) {
                try {
                    newest.sync();
                    // allocate new data file because of there has no more space
                    newest = fileManager.createDataFile();
                    writeBlock = newest.nextWriteBlock();
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }

        return true;
    }

    public Slice fetch() {
        findAvailableBlock();
        if (readBlock != null && readBlock.hasMore())
            return readBlock.fetch();

        return null;        // nothing here
    }

    private Block findAvailableBlock() {
        if (readBlock != null) {
            if (readBlock.hasMore() || !readBlock.isFrozen())
                return readBlock;
        }

        // check if the block has been frozen. may be we should
        // peek the next block from now
        if ((readBlock = earliest.nextReadBlock()) == null) {
            System.out.println("complete earliest " + earliest.getGenericFile().getName());
            DataFile next = fileManager.getEarliestDataFile();
            if (next != null) {
                // now we should release the earliest data file
                fileManager.release(earliest);
                earliest = next;
                // I'm sure that readBlock should not be nullptr
                readBlock = earliest.nextReadBlock();
            } else {
                return null;
            }
        }

        return readBlock;
    }


    @Override
    public void flush() throws IOException {
        System.out.println("============================ flush");
        newest.sync();
    }
}
