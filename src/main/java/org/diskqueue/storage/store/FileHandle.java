package org.diskqueue.storage.store;

import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.block.Block;
import org.diskqueue.storage.block.DataFile;
import org.diskqueue.storage.block.Slice;

import java.io.Flushable;
import java.io.IOException;


public class FileHandle implements Flushable {
    // parent store
    private final FileManager fileManager;

    /**
     * data file and write-read block. producer is writting
     * while consume is reading.
     */
    private volatile DataFile producer;
    private volatile DataFile consume;

    // writeable or readable target object
    private volatile Block writeBlock;
    private volatile Block readBlock;

    public FileHandle(FileManager fileManager) {
        this.fileManager = fileManager;
        this.producer = fileManager.getNewestDataFile();
        this.consume = fileManager.getLatestDataFile();
    }

    public void write(Slice slice) {
        // avoid "The End Of The World" .....
        int retry = 8;

        while (ensureWriteableBlock() && writeBlock != null && retry-- != 0) {
            // return false if this Block is full. Since we froze it that make
            // the Block in read only mode
            if (writeBlock.append(slice))
                break;

            writeBlock.froze();
        }
    }

    private boolean ensureWriteableBlock() {
        // no block space to be written. first of all we try
        // to create one new Block. if still null returned we
        // create a new DataFile (data file is incessant)
        if (writeBlock == null || writeBlock.isFrozen()) {
            writeBlock = producer.nextBlock();
        }

        if (writeBlock == null) {
            try {
                producer = fileManager.newDataFile();
                writeBlock = producer.nextBlock();
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    @Override
    public void flush() throws IOException {
        System.out.println("============================ flush");
        producer.sync();
        fileManager.sync();
    }
}
