package org.diskqueue.storage.block;

import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.store.DiskFile;
import org.diskqueue.utils.RefCount;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static org.diskqueue.storage.block.Block.BLOCK_SIZE;
import static org.diskqueue.storage.block.DataFileHeader.DATA_BLOCK_HEADER_LENGTH;

/**
 * Files which contain BlockHeader and Blocks
 */
public class DataFile extends DiskFile implements RefCount {
    public static final int DATA_BLOCK_FILE_SIZE = 512 * 1024 * 1024;
    public static final int MAX_BLOCK_COUNT = (DATA_BLOCK_FILE_SIZE - DATA_BLOCK_HEADER_LENGTH) / BLOCK_SIZE;

    // belonged FileManager. used to recyle itself
    private FileManager fileManager;

    // header of the file
    private DataFileHeader header = new DataFileHeader();
    // in memory block
    private volatile Block presentBlock;
    // data file sequence number
    private final int fileNumber;

    // memory map related
    private FileChannel mmapFileChannel;
    private MappedByteBuffer mmapBuffer;

    public DataFile(FileManager fileManager, File mmappedFile) {
        super(mmappedFile);
        this.fileManager = fileManager;
        this.fileNumber = Integer.parseInt(mmappedFile.getName().split("\\.")[2]);
    }

    public Block getBlock() {
        return this.presentBlock;
    }

    public Block nextBlock() {
        if (header.getBlockCount() + 1 <= MAX_BLOCK_COUNT) {
            // flush previous block into disk
            if (presentBlock != null) {
                header.setBlockCount(header.getBlockCount() + 1);
                header.writeable();
                mmapBuffer.put(presentBlock.getMemory());
                fileManager.getFlusher().flush(this);
            }

            presentBlock = Block.create();
            return presentBlock;
        }

        return null;
    }

    public Block readBlock() {
        return null;
    }


    @Override
    public synchronized void sync() {
        // flush header
        int now = mmapBuffer.position();
        mmapBuffer.position(0);
        mmapBuffer.put(header.encode());
        // flush block
        mmapBuffer.force();
        mmapBuffer.position(now);
    }

    @Override
    public boolean checkout() {
        try {
            if (!thisFile.exists() && !thisFile.createNewFile())
                throw new IOException(String.format("load non-exist and create data file failed : %s", thisFile.getName()));

            if (thisFile.canRead() && thisFile.canWrite()) {
                // TODO : ftruncate the file. this operation don't fill zero into the file
                RandomAccessFile accessable = new RandomAccessFile(thisFile, "rw");
                accessable.setLength(DATA_BLOCK_FILE_SIZE);
                // make channel
                mmapFileChannel = accessable.getChannel();
                // mount entire file to mmap byte array buffer
                mmapBuffer = mmapFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, thisFile.length());

                // parse datafile header
                byte[] headerBits = new byte[DATA_BLOCK_HEADER_LENGTH];
                mmapBuffer.get(headerBits);

                header.decode(headerBits);

                return true;
            } else
                return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void recyle() {
        // release file descriptor
        try {
            mmapFileChannel.close();
            fileManager.release(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getFileNumber() {
        return this.fileNumber;
    }
}
