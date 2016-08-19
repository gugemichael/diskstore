package org.diskqueue.storage.block;

import org.diskqueue.storage.FileManager;
import org.diskqueue.storage.store.DiskFile;
import org.diskqueue.storage.store.FileStatus;
import org.diskqueue.utils.FileUtils;
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
public class DataFile extends DiskFile implements Comparable, RefCount {
    public static final int DATA_BLOCK_FILE_SIZE = 512 * 1024 * 1024;
    public static final int MAX_BLOCK_COUNT = (DATA_BLOCK_FILE_SIZE - DATA_BLOCK_HEADER_LENGTH) / BLOCK_SIZE;

    // belonged FileManager. used to recyle itself
    private FileManager fileManager;

    // header of the file
    private DataFileHeader header = new DataFileHeader();
    // in memory block
    private volatile Block memoryBlock;
    // data file sequence number
    private final int fileNumber;

    // memory map related
    private FileChannel mmapFileChannel;
    private MappedByteBuffer mmapBuffer;

    private DataFile(FileManager fileManager, File mmappedFile, FileStatus fileStatus) {
        super(mmappedFile, fileStatus);
        this.fileManager = fileManager;
        this.fileNumber = FileUtils.getFileNumber(mmappedFile.getName());
    }

    public static DataFile New(FileManager fileManager, File mmappedFile) throws IOException {
        return new DataFile(fileManager, mmappedFile, FileStatus.WRITE).mmap();
    }

    public static DataFile load(FileManager fileManager, File mmappedFile) throws IOException {
        return new DataFile(fileManager, mmappedFile, FileStatus.READ).mmap();
    }

    private DataFile mmap() throws IOException {
        if (!thisFile.exists()) {
            if (fileStatus == FileStatus.READ || !thisFile.createNewFile())
                throw new IOException(String.format("create non-exist data file failed : %s", thisFile.getName()));
        }

        if (!thisFile.canRead())
            throw new IOException(String.format("data file read access violate : %s", thisFile.getName()));

        // TODO : ftruncate the file. this operation don't fill zero into the file
        RandomAccessFile accessable = new RandomAccessFile(thisFile, "rw");
        if (fileStatus == FileStatus.WRITE)
            accessable.setLength(DATA_BLOCK_FILE_SIZE);
        // make channel
        mmapFileChannel = accessable.getChannel();
        // mount entire file to New byte array buffer
        mmapBuffer = mmapFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, thisFile.length());

        // parse datafile header
        byte[] headerBits = new byte[DATA_BLOCK_HEADER_LENGTH];
        mmapBuffer.get(headerBits);
        header.decode(headerBits);

        switch (fileStatus) {
        case READ:
            if (header.getReadOffset() != 0)
                mmapBuffer.position(header.getReadOffset());
            break;
        case WRITE:
            if (header.getWriteOffset() != 0)
                mmapBuffer.position(header.getWriteOffset());
            break;
        }

        return this;
    }

    public Block nextBlock() {
        if (header.getBlockCount() + 1 <= MAX_BLOCK_COUNT) {
            if (memoryBlock != null)
                mmapBuffer.put(memoryBlock.getMemory());
            header.setBlockCount(header.getBlockCount() + 1);
            header.writeable();
            memoryBlock = Block.create();
            return memoryBlock;
        }

        return null;
    }

    public Block readBlock() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFile dataFile = (DataFile) o;
        return fileNumber == dataFile.fileNumber && fileStatus == dataFile.fileStatus;
    }

    @Override
    public int hashCode() {
        return fileNumber * fileStatus.ordinal();
    }

    @Override
    public synchronized void sync() {
        assert (fileStatus == FileStatus.WRITE);
        // flush header
        int now = mmapBuffer.position();
        mmapBuffer.position(0);
        mmapBuffer.put(header.encode());
        // sync the being flushed block
        mmapBuffer.force();
        mmapBuffer.position(now);
    }

    @Override
    public int compareTo(Object other) {
        return this.fileNumber - ((DataFile) other).fileNumber;
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
