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
import static org.diskqueue.storage.block.DataFileHeader.DATA_BLOCK_HEADER_SIZE;

/**
 * Files which contain BlockHeader and Blocks
 */
public class DataFile extends DiskFile implements Comparable, RefCount {
    public static final int DATA_BLOCK_FILE_SIZE = 1 * 512 * 1024 * 1024;
    public static final int MAX_BLOCK_COUNT = (DATA_BLOCK_FILE_SIZE - DATA_BLOCK_HEADER_SIZE) / BLOCK_SIZE;

    // belonged FileManager. used to release itself
    private FileManager fileManager;

    // fileHeader of the file
    private DataFileHeader fileHeader = new DataFileHeader();
    // in memory block
    private volatile Block memoryBlock;
    // data file sequence number
    private final int fileNumber;

    private int readBlockOffset = -1;

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
                throw new IOException(String.format("forWriteable non-exist data file failed : %s", thisFile.getName()));
        }

        if (!thisFile.canRead())
            throw new IOException(String.format("data file read access violate : %s", thisFile.getName()));

        // TODO : ftruncate the file. this operation don't fill zero into the file
        RandomAccessFile accessable = new RandomAccessFile(thisFile, "rw");
        FileChannel.MapMode mode = FileChannel.MapMode.READ_ONLY;
        if (fileStatus == FileStatus.WRITE) {
            accessable.setLength(DATA_BLOCK_FILE_SIZE);
            mode = FileChannel.MapMode.READ_WRITE;
        }
        // make channel
        mmapFileChannel = accessable.getChannel();
        // mount entire file to New byte array buffer
        mmapBuffer = mmapFileChannel.map(mode, 0, thisFile.length());

        // TODO : load all pages from disk to memory (should be pagecache) because of read only
        if (fileStatus == FileStatus.READ)
            ; // mmapBuffer.load();

        // parse datafile fileHeader
        fileHeader.from(mmapBuffer);

        switch (fileStatus) {
        case READ:
            mmapBuffer.position(skipToSpecificBlock(fileHeader.getReadOffset()));
            break;
        case WRITE:
            mmapBuffer.position(skipToSpecificBlock(fileHeader.getBlockUsed()));
            break;
        }

        return this;
    }

    private int skipToSpecificBlock(int blockOffset) {
        return BLOCK_SIZE * blockOffset + DATA_BLOCK_HEADER_SIZE;
    }

    public Block nextWriteBlock() {
        assert (fileStatus == FileStatus.WRITE);

        fileHeader.writeable();
        // fix and update the previous one's fileHeader information
        if (memoryBlock != null)
            memoryBlock.getBlockHeader().setChecksum(0x22222233); // memoryBlock.getBlockHeader().setChecksum(memoryBlock.checksum());

        if (fileHeader.getBlockUsed() + 1 <= MAX_BLOCK_COUNT) {
            fileHeader.setBlockUsed(fileHeader.getBlockUsed() + 1);
            fileHeader.unwriteable();

            moveToNextBlock();
            memoryBlock = Block.with(mmapBuffer, false);

            //sync();
            return memoryBlock;
        }

        return null;
    }

    public Block nextReadBlock() {
        System.out.println("[[[[ next read block of " + getGenericFile().getName() + "]]]]]");
//        assert (fileStatus == FileStatus.READ);
        // there is no more block in this file
        if (!moveToNextBlock()) {
            System.out.println("======= file end ======");
            return null;
        }

        readBlockOffset++;
        memoryBlock = Block.with(mmapBuffer, true);
        System.out.println("blockNumber " + memoryBlock.getBlockHeader().getBlockNumber());
        System.out.println("blockCount " + memoryBlock.getBlockHeader().getSliceCount());
        System.out.println("isFrozen " + memoryBlock.getBlockHeader().getFrozen());
        System.out.println("checksum " + memoryBlock.getBlockHeader().getChecksum());

        return memoryBlock;
    }

    private boolean moveToNextBlock() {
        // align block to BLOCK_SIZE. we need to skip some
        // hasMore bytes
        int pos = mmapBuffer.position();
        int skip = ((pos - DATA_BLOCK_HEADER_SIZE) % BLOCK_SIZE) != 0
                ? BLOCK_SIZE - ((pos - DATA_BLOCK_HEADER_SIZE) % BLOCK_SIZE)
                : 0;

        // no more block at the tail
        if (mmapBuffer.position() + skip >= mmapBuffer.capacity())
            return false;
        if (mmapBuffer.position() + skip + BLOCK_SIZE > mmapBuffer.capacity())
            return false;

        mmapBuffer.position(mmapBuffer.position() + skip);
        return true;
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
        // flush fileHeader and modified content
        mmapBuffer.force();
    }

    @Override
    public int compareTo(Object other) {
        return this.fileNumber - ((DataFile) other).fileNumber;
    }

    @Override
    public void release() {
        // release file descriptor
        close();
        fileManager.getDeleter().asyncDelete(new File[]{getGenericFile()}, false);
    }

    public void close() {
        try {
            mmapFileChannel.close();
            // TODO : invoke cleaner
            mmapBuffer = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getFileNumber() {
        return this.fileNumber;
    }
}
