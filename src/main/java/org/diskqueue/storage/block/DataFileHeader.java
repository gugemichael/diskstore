package org.diskqueue.storage.block;

import java.nio.ByteBuffer;

/**
 * Header of the datafile {@link DataFile}
 */
public class DataFileHeader {
    public static final int DATA_BLOCK_HEADER_SIZE = 4 * 1024;

    public static final int STATUS_WRITE = 1;
    public static final int STATUS_READ = 2;
    public static final int STATUS_CORRUPTED = 4;
    public static final int STATUA_DEAD = 8;

    /**
     * metadata header show details of the datafile
     */
    private static final int BLOCK_USED_OFFSET = 0;
    private static final int START_BLOCK_NUMBER_OFFSET = 4;
    private static final int STATUS_OFFSET = 8;         // 1 is write, 2 is read, 4 is conrrupt, 8 is dead
    private static final int READ_OFFSET = 12;

    private ByteBuffer buffer;
    private int startPosition = 0;

    public boolean from(ByteBuffer buffer) {
        this.buffer = buffer;
        startPosition = buffer.position();
        return true;
    }


    public int getBlockUsed() {
        return buffer.getInt(startPosition + BLOCK_USED_OFFSET);
    }

    public void setBlockUsed(int blockUsed) {
        buffer.putInt(startPosition + BLOCK_USED_OFFSET, blockUsed);
    }

    public int getStartBlockNumber() {
        return buffer.getInt(startPosition + START_BLOCK_NUMBER_OFFSET);
    }

    public void setStartBlockNumber(int startBlockNumber) {
        buffer.putInt(startPosition + START_BLOCK_NUMBER_OFFSET, startBlockNumber);
    }

    public int getStatus() {
        return buffer.getInt(startPosition + STATUS_OFFSET);
    }

    private void setStatus(int status) {
        buffer.putInt(startPosition + STATUS_OFFSET, status);
    }

    public void writeable() {
        setStatus(getStatus() | STATUS_WRITE);
    }

    public void unwriteable() {
        setStatus(getStatus() & ~STATUS_WRITE);
    }

    public void readble() {
        setStatus(getStatus() | STATUS_READ);
    }

    public void unreadble() {
        setStatus(getStatus() & ~STATUS_READ);
    }

    public void dead() {
        setStatus(STATUA_DEAD);
    }

    public int getReadOffset() {
        return buffer.getInt(startPosition + READ_OFFSET);
    }

    public void setReadOffset(int readOffset) {
        buffer.putInt(startPosition + READ_OFFSET, readOffset);
    }
}
