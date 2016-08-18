package org.diskqueue.storage.block;

import java.nio.ByteBuffer;

/**
 * Header of the datafile {@link DataFile}
 */
public class DataFileHeader {
    public static final int DATA_BLOCK_HEADER_LENGTH = 4 * 1024;

    public static final int STATUS_WRITE = 1;
    public static final int STATUS_READ = 2;
    public static final int STATUS_CORRUPTED = 4;
    public static final int STATUA_DEAD = 8;

    /**
     * metadata header show details of the datafile
     */
    private int blockCount;
    private int startBlockNumber;
    private int status;          // 1 is write, 2 is read, 4 is conrrupt, 8 is dead
    private int readOffset = -1;
    private int writeOffset = -1;

    // padding with zero and align to 16 byte
    private byte[] padding = new byte[DATA_BLOCK_HEADER_LENGTH - 20];

    public boolean decode(byte[] bits) {
        if (bits.length >= DATA_BLOCK_HEADER_LENGTH) {
            // read header
            ByteBuffer buffer = ByteBuffer.wrap(bits);
            blockCount = buffer.getInt();
            startBlockNumber = buffer.getInt();
            status = buffer.getInt();
            readOffset = buffer.getInt();
            writeOffset = buffer.getInt();
//            assert (status <= STATUA_DEAD && status >= STATUS_WRITE);
            // discard the left padding bytes
            return true;
        }

        return false;
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(DATA_BLOCK_HEADER_LENGTH);
        buffer.putInt(blockCount);
        buffer.putInt(startBlockNumber);
        buffer.putInt(status);
        buffer.putInt(readOffset);
        buffer.putInt(writeOffset);
        buffer.put(padding);
        assert (buffer.position() == DATA_BLOCK_HEADER_LENGTH);
        return buffer.array();
    }

    public int getBlockCount() {
        return blockCount;
    }

    public void setBlockCount(int blockCount) {
        this.blockCount = blockCount;
    }

    public int getStartBlockNumber() {
        return startBlockNumber;
    }

    public void setStartBlockNumber(int startBlockNumber) {
        this.startBlockNumber = startBlockNumber;
    }

    public void writeable() {
        this.status |= STATUS_WRITE;
    }

    public void unwriteable() {
        this.status &= ~STATUS_WRITE;
    }

    public void readble() {
        this.status |= STATUS_READ;
    }

    public void unreadble() {
        this.status &= ~STATUS_READ;
    }

    public void dead() {
        this.status = STATUA_DEAD;
    }

    public int getReadOffset() {
        return readOffset;
    }

    public void setReadOffset(int readOffset) {
        this.readOffset = readOffset;
    }

    public int getWriteOffset() {
        return writeOffset;
    }

    public void setWriteOffset(int writeOffset) {
        this.writeOffset = writeOffset;
    }
}
