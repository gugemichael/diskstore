package org.diskqueue.storage.block;

import java.nio.ByteBuffer;

/**
 * Header of the datafile {@link DataFile}
 */
public class DataFileHeader {
    public static final int HEADER_LENGTH = 32;

    public static final int PHASE_WRITE = 0;
    public static final int PHASE_READ = 1;
    public static final int PHASE_CORRUPTED = 2;
    public static final int PHASE_DEAD = 4;

    /**
     * metadata header show details of the datafile
     */
    private int blockSize;
    private int blockCount;
    private int startBlockNumber;
    private int phase;          // 0 is write, 1 is read, 2 is conrrupt, 4 is dead
    private int offset;

    // padding with zero and align to 16 byte
    private byte[] padding = new byte[12];

    public boolean decode(byte[] bits) {
        if (bits.length >= HEADER_LENGTH) {
            // read header
            ByteBuffer buffer = ByteBuffer.wrap(bits);
            blockSize = buffer.getInt();
            blockCount = buffer.getInt();
            startBlockNumber = buffer.getInt();
            phase = buffer.getInt();
            assert (blockSize == Block.BLOCK_SIZE);
            assert (phase <= PHASE_DEAD && phase >= PHASE_WRITE);
            // discard the left padding bytes
            return true;
        }

        return false;
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH);
        buffer.putInt(blockSize);
        buffer.putInt(blockCount);
        buffer.putInt(startBlockNumber);
        buffer.putInt(phase);
        buffer.put(padding);
        assert (buffer.position() == HEADER_LENGTH);
        return buffer.array();
    }
}
