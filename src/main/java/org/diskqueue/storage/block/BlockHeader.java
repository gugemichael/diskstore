package org.diskqueue.storage.block;

import java.nio.ByteBuffer;

public class BlockHeader {
    public static final int BLOCK_HEADER_LENGTH = 16;

    public static final int BLOCK_WRITEABLE = 0;
    public static final int BLOCK_FROZEN = 1;

    // number of slice
    private int sliceCount = 0;
    // is already filled up since we can only read this Block
    private int isFrozen = BLOCK_WRITEABLE;
    // checksum of the entire Block except this BlockHeader
    private int checksum = 0;

    private byte[] padding = new byte[4];

    public boolean decode(byte[] bits) {
        if (bits.length >= BLOCK_HEADER_LENGTH) {
            // read header
            ByteBuffer buffer = ByteBuffer.wrap(bits);
            sliceCount = buffer.getInt();
            isFrozen = buffer.getInt();
            checksum = buffer.getInt();
            assert (isFrozen == 0 || isFrozen == 1);
            assert (checksum != 0);
            // discard the left padding bytes
            return true;
        }

        return false;
    }

    public void encode(byte[] bits) {
        ByteBuffer buffer = ByteBuffer.wrap(bits);
        buffer.putInt(sliceCount);
        buffer.putInt(isFrozen);
        buffer.putInt(checksum);
        buffer.put(padding);
        assert (buffer.position() == BLOCK_HEADER_LENGTH);
    }


    public int getSliceCount() {
        return sliceCount;
    }

    public void setSliceCount(int sliceCount) {
        this.sliceCount = sliceCount;
    }

    public int isFrozen() {
        return isFrozen;
    }

    public void setFrozen(int isFrozen) {
        this.isFrozen = isFrozen;
    }

    public int getChecksum() {
        return checksum;
    }

    public void setChecksum(int checksum) {
        this.checksum = checksum;
    }
}
