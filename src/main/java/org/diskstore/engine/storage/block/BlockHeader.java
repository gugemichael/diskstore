package org.diskstore.engine.storage.block;

import java.nio.ByteBuffer;

public class BlockHeader {
    public static final int BLOCK_HEADER_SIZE = 16;

    public static final int BLOCK_WRITEABLE = 0;
    public static final int BLOCK_FROZEN = 1;

    // number of slice
    private static final int BLOCK_NUMBER_OFFSET = 0;
    // number of slice
    private static final int SLICE_COUNT_OFFSET = 4;
    // is already filled up since we can only read this Block
    private static final int IS_FROZEN_OFFSET = 8;
    // checksum of the entire Block except this BlockHeader
    private static final int CHECKSUM_OFFSET = 12;

    private ByteBuffer buffer;
    private int startPosition = 0;

    public boolean from(ByteBuffer buffer, boolean forRead) {
        this.buffer = buffer;
        startPosition = buffer.position();
        // check header
        if (!forRead) {
            setBlockNumber(Block.BlockNumber.getAndIncrement());
        }
        return true;
    }

    public int getBlockNumber() {
        return buffer.getInt(startPosition + BLOCK_NUMBER_OFFSET);
    }

    public void setBlockNumber(int blockNumber) {
        buffer.putInt(startPosition + BLOCK_NUMBER_OFFSET, blockNumber);
    }

    public int getSliceCount() {
        return buffer.getInt(startPosition + SLICE_COUNT_OFFSET);
    }

    public void setSliceCount(int sliceCount) {
        buffer.putInt(startPosition + SLICE_COUNT_OFFSET, sliceCount);
    }

    public void incrSliceCount() {
        buffer.putInt(startPosition + SLICE_COUNT_OFFSET, getSliceCount() + 1);
    }

    public int getFrozen() {
        return buffer.getInt(startPosition + IS_FROZEN_OFFSET);
    }

    public void setFrozen(int isFrozen) {
        buffer.putInt(startPosition + IS_FROZEN_OFFSET, isFrozen);
    }

    public int getChecksum() {
        return buffer.getInt(startPosition + CHECKSUM_OFFSET);
    }

    public void setChecksum(int checksum) {
        buffer.putInt(startPosition + CHECKSUM_OFFSET, checksum);

    }

    public int getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(int startPosition) {
        this.startPosition = startPosition;
    }
}
