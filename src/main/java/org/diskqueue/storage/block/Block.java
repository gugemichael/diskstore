package org.diskqueue.storage.block;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import static org.diskqueue.storage.block.BlockHeader.BLOCK_HEADER_SIZE;

public class Block {
    // Fixed block size
    public static int BLOCK_SIZE = 4 * 1024 * 1024;
    // Block sequence bumber
    public static final AtomicInteger blockNumber = new AtomicInteger();

    // detail meta data of this Block
    private BlockHeader blockHeader = new BlockHeader();

    // block memory
    private ByteBuffer buffer;

    private int remainSize = BLOCK_SIZE - BLOCK_HEADER_SIZE;

    // for checksum calculate
    private CRC32 crc32 = new CRC32();

    private Block() {
    }

    static Block with(ByteBuffer underlayer, boolean forRead) {
        Block block = new Block();
        // initial and write the blockHeader area
        block.blockHeader.from(underlayer, forRead);
        underlayer.position(underlayer.position() + BLOCK_HEADER_SIZE);

        // current buffer position has cross over the blockHeader and
        // moved to data area already. so directly read at this offset
        block.buffer = underlayer;
        return block;
    }

    //  | ---------- | ------------- |
    //  |   length  |      body      |
    //  | ---------  | ------------- |
    //  |   4byte   |     n bytes   |
    //  | ---------- | ------------- |
    //
    public boolean write(Slice slice) {
        int need = slice.size + 4;
        if (ensureCapacity(need)) {
            blockHeader.incrSliceCount();
            // write the content firstly that we could prevent the
            // reader see the *length* field. but the content has
            // not entirely written. like a partitial write
            int now = buffer.position();
            buffer.position(now + 4);
            buffer.put(slice.body);
            buffer.putInt(now, slice.body.length);
            remainSize -= need;
            return true;
        } else {
            return false;
        }
    }

    public Slice fetch() {
        if (buffer.remaining() > 4) {
            int len = buffer.getInt();
            if (buffer.remaining() >= len) {
                byte[] array = new byte[len];
                buffer.get(array);
                return new Slice(array);
            }
        }
        return null;
    }

    public int checksum() {
        byte[] content = new byte[BLOCK_SIZE - BLOCK_HEADER_SIZE];
        int now = buffer.position();
        buffer.position(blockHeader.getStartPosition() + BLOCK_HEADER_SIZE);
        buffer.get(content, 0, content.length);
        buffer.position(now);

        crc32.update(content);
        int value = (int) crc32.getValue();
        crc32.reset();
        return value;
    }


    private boolean ensureCapacity(int size) {
        return remainSize >= size;
    }

    public boolean isFrozen() {
        return blockHeader.getFrozen() == BlockHeader.BLOCK_FROZEN;
    }

    public void froze() {
        blockHeader.setFrozen(BlockHeader.BLOCK_FROZEN);
    }

    public boolean hasMore() {
        // take a look if there has more slices
        try {
            return buffer.getInt(buffer.position()) != 0;
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
            System.err.println(buffer.capacity() + " " + buffer.position());
            System.exit(-1);
            return false;
        }
    }

    public BlockHeader getBlockHeader() {
        return blockHeader;
    }

    public int getRemainSize() {
        return remainSize;
    }
}
