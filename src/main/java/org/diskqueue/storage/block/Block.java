package org.diskqueue.storage.block;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class Block {
    // Fixed block size
    public static int BLOCK_SIZE = 4 * 1024 * 1024;
    // Block sequence bumber
    public static final AtomicLong blockNumber = new AtomicLong();

    // detail meta data of this Block
    private BlockHeader header = new BlockHeader();
    // block memory
    private byte[] memory;
//    // elements in the Block. shard underlayer space with Block::memory[]
//    private Slice[] pieces;

    private ByteBuffer buffer;


    private Block() {
    }

    static Block create() {
        Block block = new Block();
        // initial and write the header area
        byte[] inMemory = new byte[Block.BLOCK_SIZE];
        block.memory = inMemory;
        block.header.encode(inMemory);
        block.buffer = ByteBuffer.wrap(block.memory);

        // skip the header offset
        block.buffer.position(BlockHeader.BLOCK_HEADER_LENGTH);
        return block;
    }

    public static Block read(byte[] memory) {
        Block block = new Block();
        block.memory = memory;
        block.header.decode(block.memory);
        block.buffer = ByteBuffer.wrap(block.memory);
        // skip the header offset
        block.buffer.position(BlockHeader.BLOCK_HEADER_LENGTH);
        return block;
    }

    boolean ensureCapacity(int size) {
        return buffer.remaining() >= size;
    }

    //  | ---------- | ------------- |
    //  |   length  |      body      |
    //  | ---------  | ------------- |
    //  |   4byte   |     n bytes   |
    //  | ---------- | ------------- |
    //
    public boolean write(Slice slice) {
        if (ensureCapacity(slice.size + 4)) {
//            buffer.putInt(slice.body.length);
            buffer.putInt(0x11111111);
            buffer.put(slice.body);
            return true;
        } else
            return false;
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

    public boolean isFrozen() {
        return header.isFrozen() == BlockHeader.BLOCK_FROZEN;
    }

    public void froze() {
        header.setFrozen(BlockHeader.BLOCK_FROZEN);
    }

    public boolean remain() {
        // take a look if there has more slices
        return buffer.getInt(buffer.position()) != 0;
    }

    public BlockHeader getHeader() {
        return header;
    }

    public void setHeader(BlockHeader header) {
        this.header = header;
    }

    public byte[] getMemory() {
        return memory;
    }

    public void setMemory(byte[] memory) {
        this.memory = memory;
    }
}
