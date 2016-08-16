package org.diskqueue.storage.block;

import org.diskqueue.storage.FileHandle;

public class Block {
    // Fixed block size
    public static int BLOCK_SIZE = 8 * 1024 * 1024;

    // belonged file handle
    private FileHandle fileHandle;
    // detail meta data of this Block
    private BlockHeader header;
    // elements in the Block
    private Record[] records;

}
