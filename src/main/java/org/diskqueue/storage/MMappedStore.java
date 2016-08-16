package org.diskqueue.storage;

import org.diskqueue.storage.block.Block;

/**
 * MMaped file backend that store block(s) {@link org.diskqueue.storage.block.Block}
 */
public class MMappedStore {
    // file handle
    private FileHandle fileHandle;
    // current reading block
    private Block block;

    enum MMappedMode {
        WRITE_ABLE, READ_ABLE
    }
}
