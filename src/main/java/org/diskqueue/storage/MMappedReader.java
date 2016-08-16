package org.diskqueue.storage;

import org.diskqueue.storage.block.Record;

/*
 * MMaped store for reading
 *
 */
public class MMappedReader extends MMappedStore {
    // only for read
    private final MMappedMode mode = MMappedMode.READ_ABLE;

    public Record readRecord() {
        return null;
    }

    public Record lookRecord() {
        return null;
    }
}
