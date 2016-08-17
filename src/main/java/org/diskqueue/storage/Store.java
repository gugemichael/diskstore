package org.diskqueue.storage;

import org.diskqueue.storage.block.Slice;

public interface Store {
    public void writeSlice(Slice slice);
    public Slice readSlice();
}
