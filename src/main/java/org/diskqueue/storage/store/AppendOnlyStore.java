package org.diskqueue.storage.store;

import org.diskqueue.storage.block.Slice;

public interface AppendOnlyStore extends Store {
    public void writeAppend(Slice slice);
    public Slice fetch();
}
