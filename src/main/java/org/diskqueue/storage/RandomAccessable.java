package org.diskqueue.storage;

public interface RandomAccessable {
    void seek(StorageOffset offset);
}
