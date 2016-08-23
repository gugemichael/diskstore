package org.diskstore.engine.storage;

public interface RandomAccessable {
    void seek(Offset offset);
}
