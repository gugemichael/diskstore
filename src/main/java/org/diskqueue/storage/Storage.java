package org.diskqueue.storage;

import org.diskqueue.storage.block.Record;

/**
 * An abstract of underlayer storage
 */
public interface Storage extends RandomAccessable {
    void write(Record record);
    Record read();
}
