package org.diskqueue.storage;

import org.diskqueue.storage.block.Record;

/**
 * An abstract of underlayer storage
 */
public interface Storage extends Accessor {
    public void add(Record record);

    public Record pop();

    public Record peek();
}
