package org.diskqueue.storage;

import org.diskqueue.option.Syncer;
import org.diskqueue.storage.block.Record;

public class MMapedStorage implements Storage {
    // write append store handle
    private final MMappedWriter writer;
    // reader store handle
    private final MMappedReader reader;

    public MMapedStorage(Syncer syncer) {
        writer = new MMappedWriter(syncer);
        reader = new MMappedReader();
    }

    @Override
    public void seek(StorageOffset offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(Record record) {
        writer.writeRecord(record);
    }

    @Override
    public Record pop() {
        return reader.readRecord();
    }

    @Override
    public Record peek() {
        return reader.lookRecord();
    }
}
