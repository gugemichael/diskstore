package org.diskqueue.storage;

import org.diskqueue.storage.block.Record;
import org.diskqueue.storage.block.Slice;
import org.diskqueue.storage.store.AppendOnlyStore;

import java.util.concurrent.atomic.AtomicLong;

public class DiskStorage implements Storage {
    // actually mmaped file store engine
    private AppendOnlyStore store;

    // total count bytes has written and read
    private AtomicLong totalWritten = new AtomicLong();
    private AtomicLong totalRead = new AtomicLong();

    public DiskStorage use(AppendOnlyStore store) {
        this.store = store;
        return this;
    }

    @Override
    public void seek(Offset offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(Record record) {
        store.writeAppend(new Slice(record.getBody()));
        totalWritten.incrementAndGet();
    }

    @Override
    public Record read() {
        totalRead.incrementAndGet();
        return null;
    }
}
