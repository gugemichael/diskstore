package org.diskqueue.storage;

import org.diskqueue.storage.block.Record;
import org.diskqueue.storage.block.Slice;

import java.util.concurrent.atomic.AtomicLong;

public class DiskStorage implements Storage {
    // actually mmaped file store engine
    private Store store;

    // disk file deleter thread who is responsibility for clean up
    // consumed (refcount equals zero) files
    private CleanupThreadDeleter deleter = new CleanupThreadDeleter();

    // total count bytes has written and read
    private AtomicLong totalWrittenCount = new AtomicLong();
    private AtomicLong totalReadCount = new AtomicLong();

    public DiskStorage() {
        this.deleter.start();
    }

    public DiskStorage use(Store store) {
        this.store = store;
        return this;
    }

    @Override
    public void seek(StorageOffset offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(Record record) {
        store.writeSlice(new Slice(record.getBody()));
        totalWrittenCount.incrementAndGet();
    }

    @Override
    public Record read() {
        totalReadCount.incrementAndGet();
        return null;
    }
}
