package org.diskstore.engine.storage;

import org.diskstore.engine.storage.store.DiskFile;
import org.diskstore.common.Hypervisor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Async I/O flusher
 *
 */
public class AsyncIOFlusher extends Hypervisor {
    // tasks of file is being deleted
    private BlockingQueue<DiskFile> flushQueue = new ArrayBlockingQueue<>(512);

    AsyncIOFlusher() {
        super("Async-I/O-Thread-Flusher", true);
    }

    @Override
    protected boolean execute() {
        DiskFile flushable = null;
        try {
            for (; ; ) {
                flushable = flushQueue.take();
                flushable.sync();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    public void flush(DiskFile flushable) {
        try {
            flushQueue.put(flushable);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
