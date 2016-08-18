package org.diskqueue.storage;

import org.diskqueue.storage.store.DiskFile;
import org.diskqueue.utils.Hypervisor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Async I/O flusher
 *
 */
public class AsyncIOFlusher extends Hypervisor {
    // tasks of file is being deleted
    private BlockingQueue<DiskFile> flushQueue = new ArrayBlockingQueue<DiskFile>(8192);

    AsyncIOFlusher() {
        super("Async-I/O-Thread-Flusher", false);
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
        flushQueue.offer(flushable);
    }
}
