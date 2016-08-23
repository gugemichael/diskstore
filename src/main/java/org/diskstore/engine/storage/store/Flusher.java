package org.diskstore.engine.storage.store;

import org.diskstore.engine.option.Syncer;

import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher {

    /**
     * flush target object
     */
    Flushable flushable;

    private Flusher(Flushable flushable) {
        this.flushable = flushable;
    }

    /**
     * real flush method
     */
    public abstract void flushAll() throws IOException;

    public static Flusher policy(Flushable flushable, Syncer syncer) {
        switch (syncer) {
        case BLOCK:
            return new NoOpFlusher(flushable);
        case PAGE_CACHE:
            return new PageCacheFlusher(flushable);
        case EVERY_SECOND:
            return new EverySecondFlusher(flushable);
        default:
            throw new UnsupportedOperationException();
        }
    }

    static class NoOpFlusher extends Flusher {
        public NoOpFlusher(Flushable flushable) {
            super(flushable);
        }

        @Override
        public void flushAll() throws IOException {
            // no operation
        }
    }

    static class EverySecondFlusher extends Flusher {
        private long timestamp = System.currentTimeMillis();

        public EverySecondFlusher(Flushable flushable) {
            super(flushable);
        }

        @Override
        public void flushAll() throws IOException {
            if (System.currentTimeMillis() - timestamp >= 1000) {
                // TODO : the order of following invocation is right ?
                timestamp = System.currentTimeMillis();
                flushable.flush();
            }
        }
    }

    static class PageCacheFlusher extends Flusher {
        public PageCacheFlusher(Flushable flushable) {
            super(flushable);
        }

        @Override
        public void flushAll() throws IOException {
            // TODO : call memory map msync() here ?!
        }
    }
}
