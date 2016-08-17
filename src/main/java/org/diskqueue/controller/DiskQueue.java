package org.diskqueue.controller;

import org.diskqueue.controller.impl.PersistenceQueue;
import org.diskqueue.option.Options;
import org.diskqueue.storage.DiskStorage;
import org.diskqueue.storage.MMappedStore;
import org.diskqueue.storage.Storage;

import java.util.AbstractCollection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class DiskQueue<E> extends AbstractCollection<E> implements Queue<E> {

    // element counter
    protected AtomicInteger capacity = new AtomicInteger();

    @Override
    public int size() {
        return capacity.intValue();
    }

    public static DiskQueueBuilder newBuilder() {
        return new DiskQueueBuilder();
    }

    /**
     * Disk queue builder. construct from options {@link Options}
     */
    static class DiskQueueBuilder {
        private Map<Options<?>, Object> opsMap = new ConcurrentHashMap<>();

        public <T> DiskQueueBuilder option(Options<T> option, T value) {
            opsMap.put(option, value);
            return this;
        }

        /**
         * return specified option name's value if this option is setted. or named default value.
         *
         * @param option named option already defined
         * @return setted value or default value
         */
        @SuppressWarnings("unchecked")
        public <T> T option(Options<T> option) {
            T value;
            if ((value = (T) opsMap.get(option)) != null)
                return value;
            else
                return option.defaultValue();
        }

        public DiskQueue build() throws IllegalAccessException {
            String queueName = option(Options.NAME);
            Storage storage = null;
            switch (option(Options.STORAGE)) {
            case PERSISTENCE:
                // memory mapped storage engine. It's a container of
                // following actually I/O store
                storage = new DiskStorage().use(new MMappedStore().byteOrder(option(Options.BYTE_ORDER))
                                            .syncer(option(Options.SYNC)).initialize(option(Options.DATA_PATH), option(Options.NAME)));
                break;
            case MEMORY:
                throw new UnsupportedOperationException();
            }
            assert (storage != null);
            return new PersistenceQueue(queueName, storage);
        }
    }
}
