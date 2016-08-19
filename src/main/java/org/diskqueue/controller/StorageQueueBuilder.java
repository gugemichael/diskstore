package org.diskqueue.controller;

import org.diskqueue.controller.impl.PersistenceQueue;
import org.diskqueue.option.Option;
import org.diskqueue.storage.DiskStorage;
import org.diskqueue.storage.Storage;
import org.diskqueue.storage.store.MappedAppendOnlyStore;

/**
 * Disk queue builder. construct from options {@link Configure}
 */
public class StorageQueueBuilder<E> {
    private Configure configure = new Configure();

    public <T> StorageQueueBuilder<E> option(Option option, T value) {
        configure.set(option, value);
        return this;
    }

    public StorageQueue<E> build() {
        Storage storage = null;
        switch (configure.get(Option.STORAGE)) {
        case PERSISTENCE:
            // memory mapped storage engine. It's a container of
            // following actually I/O store
            DiskStorage diskStorage = new DiskStorage();
            storage = diskStorage.use(new MappedAppendOnlyStore(diskStorage, configure));
            break;
        case MEMORY:
            throw new UnsupportedOperationException();
        }
        assert (storage != null);

        return new PersistenceQueue<E>(configure.get(Option.NAME), storage);
    }
}
