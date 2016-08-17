package org.diskqueue.option;


import java.nio.ByteOrder;

public class Options<T> {

    // default value for every option
    private final T defaultValue;

    public Options(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public T defaultValue() {
        return defaultValue;
    }

    /**
     * take sync operation with the following policy
     *
     * NONE,                     don't take sync behaviour. depends on system flush
     * ONCE,                     sync with every write() method called. always issue to disk
     * MMAP_PAGECACHE,  sync with every Block fills up
     * EVERY_SECOND,      sync every second
     */
    public static Options<Syncer> SYNC = new Options<>(Syncer.MMAP_PAGECACHE);

    /**
     * a folder path that includes meta data files and block files
     *
     */
    public static Options<String> DATA_PATH = new Options<>("./");

    /**
     * name of the queue. used to create folder name if storage is file backend
     *
     */
    public static Options<String> NAME = new Options<>("diskqueue");

    /**
     * storage type .
     *
     * @see StorageType
     */
    public static Options<StorageType> STORAGE = new Options<>(StorageType.PERSISTENCE);

    /**
     * byteorder of storage file binary stream
     *
     */
    public static Options<ByteOrder> BYTE_ORDER= new Options<>(ByteOrder.BIG_ENDIAN);
}

