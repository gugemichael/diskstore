package org.diskstore.engine.option;

public class Option<T> {

    // default value for every get
    private final T defaultValue;

    public Option(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public T defaultValue() {
        return defaultValue;
    }

    /**
     * take sync operation with the following policy
     * <p>
     * NONE,                     don't take sync behaviour. depends on system flush
     * BLOCK,                   sync with every Block fills up
     * EVERY_SECOND,      sync every second
     *
     */
    public static Option<Syncer> SYNC = new Option<>(Syncer.EVERY_SECOND);

    /**
     * a folder path that includes manifest and data files
     *
     */
    public static Option<String> DATA_PATH = new Option<>("./");

    /**
     * name of the queue. used to create folder name if storage is file backend
     *
     */
    public static Option<String> NAME = new Option<>("diskstore");

    /**
     * storage type .
     *
     * @see StorageType
     */
    public static Option<StorageType> STORAGE = new Option<>(StorageType.PERSISTENCE);

    /**
     * recover from the prevois status with file underlay storage while restart
     *
     */
    public static Option<Boolean> RECOVERY = new Option<>(Boolean.TRUE);
}

