package org.diskqueue.test;

import org.diskqueue.controller.Configure;
import org.diskqueue.option.Option;
import org.diskqueue.option.Syncer;
import org.diskqueue.storage.DiskStorage;
import org.diskqueue.storage.Storage;
import org.diskqueue.storage.block.Record;
import org.diskqueue.storage.store.MappedAppendOnlyStore;

import java.util.Arrays;

public class SimpleStore {

    public static void main(String[] args) throws InterruptedException {

//        StorageQueue<Integer> queue = new StorageQueueBuilder<Integer>().option(Option.SYNC, Syncer.EVERY_SECOND).option(Option.DATA_PATH, "/tmp/diskqueue")
//                                        .option(Option.NAME, "myqueue").option(Option.RECOVERY, true).build();

        DiskStorage diskStorage = new DiskStorage();
        Storage storage = diskStorage.use(new MappedAppendOnlyStore(diskStorage,
                                        new Configure().set(Option.SYNC, Syncer.BLOCK)
                                                                .set(Option.DATA_PATH, "/tmp/diskqueue")
                                                                .set(Option.NAME, "myqueue").set(Option.RECOVERY, true)));

        long i = 10000000000L;
        byte[] mem = new byte[256];
        Arrays.fill(mem, (byte) 8);
        while(i-- != 0) {
            diskStorage.write(new Record().setBody(mem));
        }

        System.out.println("Write DONE");
    }
}
