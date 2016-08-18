package org.diskqueue.test;

import org.diskqueue.option.Syncer;
import org.diskqueue.storage.DiskStorage;
import org.diskqueue.storage.block.Record;
import org.diskqueue.storage.store.MappedAppendOnlyStore;

import java.util.Arrays;

public class SimpleStore {

    public static void main(String[] args) throws InterruptedException {

        DiskStorage diskStorage = new DiskStorage();
        diskStorage.use(new MappedAppendOnlyStore(diskStorage)
                            .recovery(true).syncer(Syncer.BLOCK).initialize("/tmp/diskqueue", "diskqueue"));

        long i = 10000000000L;
        byte[] mem = new byte[128];
        Arrays.fill(mem, (byte) 8);
        while(i-- != 0) {
            diskStorage.write(new Record().setBody(mem));
        }

        System.out.println("Write DONE");
    }
}
