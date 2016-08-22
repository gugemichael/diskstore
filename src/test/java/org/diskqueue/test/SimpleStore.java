package org.diskqueue.test;

import org.diskqueue.controller.Configure;
import org.diskqueue.option.Option;
import org.diskqueue.option.Syncer;
import org.diskqueue.storage.block.Slice;
import org.diskqueue.storage.store.AppendOnlyStore;
import org.diskqueue.storage.store.MappedAppendOnlyStore;

import java.util.Arrays;

public class SimpleStore {

    public static void main(String[] args) throws InterruptedException {


        final AppendOnlyStore store = new MappedAppendOnlyStore(null,
                new Configure().set(Option.SYNC, Syncer.BLOCK)
                        .set(Option.DATA_PATH, "/tmp/diskqueue")
                        .set(Option.NAME, "myqueue").set(Option.RECOVERY, true));

        class Writer extends Thread {
            @Override
            public void run() {
                long i = 10000000000L;
                long sum = 0;
                byte[] mem = new byte[256];
                Slice slice;
                Arrays.fill(mem, (byte) 8);
                while (i-- != 0) {
                    store.writeAppend(new Slice(mem));
                    if (i % 100000 == 0)
                        System.out.println("writer 10w ....");
                }
            }
        }

        class Reader extends Thread {
            public Reader(String name) {
                super(name);
            }

            @Override
            public void run() {
                try {
                    long i = 10000000000L;
                    long sum = 0;
                    byte[] mem = new byte[256];
                    Slice slice;
                    Arrays.fill(mem, (byte) 8);
                    while (i-- != 0) {
                        if ((slice = store.fetch()) == null) {
                            Thread.sleep(5);
                        } else {
                            if (!Arrays.equals(mem, slice.body)) {
                                for (int l = 0; l != slice.body.length; l++)
                                    System.err.print(slice.body[l]);
                                System.err.println();

                            }
                        }
                        sum++;
                        if (i % 100000 == 0)
                            System.err.println("reader 10w ....");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


        Thread w = new Writer();
        Thread r = new Reader("Reader");
        w.start();
        r.start();

        w.join();
        r.join();

        System.out.println("Write DONE");

    }
}
