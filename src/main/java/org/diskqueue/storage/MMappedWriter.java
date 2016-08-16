package org.diskqueue.storage;

import org.diskqueue.option.Syncer;
import org.diskqueue.storage.block.Record;

import java.io.Flushable;
import java.io.IOException;

/**
 * MMaped store for writting
 */
public class MMappedWriter extends MMappedStore implements Flushable {
    // only for write
    private final MMappedMode mode = MMappedMode.WRITE_ABLE;

    // flush caller. change with Syncer
    private final Flusher flusher;

    public MMappedWriter(Syncer syncer) {
        this.flusher = Flusher.policy(this, syncer);
    }


    public void writeRecord(Record record) {
        try {
            this.flusher.flushAll();
        } catch (IOException ignored) {
            ignored.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {

    }
}
