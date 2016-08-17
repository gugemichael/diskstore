package org.diskqueue.storage;

import org.diskqueue.storage.block.DataFile;
import org.diskqueue.utils.Hypervisor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Disk file clean up thread.
 *
 * Delete files on Linux file system with ext3 or later. ext3 will approximately block
 * for a long time while removing a big normal file. discuss at :
 *
 * http://serverfault.com/questions/128012/how-to-make-rm-faster-on-ext3-linux
 *
 * therefore, we provide a seperate standalone thread to acompulish
 *
 */
public class CleanupThreadDeleter extends Hypervisor {
    // remove task queue
    private BlockingQueue<DataFile> toBeDeleted = new LinkedBlockingQueue<>();

    @Override
    protected boolean execute() {
        DataFile delete = null;
        try {
            for (;;) {
                delete = toBeDeleted.take();
                delete.getGenericFile().delete();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
}
