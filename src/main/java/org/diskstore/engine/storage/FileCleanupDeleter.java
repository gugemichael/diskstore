package org.diskstore.engine.storage;

import org.diskstore.common.Hypervisor;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Disk file clean up thread.
 *
 * Delete files on Linux file system with ext3 or later. ext3 will approximately block
 * for a long time while removing a big normal file. discuss at :
 *
 * http://serverfault.com/questions/128012/how-to-make-rm-faster-on-ext3-linux
 *
 * therefore, we provide a seperate standalone thread to acompulish
 */
public class FileCleanupDeleter extends Hypervisor {
    private static final boolean onlyMarkedRemove = false;

    // tasks of file is being deleted
    private BlockingQueue<File> toBeDeleted = new LinkedBlockingQueue<>();
    // task complete notifier
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wait = lock.newCondition();


    FileCleanupDeleter() {
        super("Clean-Thread-Deleter", true);
    }

    @Override
    protected boolean execute() {
        File delete = null;
        try {
            for (; ; ) {
                delete(toBeDeleted.take());
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean asyncDelete(File[] files, boolean waitCompleted) {
        // rename first
        for (File file : files) {
            // rename is vary fast !
            File renamed = new File(String.format("%s.deleted", file.getAbsolutePath()));
            file.renameTo(renamed);

            // wait all above inserted file elements for complete
            if (waitCompleted)
                delete(renamed);
            else
                toBeDeleted.offer(renamed);
        }


        return true;
    }

    private void delete(File delete) {
        if (!onlyMarkedRemove)
            delete.delete();
    }

}
