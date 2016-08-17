package org.diskqueue.utils;

public abstract class Hypervisor implements Runnable {
    // executing thread
    private Thread parent;

    protected abstract boolean execute();

    @Override
    public void run() {
        while (true) {
            try {
                if (!execute())
                    break;
            } catch (OutOfMemoryError oom) {
                oom.printStackTrace();
                break;
            } catch (Throwable error) {
                error.printStackTrace();
                continue;
            }
        }
    }

    public void start() {
        parent = new Thread(this);
        parent.start();
    }
}
