package org.diskstore.common;

public abstract class Hypervisor implements Runnable {
    // executing thread
    private Thread parent;

    private final String name;
    private final boolean daemon;

    public Hypervisor(String name, boolean daemon) {
        this.name = name;
        this.daemon = daemon;
    }

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
        parent = new Thread(this, name);
        parent.setDaemon(daemon);
        parent.start();
    }
}
