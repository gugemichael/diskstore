package org.diskqueue.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class RefCounter<T extends RefCount> {
    private AtomicInteger count = new AtomicInteger();
    private final T instance;

    public RefCounter(T instance) {
        this.instance = instance;
    }

    public void incrRef() {
        this.count.incrementAndGet();
    }

    public void decrRef() {
        if (this.count.decrementAndGet() == 0)
            instance.recyle();
    }

    public T getInstance() {
        return this.instance;
    }
}
