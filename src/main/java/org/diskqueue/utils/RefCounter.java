package org.diskqueue.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class RefCounter<T extends RefCount> {
    private AtomicInteger count = new AtomicInteger();
    private final T instance;

    public RefCounter(T instance) {
        this.instance = instance;
    }

    public RefCounter<T> incrRef() {
        this.count.incrementAndGet();
        return this;
    }

    public RefCounter<T> decrRef() {
        if (this.count.decrementAndGet() == 0)
            instance.recyle();
        return this;
    }

    public int getRefCount() {
        return this.count.intValue();
    }

    public T getInstance() {
        return this.instance;
    }
}
