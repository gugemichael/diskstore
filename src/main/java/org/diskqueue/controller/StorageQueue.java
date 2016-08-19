package org.diskqueue.controller;

import java.util.AbstractCollection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class StorageQueue<E> extends AbstractCollection<E> implements Queue<E> {
    // element counter
    protected AtomicInteger capacity = new AtomicInteger();

    @Override
    public int size() {
        return capacity.intValue();
    }
}
