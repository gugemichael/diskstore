package org.diskqueue.controller.impl;

import org.diskqueue.controller.DiskQueue;
import org.diskqueue.storage.Storage;
import org.diskqueue.storage.block.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PersistenceQueue<E> extends DiskQueue<E> {

    // the name of the Queue
    private final String queueName;
    // storage engine layer
    private Storage storage;

    public PersistenceQueue(String queueName, Storage storage) {
        this.queueName = queueName;
        this.storage = storage;
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(E e) {
        storage.add(new Record().setObject(e));
        return true;
    }

    @Override
    public E remove() {
        if (capacity.intValue() == 0)
            throw new NoSuchElementException();

        return poll();
    }

    @Override
    public E poll() {
        return (E) storage.pop().getObject();
    }

    @Override
    public E element() {
        if (capacity.intValue() == 0)
            throw new NoSuchElementException();

        return peek();
    }

    @Override
    public E peek() {
        return (E) storage.peek().getObject();
    }
}
