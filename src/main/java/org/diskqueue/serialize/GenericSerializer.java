package org.diskqueue.serialize;

public interface GenericSerializer {

    public byte[] encode(Object o);

    public Object decode(byte[] bytes);
}
