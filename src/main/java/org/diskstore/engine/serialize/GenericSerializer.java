package org.diskstore.engine.serialize;

public interface GenericSerializer {

    public byte[] encode(Object o);

    public Object decode(byte[] bytes);
}
