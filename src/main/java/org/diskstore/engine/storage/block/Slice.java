package org.diskstore.engine.storage.block;

public class Slice {
    public byte[] body;
    public int size;

    public Slice(byte[] body) {
        this.body = body;
        this.size = body.length;
    }
}
