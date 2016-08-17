package org.diskqueue.storage.block;

public class Slice {
    private byte[] body;

    public Slice(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
