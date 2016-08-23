package org.diskstore.common;

public interface RefCount {

    /**
     * clean up method after decr() called if refcount decreased
     * to zero !
     *
     */
    public void release();
}
