package org.diskstore.engine;

import org.diskstore.engine.storage.block.Slice;

public interface Store {

    void writeAppend(Slice slice);

    Slice readNext();
}
