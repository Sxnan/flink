package org.apache.flink.api.java.io;

import org.apache.flink.core.io.InputSplit;

class BlockingShuffleInputSplit implements InputSplit {

    @Override
    public int getSplitNumber() {
        throw new UnsupportedOperationException();
    }
}
