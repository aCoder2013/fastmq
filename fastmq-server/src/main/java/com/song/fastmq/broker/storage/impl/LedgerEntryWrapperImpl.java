package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.Position;

/**
 * @author song
 */
public class LedgerEntryWrapperImpl implements LedgerEntryWrapper {

    private final byte[] data;

    private final Position position;

    public LedgerEntryWrapperImpl(byte[] data, Position position) {
        this.data = data;
        this.position = position;
    }

    @Override public byte[] getData() {
        return this.data;
    }

    @Override public int length() {
        return this.data.length;
    }

    @Override public Position getPosition() {
        return this.position;
    }
}
