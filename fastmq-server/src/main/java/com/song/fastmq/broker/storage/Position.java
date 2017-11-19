package com.song.fastmq.broker.storage;

/**
 * @author song
 */
public class Position {

    private long ledgerId;

    private long entryId;

    public Position() {
    }

    public Position(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }
}
