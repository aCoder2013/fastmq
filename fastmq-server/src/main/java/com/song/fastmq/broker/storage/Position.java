package com.song.fastmq.broker.storage;

/**
 * @author song
 */
public class Position {

    private final long ledgerId;

    private final long entryId;

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
