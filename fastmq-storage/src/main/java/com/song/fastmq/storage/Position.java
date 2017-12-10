package com.song.fastmq.storage;

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

    @Override public String toString() {
        return "Position{" +
            "ledgerId=" + ledgerId +
            ", entryId=" + entryId +
            '}';
    }
}
