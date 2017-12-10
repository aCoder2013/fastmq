package com.song.fastmq.storage.storage;

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

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    @Override public String toString() {
        return "Position{" +
            "ledgerId=" + ledgerId +
            ", entryId=" + entryId +
            '}';
    }
}
