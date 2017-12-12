package com.song.fastmq.storage.storage;

/**
 * @author song
 */
public class Offset {

    private long ledgerId;

    private long entryId;

    public Offset() {
    }

    public Offset(long ledgerId, long entryId) {
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
        return "Offset{" +
            "ledgerId=" + ledgerId +
            ", entryId=" + entryId +
            '}';
    }
}
