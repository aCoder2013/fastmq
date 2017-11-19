package com.song.fastmq.broker.storage;

/**
 * Created by song on 2017/11/5.
 */
public class LedgerInfo {

    private long ledgerId;

    private long timestamp;

    public long getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
