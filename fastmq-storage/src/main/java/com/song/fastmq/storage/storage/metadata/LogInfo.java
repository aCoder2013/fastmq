package com.song.fastmq.storage.storage.metadata;

import java.util.List;

/**
 * Created by song on 2017/11/5.
 */
public class LogInfo {

    /**
     * Name of this ledger
     */
    private String name;

    private List<LogSegmentInfo> ledgers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<LogSegmentInfo> getLedgers() {
        return ledgers;
    }

    public void setLedgers(List<LogSegmentInfo> ledgers) {
        this.ledgers = ledgers;
    }
}
