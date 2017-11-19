package com.song.fastmq.broker.storage;

import java.util.List;

/**
 * Created by song on 2017/11/5.
 */
public class LedgerInfoManager {

    /**
     * Name of this ledger
     */
    private String name;

    private List<LedgerInfo> ledgers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<LedgerInfo> getLedgers() {
        return ledgers;
    }

    public void setLedgers(List<LedgerInfo> ledgers) {
        this.ledgers = ledgers;
    }
}
