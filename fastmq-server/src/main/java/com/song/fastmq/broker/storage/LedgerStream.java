package com.song.fastmq.broker.storage;

import java.util.List;

/**
 * Created by song on 2017/11/5.
 */
public class LedgerStream {

    private String name;

    private List<LedgerMetadata> ledgers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<LedgerMetadata> getLedgers() {
        return ledgers;
    }

    public void setLedgers(List<LedgerMetadata> ledgers) {
        this.ledgers = ledgers;
    }
}
