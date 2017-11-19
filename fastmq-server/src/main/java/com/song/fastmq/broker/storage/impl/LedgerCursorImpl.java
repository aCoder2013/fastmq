package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.LedgerCursor;
import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.LedgerManager;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Position;
import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import java.util.List;

/**
 * @author song
 */
public class LedgerCursorImpl implements LedgerCursor {

    private final String name;

    private final LedgerManager ledgerManager;

    private Position readPosition;

    public LedgerCursorImpl(String name, LedgerManager manager) {
        this.name = name;
        ledgerManager = manager;
    }

    @Override public String name() {
        return name;
    }

    @Override
    public List<LedgerEntryWrapper> readEntries(int numberToRead) throws InterruptedException, LedgerStorageException {
        List<LedgerEntryWrapper> wrappers = this.ledgerManager.readEntries(numberToRead, readPosition);
        readPosition = new Position(readPosition.getLedgerId(), readPosition.getEntryId() + numberToRead);
        return wrappers;
    }

    @Override public void asyncReadEntries(int numberToRead, AsyncCallbacks.ReadEntryCallback callback) {

    }

    @Override public void close() {

    }

    @Override public void asyncClose(AsyncCallbacks.CloseLedgerCursorCallback callback) {

    }
}
