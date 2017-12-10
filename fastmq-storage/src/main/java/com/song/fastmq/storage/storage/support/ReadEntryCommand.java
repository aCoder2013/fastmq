package com.song.fastmq.storage.storage.support;

import com.song.fastmq.storage.storage.LedgerEntry;
import com.song.fastmq.storage.storage.Position;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.ReadEntryCallback;
import com.song.fastmq.storage.storage.impl.LedgerCursorImpl;
import java.util.ArrayList;
import java.util.List;

/**
 * @author song
 */
public class ReadEntryCommand implements AsyncCallbacks.ReadEntryCallback {

    private LedgerCursorImpl ledgerCursor;

    private volatile int maxNumberToRead;

    private List<LedgerEntry> entries = new ArrayList<>();

    private AsyncCallbacks.ReadEntryCallback callback;

    public ReadEntryCommand(LedgerCursorImpl ledgerCursor, int maxNumberToRead,
        ReadEntryCallback callback) {
        this.ledgerCursor = ledgerCursor;
        this.maxNumberToRead = maxNumberToRead;
        this.callback = callback;
    }

    public int getMaxNumberToRead() {
        return maxNumberToRead;
    }

    public ReadEntryCallback getCallback() {
        return callback;
    }

    @Override
    public void readEntryComplete(List<LedgerEntry> entries) {
        if (entries.size() > 0) {
            LedgerEntry ledgerEntry = entries.get(entries.size() - 1);
            Position position = ledgerEntry.getPosition();
            this.ledgerCursor
                .updateReadPosition(
                    new Position(position.getLedgerId(), position.getEntryId() + 1));
            this.entries.addAll(entries);
        }
        callback.readEntryComplete(this.entries);
    }

    public void updateReadPosition(Position position) {
        this.ledgerCursor.updateReadPosition(position);
    }

    @Override
    public void readEntryFailed(Throwable throwable) {
        callback.readEntryFailed(throwable);
    }

    public Position getReadPosition() {
        return this.ledgerCursor.getReadPosition();
    }

    public void setMaxNumberToRead(int maxNumberToRead) {
        this.maxNumberToRead = maxNumberToRead;
    }

    public synchronized void addEntries(List<LedgerEntry> ledgerEntries) {
        this.entries.addAll(ledgerEntries);
    }

    public String name(){
        return this.ledgerCursor.name();
    }
}
