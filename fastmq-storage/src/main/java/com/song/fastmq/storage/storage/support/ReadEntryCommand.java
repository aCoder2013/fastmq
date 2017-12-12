package com.song.fastmq.storage.storage.support;

import com.song.fastmq.storage.storage.LogRecord;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.ReadEntryCallback;
import com.song.fastmq.storage.storage.impl.LogReaderImpl;
import java.util.ArrayList;
import java.util.List;

/**
 * @author song
 */
public class ReadEntryCommand implements AsyncCallbacks.ReadEntryCallback {

    private LogReaderImpl ledgerCursor;

    private volatile int maxNumberToRead;

    private List<LogRecord> entries = new ArrayList<>();

    private AsyncCallbacks.ReadEntryCallback callback;

    public ReadEntryCommand(LogReaderImpl ledgerCursor, int maxNumberToRead,
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
    public void readEntryComplete(List<LogRecord> entries) {
        if (entries.size() > 0) {
            LogRecord logRecord = entries.get(entries.size() - 1);
            Offset offset = logRecord.getOffset();
            this.ledgerCursor
                .updateReadPosition(
                    new Offset(offset.getLedgerId(), offset.getEntryId() + 1));
            this.entries.addAll(entries);
        }
        callback.readEntryComplete(this.entries);
    }

    public void updateReadPosition(Offset offset) {
        this.ledgerCursor.updateReadPosition(offset);
    }

    @Override
    public void readEntryFailed(Throwable throwable) {
        callback.readEntryFailed(throwable);
    }

    public Offset getReadPosition() {
        return this.ledgerCursor.getReadOffset();
    }

    public void setMaxNumberToRead(int maxNumberToRead) {
        this.maxNumberToRead = maxNumberToRead;
    }

    public synchronized void addEntries(List<LogRecord> ledgerEntries) {
        this.entries.addAll(ledgerEntries);
    }

    public String name(){
        return this.ledgerCursor.name();
    }
}
