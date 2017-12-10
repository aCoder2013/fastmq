package com.song.fastmq.storage.concurrent;

import com.song.fastmq.storage.LedgerCursor;
import com.song.fastmq.storage.LedgerRecord;
import java.util.List;

/**
 * @author song
 */
public class AsyncCallbacks {

    public interface VoidCallback{
        void onComplete();

        void onThrowable(Throwable throwable);
    }

    public interface ReadEntryCallback {
        void readEntryComplete(List<LedgerRecord> entries);

        void readEntryFailed(Throwable throwable);
    }

    public interface CloseLedgerCursorCallback {
        void onComplete();

        void onThrowable(Throwable throwable);
    }

    public interface OpenCursorCallback {
        void onComplete(LedgerCursor ledgerCursor);

        void onThrowable(Throwable throwable);
    }
}
