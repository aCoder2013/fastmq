package com.song.fastmq.storage.storage.concurrent;

import com.song.fastmq.storage.storage.LogSegmentManager;
import com.song.fastmq.storage.storage.LogRecord;
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
        void readEntryComplete(List<LogRecord> entries);

        void readEntryFailed(Throwable throwable);
    }

    public interface CloseLedgerCursorCallback {
        void onComplete();

        void onThrowable(Throwable throwable);
    }

    public interface OpenCursorCallback {
        void onComplete(LogSegmentManager logSegmentManager);

        void onThrowable(Throwable throwable);
    }
}
