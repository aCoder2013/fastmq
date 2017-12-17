package com.song.fastmq.storage.storage.concurrent;

import com.song.fastmq.storage.storage.LogRecord;
import com.song.fastmq.storage.storage.LogReader;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.Version;
import java.util.List;

/**
 * @author song
 */
public class AsyncCallbacks {

    public interface VoidCallback {

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

        void onComplete(LogReader logReader);

        void onThrowable(Throwable throwable);
    }

    public interface ReadOffsetCallback {

        void onComplete(Offset offset);

        void onThrowable(Throwable throwable);
    }

    /**
     * Created by song on 2017/11/5.
     */
    public  interface CommonCallback<T, E> {

        void onCompleted(T data, Version version);

        void onThrowable(E throwable);
    }
}
