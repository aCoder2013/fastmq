package com.song.fastmq.storage.storage.concurrent;

import com.song.fastmq.storage.storage.GetMessageResult;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.Version;

/**
 * @author song
 */
public class AsyncCallbacks {

    public interface VoidCallback {

        void onComplete();

        void onThrowable(Throwable throwable);
    }

    public interface CloseLedgerCursorCallback {

        void onComplete();

        void onThrowable(Throwable throwable);
    }

    public interface ReadOffsetCallback {

        void onComplete(Offset offset);

        void onThrowable(Throwable throwable);
    }

    public interface PutMessageCallback {

        void onComplete(Offset offset);

        void onThrowable(Throwable throwable);
    }

    public interface GetMessageCallback {

        void readEntryComplete(GetMessageResult result);

        void readEntryFailed(Throwable throwable);

    }

    /**
     * Created by song on 2017/11/5.
     */
    public interface CommonCallback<T, E> {

        void onCompleted(T data, Version version);

        void onThrowable(E throwable);
    }
}
