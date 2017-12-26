package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.CommonCallback;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

/**
 * Created by song on 2017/11/5.
 */
public interface LogManager {

    String getName();

    Offset addEntry(byte[] data) throws InterruptedException, LedgerStorageException;

    void asyncAddEntry(byte[] data, CommonCallback<Offset, LedgerStorageException> asyncCallback);

    void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback);

    boolean isClosed();

    void close() throws InterruptedException, LedgerStorageException;
}
