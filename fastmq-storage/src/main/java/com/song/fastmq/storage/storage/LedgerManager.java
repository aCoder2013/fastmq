package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;

/**
 * Created by song on 2017/11/5.
 */
public interface LedgerManager {

    String getName();

    Position addEntry(byte[] data) throws InterruptedException, LedgerStorageException;

    void asyncAddEntry(byte[] data, AsyncCallback<Position, LedgerStorageException> asyncCallback);

    void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback);

    void close() throws InterruptedException, LedgerStorageException;
}
