package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

/**
 * Created by song on 2017/11/5.
 */
public interface LogManager {

    String getTopic();

    Offset addEntry(byte[] data) throws InterruptedException, LedgerStorageException;

    void asyncAddEntry(byte[] data, AsyncCallback<Offset, LedgerStorageException> asyncCallback);

    void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback);

    void close() throws InterruptedException, LedgerStorageException;
}
