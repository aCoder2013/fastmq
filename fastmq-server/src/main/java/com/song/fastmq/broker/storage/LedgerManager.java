package com.song.fastmq.broker.storage;

import com.song.fastmq.broker.storage.concurrent.AsyncCallback;
import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import java.util.List;

/**
 * Created by song on 2017/11/5.
 */
public interface LedgerManager {

    String getName();

    Position addEntry(byte[] data) throws InterruptedException, LedgerStorageException;

    void asyncAddEntry(byte[] data, AsyncCallback<Position, LedgerStorageException> asyncCallback);

    List<LedgerEntryWrapper> readEntries(int numberToRead,
        Position position) throws InterruptedException, LedgerStorageException;

    void asyncReadEntries(int numberToRead, Position position,
        AsyncCallbacks.ReadEntryCallback callback);

    void close() throws InterruptedException, LedgerStorageException;
}
