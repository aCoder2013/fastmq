package com.song.fastmq.broker.storage;

/**
 * Created by song on 2017/11/5.
 */
public interface LedgerManager {

    String getName();

    Position addEntry(byte[] data) throws InterruptedException, LedgerStorageException;

    void asyncAddEntry(byte[] data, AsyncCallback<Position, LedgerStorageException> asyncCallback);

    void close() throws InterruptedException, LedgerStorageException;
}
