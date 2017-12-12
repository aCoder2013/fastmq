package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.metadata.Log;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

/**
 * Created by song on 2017/11/5.
 */
public interface LogInfoStorage {

    /**
     * Get ledgerStream by name
     *
     * @param topic name of the topic
     * @return the ledger with given name
     * @throws InterruptedException
     * @throws LedgerStorageException
     */
    Log getLogInfo(String topic) throws InterruptedException, LedgerStorageException;

    /**
     * Get ledger asynchronously
     *
     * @param name
     * @param asyncCallback
     * @see #getLogInfo(String)
     */
    void asyncGetLogInfo(String name, AsyncCallback<Log, LedgerStorageException> asyncCallback);

    void asyncUpdateLogInfo(String name, Log log, Version version,
        AsyncCallback<Void, LedgerStorageException> asyncCallback);

    void removeLogInfo(String name) throws InterruptedException, LedgerStorageException;

    /**
     * Delete ledger with the given name asynchronously
     *
     * @param name
     * @param asyncCallback
     */
    void asyncRemoveLogInfo(String name, AsyncCallback<Void, LedgerStorageException> asyncCallback);
}
