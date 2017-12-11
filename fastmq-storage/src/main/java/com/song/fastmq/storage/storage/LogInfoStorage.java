package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.metadata.LogInfo;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

/**
 * Created by song on 2017/11/5.
 */
public interface LogInfoStorage {

    /**
     * Get ledgerStream by name
     *
     * @param ledgerName name of the ledger
     * @return the ledger with given name
     * @throws InterruptedException
     * @throws LedgerStorageException
     */
    LogInfo getLogInfo(String ledgerName) throws InterruptedException, LedgerStorageException;

    /**
     * Get ledger asynchronously
     *
     * @param name
     * @param asyncCallback
     * @see #getLogInfo(String)
     */
    void asyncGetLogInfo(String name, AsyncCallback<LogInfo, LedgerStorageException> asyncCallback);

    void asyncUpdateLogInfo(String name, LogInfo logInfo, Version version,
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
