package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.CommonCallback;
import com.song.fastmq.storage.storage.metadata.Log;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

/**
 * Created by song on 2017/11/5.
 */
public interface LogInfoStorage {

    /**
     * Get ledgerStream by name
     *
     * @param name name of the topic
     * @return the ledger with given name
     */
    Log getLogInfo(String name) throws InterruptedException, LedgerStorageException;

    /**
     * Get ledger asynchronously
     *
     * @see #getLogInfo(String)
     */
    void asyncGetLogInfo(String name, CommonCallback<Log, LedgerStorageException> asyncCallback);

    void asyncUpdateLogInfo(String name, Log log, Version version,
        CommonCallback<Void, LedgerStorageException> asyncCallback);

    void removeLogInfo(String name) throws InterruptedException, LedgerStorageException;

    /**
     * Delete ledger with the given name asynchronously
     */
    void asyncRemoveLogInfo(String name,
        CommonCallback<Void, LedgerStorageException> asyncCallback);
}
