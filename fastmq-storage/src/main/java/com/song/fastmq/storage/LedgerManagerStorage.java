package com.song.fastmq.storage;

import com.song.fastmq.storage.concurrent.AsyncCallback;

/**
 * Created by song on 2017/11/5.
 */
public interface LedgerManagerStorage {

    /**
     * Get ledgerStream by name
     *
     * @param ledgerName name of the ledger
     * @return the ledger with given name
     * @throws InterruptedException
     * @throws LedgerStorageException
     */
    LedgerInfoManager getLedgerManager(String ledgerName) throws InterruptedException, LedgerStorageException;

    /**
     * Get ledger asynchronously
     *
     * @param name
     * @param asyncCallback
     * @see #getLedgerManager(String)
     */
    void asyncGetLedgerManager(String name, AsyncCallback<LedgerInfoManager, LedgerStorageException> asyncCallback);

    void asyncUpdateLedgerManager(String name, LedgerInfoManager ledgerInfoManager, Version version,
        AsyncCallback<Void, LedgerStorageException> asyncCallback);

    void removeLedger(String name) throws InterruptedException, LedgerStorageException;

    /**
     * Delete ledger with the given name asynchronously
     *
     * @param name
     * @param asyncCallback
     */
    void asyncRemoveLedger(String name, AsyncCallback<Void, LedgerStorageException> asyncCallback);
}
