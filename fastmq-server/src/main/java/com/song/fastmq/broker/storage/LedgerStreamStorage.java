package com.song.fastmq.broker.storage;

/**
 * Created by song on 2017/11/5.
 */
public interface LedgerStreamStorage {

    /**
     * Get ledgerStream by name
     *
     * @param ledgerName name of the ledger
     * @return the ledger with given name
     * @throws InterruptedException
     * @throws LedgerStorageException
     */
    LedgerStream getLedgerStream(String ledgerName) throws InterruptedException, LedgerStorageException;

    /**
     * Get ledger asynchronously
     *
     * @param name
     * @param asyncCallback
     * @see #getLedgerStream(String)
     */
    void asyncGetLedgerStream(String name, AsyncCallback<LedgerStream, LedgerStorageException> asyncCallback);

    void asyncUpdateLedgerStream(String name, LedgerStream ledgerStream, Version version,
        AsyncCallback<Void, LedgerStorageException> asyncCallback);

    /**
     * Delete ledger with the given name asynchronously
     *
     * @param name
     * @param asyncCallback
     */
    void asyncRemoveLedger(String name, AsyncCallback<Void, LedgerStorageException> asyncCallback);
}
