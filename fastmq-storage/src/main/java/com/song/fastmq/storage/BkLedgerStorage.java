package com.song.fastmq.storage;

import com.song.fastmq.storage.concurrent.AsyncCallback;

/**
 * A factory to manage ledgers.
 *
 * Created by song on 2017/11/4.
 */
public interface BkLedgerStorage {

    /**
     * Open a ledger of given name. If the ledger doesn't exist, a new one will be automatically created.
     *
     * @param name name of the ledger,must be unique.
     * @return the ledger
     */
    LedgerManager open(String name) throws LedgerStorageException, InterruptedException;

    void asyncOpen(String name, AsyncCallback<LedgerManager, LedgerStorageException> asyncCallback);

}

